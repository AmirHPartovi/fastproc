"""
Modern queue implementations for inter-process communication.

Note: This file has been renamed from "queue.py" to "fastqueue.py" to prevent
a circular import conflict with Python's built-in queue module.
"""

from __future__ import annotations
import sys
import threading
import collections
import time
import weakref
import logging
import queue as std_queue  # Standard library queue for Empty and Full exceptions
from typing import Any, Optional, Deque, TypeVar, Callable, Generic
from dataclasses import dataclass
from multiprocessing import (
    Pipe,
    get_context,
)
from multiprocessing.util import Finalize, is_exiting, register_after_fork
from threading import Condition
from multiprocessing.context import BaseContext


T = TypeVar('T')
_sentinel = object()

logging.basicConfig(level=logging.DEBUG)

# Obtain the default context and use it to create synchronization primitives
_ctx = get_context()


@dataclass
class QueueState:
    maxsize: int
    buffer: Deque
    thread: Optional[threading.Thread]
    jointhread: Optional[Finalize]
    close: Optional[Finalize]
    closed: bool = False
    joincancelled: bool = False
    size: int = 0  # Manual counter for queue size


class Queue(Generic[T]):
    """
    A process-safe FIFO queue with enhanced performance.
    """

    def __init__(self, maxsize: int = 0, ctx: Optional[BaseContext] = None):
        self._ctx = ctx or get_context('spawn')
        self._maxsize = maxsize
        self._lock = self._ctx.Lock()  # Use multiprocessing Lock
        self._data: collections.deque[T] = collections.deque()
        # Use a smaller value for semaphore initialization
        self._sem = self._ctx.Semaphore(
            min(maxsize, 32767) if maxsize > 0 else 32767)

        if maxsize <= 0:
            maxsize = 2 ** 31 - 1  # Use a very large number for unlimited size

        self._state = QueueState(
            maxsize=maxsize,
            buffer=collections.deque(),
            thread=None,
            jointhread=None,
            close=None
        )

        self._reader, self._writer = Pipe(duplex=False)
        self._rlock = _ctx.Lock()
        self._wlock = None if sys.platform == 'win32' else _ctx.Lock()
        self._notempty = threading.Condition(threading.Lock())
        self._after_fork()

        if sys.platform != 'win32':
            register_after_fork(self, Queue._after_fork)
        logging.debug("Queue initialized with maxsize %s", maxsize)

    def _after_fork(self) -> None:
        """
        Initialize queue state after a fork.
        """
        logging.debug("Queue._after_fork() called")
        self._state.buffer.clear()
        self._state.thread = None
        self._send = self._writer.send
        self._recv = self._reader.recv
        self._poll = self._reader.poll

    def put(self, obj: T, block: bool = True, timeout: Optional[float] = None) -> None:
        """
        Put an item into the queue.
        """
        if self._state.closed:
            raise ValueError("Queue is closed")

        if not self._sem.acquire(block, timeout):
            raise std_queue.Full

        with self._notempty:
            if self._state.thread is None:
                self._start_thread()
            self._state.buffer.append(obj)
            self._state.size += 1
            logging.debug("Item put into queue. New size: %d",
                          self._state.size)
            self._notempty.notify()

    def get(self, block: bool = True, timeout: Optional[float] = None) -> T:
        """
        Remove and return an item from the queue.
        """
        if block and timeout is None:
            with self._rlock:
                item = self._recv()
                self._sem.release()
                self._state.size -= 1
                logging.debug(
                    "Item retrieved from queue. New size: %d", self._state.size)
                return item

        deadline = time.monotonic() + timeout if timeout else None

        if not self._rlock.acquire(block, timeout):
            raise std_queue.Empty

        try:
            wait_time = deadline - time.monotonic() if deadline else 0.0
            if not self._poll(wait_time):
                raise std_queue.Empty
            item = self._recv()
            self._sem.release()
            self._state.size -= 1
            logging.debug(
                "Item retrieved (with timeout) from queue. New size: %d", self._state.size)
            return item
        finally:
            self._rlock.release()

    def qsize(self) -> int:
        """
        Return an approximate size of the queue.
        """
        return self._state.size

    def empty(self) -> bool:
        """
        Return True if the queue is empty.
        """
        return self._state.size == 0

    def full(self) -> bool:
        """
        Return True if the queue is full.
        """
        return self._state.size >= self._state.maxsize

    def get_nowait(self) -> T:
        """
        Remove and return an item if one is immediately available.
        """
        return self.get(block=False)

    def put_nowait(self, obj: T) -> None:
        """
        Put an item into the queue without blocking.
        """
        self.put(obj, block=False)

    def close(self) -> None:
        """
        Close the queue.
        """
        self._state.closed = True
        self._reader.close()
        if self._state.close:
            self._state.close()
        logging.debug("Queue closed.")

    def join_thread(self) -> None:
        """
        Wait for the background thread to exit.
        """
        if not self._state.closed:
            raise ValueError("Queue must be closed first")
        if self._state.jointhread:
            self._state.jointhread()
        logging.debug("Join thread completed.")

    def cancel_join_thread(self) -> None:
        """
        Cancel joining the background thread upon close.
        """
        self._state.joincancelled = True
        if self._state.jointhread:
            self._state.jointhread.cancel()
        logging.debug("Join thread cancelled.")

    def _start_thread(self) -> None:
        """
        Start the background thread that feeds items from the buffer to the pipe.
        """
        logging.debug("Starting queue feeder thread.")
        self._state.buffer.clear()
        self._state.thread = threading.Thread(
            target=self._feed,
            args=(
                self._state.buffer,
                self._notempty,
                self._send,
                self._wlock,
                self._writer.close
            ),
            name='QueueFeederThread',
            daemon=True
        )
        self._state.thread.start()
        logging.debug("Queue feeder thread started.")

        self._state.jointhread = Finalize(
            self._state.thread,
            Queue._finalize_join,
            args=(weakref.ref(self._state.thread),),
            exitpriority=-5
        )
        self._state.close = Finalize(
            self,
            Queue._finalize_close,
            args=(self._state.buffer, self._notempty),
            exitpriority=10
        )

    @staticmethod
    def _finalize_close(buffer: Deque, notempty: threading.Condition) -> None:
        """
        Clean up the queue when the process exits.
        """
        logging.debug("Finalizing queue close.")
        with notempty:
            buffer.append(_sentinel)
            notempty.notify()

    @staticmethod
    def _finalize_join(twr: weakref.ReferenceType[threading.Thread]) -> None:
        """
        Finalize the background thread join.
        """
        logging.debug("Finalizing join thread.")
        thread = twr()
        if thread is not None:
            thread.join()
        logging.debug("Queue feeder thread joined.")

    @staticmethod
    def _feed(
        buffer: Deque,
        notempty: threading.Condition,
        send: Callable,
        writelock: Optional[Any],
        close: Callable
    ) -> None:
        """
        Continuously feed items from the buffer to the pipe.
        """
        logging.debug("Feeder thread started.")
        while True:
            with notempty:
                if not buffer:
                    notempty.wait()
                try:
                    while True:
                        obj = buffer.popleft()
                        if obj is _sentinel:
                            logging.debug(
                                "Feeder thread received sentinel, exiting.")
                            close()
                            return
                        if writelock:
                            with writelock:
                                send(obj)
                        else:
                            send(obj)
                except IndexError:
                    continue
                except Exception as e:
                    if not is_exiting():
                        import traceback
                        traceback.print_exc()
                    logging.error("Error in feeder thread: %s", e)
                    return


class JoinableQueue(Queue):
    """
    A queue with task tracking to support join operations.
    """

    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize)
        # Use an explicit counter for unfinished tasks instead of a Semaphore
        self._unfinished_tasks_count = 0
        self._cond = Condition()

    def put(self, item: T, block: bool = True, timeout: Optional[float] = None) -> None:
        super().put(item, block, timeout)
        with self._cond:
            self._unfinished_tasks_count += 1
            logging.debug("Task added. Unfinished tasks count: %d",
                          self._unfinished_tasks_count)

    def task_done(self) -> None:
        """
        Indicate that a previously enqueued task is complete.
        """
        with self._cond:
            if self._unfinished_tasks_count <= 0:
                raise ValueError("task_done() called too many times")
            self._unfinished_tasks_count -= 1
            if self._unfinished_tasks_count == 0:
                self._cond.notify_all()
                logging.debug("All tasks finished; notified all.")

    def join(self) -> None:
        """
        Block until all items in the queue have been processed.
        """
        with self._cond:
            while self._unfinished_tasks_count > 0:
                self._cond.wait()
            logging.debug("Join complete; all tasks have been processed.")


class SimpleQueue:
    """
    A simplified queue implementation using a locked pipe.
    """

    def __init__(self, *, ctx: Optional[BaseContext] = None):
        # Use the provided context or the default one
        ctx = ctx or get_context()
        self._reader, self._writer = Pipe(duplex=False)
        self._rlock = ctx.Lock()
        self._wlock = None if sys.platform == 'win32' else ctx.Lock()
        self._make_methods()
        logging.debug("SimpleQueue initialized.")

    def empty(self) -> bool:
        """
        Return True if the queue is empty.
        """
        return not self._reader.poll()

    def _make_methods(self) -> None:
        """
        Setup the get and put methods based on the platform.
        """
        if self._wlock is None:
            self.put = self._writer.send
        else:
            def put(obj: Any) -> None:
                if self._wlock is not None:
                    with self._wlock:
                        self._writer.send(obj)
                else:
                    self._writer.send(obj)
            self.put = put

        def get() -> Any:
            with self._rlock:
                return self._reader.recv()
        self.get = get
