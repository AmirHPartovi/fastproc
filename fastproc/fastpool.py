"""
Process Pool implementation for multiprocessing.

This module provides the Pool class for parallel execution using process pools.
"""

from __future__ import annotations

import threading
import itertools
from typing import Any, Optional, Callable, Iterator, TypeVar, Generic
from dataclasses import dataclass
import concurrent.futures
from multiprocessing import get_context
from multiprocessing.context import BaseContext
from queue import Queue

# Type variables
T = TypeVar('T')
R = TypeVar('R')

__all__ = ['Pool', 'ThreadPool']
# # Obtain the default context and use it to create synchronization primitives
# _ctx = get_context()
# Pool states


class PoolState:
    RUN = 0
    CLOSE = 1
    TERMINATE = 2


@dataclass
class TaskResult(Generic[T]):
    """Represents the result of a task execution."""
    success: bool
    value: T
    job_id: int
    index: int


class AsyncResult(Generic[T]):
    """Represents an asynchronous result."""

    def __init__(self, cache: dict, callback: Optional[Callable[[T], None]] = None):
        self.cache = cache
        self.callback = callback
        self.job_id = next(job_counter)
        self._ready = threading.Event()
        self._value: Optional[T] = None
        cache[self.job_id] = self

    def get(self, timeout: Optional[float] = None) -> T:
        """Get the result when it arrives."""
        if not self._ready.wait(timeout):
            raise TimeoutError()
        if isinstance(self._value, Exception):
            raise self._value
        assert self._value is not None
        return self._value

    def add_result(self, result: TaskResult[T]) -> None:
        """Add result and trigger callback if provided."""
        self._value = result.value
        self._ready.set()
        if self.callback and result.success:
            self.callback(result.value)


class AsyncMapResult(AsyncResult[list[T]]):
    """Represents an asynchronous map result."""

    def __init__(self, cache: dict, chunksize: int, callback: Optional[Callable[[list[T]], None]] = None):
        super().__init__(cache, callback)
        self.chunksize = chunksize


# Counter for generating unique job IDs
job_counter = itertools.count()


class Pool:
    """Process pool for parallel execution of tasks."""

    def __init__(
        self,
        processes: Optional[int] = None,
        initializer: Optional[Callable] = None,
        initargs: tuple = (),
        maxtasksperchild: Optional[int] = None,
        context: Any = None
    ):
        """Initialize the pool with the given parameters."""
        self._ctx: BaseContext = context or get_context('spawn')
        self._processes = processes or self._ctx.cpu_count()

        # Create queues using the context
        self._taskqueue: Queue = Queue()
        self._inqueue: Queue = Queue()
        self._outqueue: Queue = Queue()

        # Initialize other attributes
        self._state = PoolState.RUN
        self._cache: dict[int, AsyncResult] = {}
        self._initializer = initializer
        self._initargs = initargs
        self._maxtasksperchild = maxtasksperchild

        # Set up input and output queues
        self._quick_put = self._inqueue.put
        self._quick_get = self._outqueue.get

        # Start worker processes
        self._pool = []
        for _ in range(self._processes):
            w = self._ctx.Process(
                target=self._worker,
                args=(
                    self._inqueue,
                    self._outqueue,
                    initializer,
                    initargs,
                    maxtasksperchild
                )
            )
            self._pool.append(w)
            w.start()

        # Start task and result handlers
        self._handle_tasks()
        self._handle_results()

    def _handle_tasks(self):
        """Start a thread to handle task distribution to workers."""
        thread = threading.Thread(
            target=self._distribute_tasks,
            daemon=True
        )
        thread.start()

    def _handle_results(self):
        """Start a thread to handle results from workers."""
        thread = threading.Thread(
            target=self._collect_results,
            daemon=True
        )
        thread.start()

    def _distribute_tasks(self):
        """Distribute tasks to worker processes."""
        while True:
            try:
                tasks = self._taskqueue.get()
                if tasks is None:
                    # Send termination sentinel to all workers
                    for _ in self._pool:
                        self._quick_put(None)
                    break
                for task in tasks:
                    self._quick_put(task)
            except (EOFError, OSError):
                break

    def _collect_results(self):
        """Collect results from worker processes."""
        while True:
            try:
                result = self._quick_get()
                if result is None:
                    break
                self._cache[result.job_id].add_result(result)
            except (EOFError, OSError):
                break

    def _worker(
        self,
        inqueue: Queue,  # Changed from queue.Queue
        outqueue: Queue,  # Changed from queue.Queue
        initializer: Optional[Callable],
        initargs: tuple,
        maxtasks: Optional[int]
    ) -> None:
        """Worker process main loop."""
        if initializer is not None:
            initializer(*initargs)

        completed = 0
        while maxtasks is None or completed < maxtasks:
            try:
                task = inqueue.get()
                if task is None:
                    break

                job_id, index, func, args, kwargs = task
                try:
                    result = TaskResult(
                        success=True,
                        value=func(*args, **kwargs),
                        job_id=job_id,
                        index=index
                    )
                except Exception as e:
                    result = TaskResult(
                        success=False,
                        value=e,
                        job_id=job_id,
                        index=index
                    )
                outqueue.put(result)
                completed += 1

            except (EOFError, OSError):
                break

    def map(
        self,
        func: Callable[[T], R],
        iterable: Iterator[T],
        chunksize: Optional[int] = None
    ) -> list[R]:
        """Parallel map implementation."""
        if self._state != PoolState.RUN:
            raise ValueError("Pool not running")

        if not chunksize:
            chunksize = self._calculate_chunksize(iterable)

        result = self.map_async(func, iterable, chunksize)
        return result.get()

    def map_async(
        self,
        func: Callable[[T], R],
        iterable: Iterator[T],
        chunksize: Optional[int] = None,
        callback: Optional[Callable[[list[R]], None]] = None
    ) -> AsyncResult[list[R]]:
        """Asynchronous parallel map."""
        if self._state != PoolState.RUN:
            raise ValueError("Pool not running")

        if not chunksize:
            chunksize = self._calculate_chunksize(iterable)

        result: AsyncMapResult[R] = AsyncMapResult[R](
            self._cache, chunksize, callback)
        self._taskqueue.put(self._get_tasks(func, iterable, chunksize, result))
        return result

    def apply(
        self,
        func: Callable[..., R],
        args: tuple = (),
        kwds: Optional[dict] = None
    ) -> R:
        """Apply function to arguments."""
        return self.apply_async(func, args, kwds).get()

    def apply_async(
        self,
        func: Callable[..., R],
        args: tuple = (),
        kwds: Optional[dict] = None,
        callback: Optional[Callable[[R], None]] = None
    ) -> AsyncResult[R]:
        """Asynchronously apply function to arguments."""
        if self._state != PoolState.RUN:
            raise ValueError("Pool not running")

        result = AsyncResult(self._cache, callback)
        self._taskqueue.put([(result.job_id, 0, func, args, kwds or {})])
        return result

    def close(self) -> None:
        """Close the pool."""
        if self._state == PoolState.RUN:
            self._state = PoolState.CLOSE
            self._taskqueue.put(None)

    def terminate(self) -> None:
        """Terminate the pool immediately."""
        self._state = PoolState.TERMINATE
        for p in self._pool:
            p.terminate()

    def join(self) -> None:
        """Wait for the pool to terminate."""
        if self._state not in (PoolState.CLOSE, PoolState.TERMINATE):
            raise ValueError("Pool is still running")
        for p in self._pool:
            p.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()

    def _setup_queues(self) -> None:
        """Set up the task and result queues."""
        self._inqueue = Queue()
        self._outqueue = Queue()
        self._quick_put = self._inqueue.put
        self._quick_get = self._outqueue.get

    def _calculate_chunksize(self, iterable: Iterator) -> int:
        """Calculate size of chunks for iterator."""
        try:
            length = len(list(iterable))
        except (TypeError, AttributeError):
            length = 0
        chunksize, extra = divmod(length, len(self._pool) * 4)
        return chunksize + 1 if extra else chunksize

    def _get_tasks(self, func: Callable, iterable: Iterator, chunksize: int, result: AsyncMapResult) -> list:
        """Create task chunks for parallel processing."""
        tasks: list[tuple[int, int, Callable, tuple, dict]] = []
        it = iter(iterable)
        while True:
            chunk = list(itertools.islice(it, chunksize))
            if not chunk:
                break
            tasks.append((result.job_id, len(tasks), func, (chunk,), {}))
        return tasks


class ThreadPool(Pool):
    def __init__(self, processes=None, initializer=None, initargs=()):
        from multiprocessing.dummy import Process as DummyProcess
        self.Process = DummyProcess
        super().__init__(processes, initializer, initargs)

    def _setup_queues(self):
        self._inqueue = Queue()  # Changed from queue.Queue
        self._outqueue = Queue()  # Changed from queue.Queue
        self._quick_put = self._inqueue.put
        self._quick_get = self._outqueue.get

    @staticmethod
    def _help_stuff_finish(inqueue, task_handler, size):
        # put sentinels at head of inqueue to make workers finish
        inqueue.not_empty.acquire()
        try:
            inqueue.queue.clear()
            inqueue.queue.extend([None] * size)
            inqueue.not_empty.notify_all()
        finally:
            inqueue.not_empty.release()
