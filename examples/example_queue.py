# examples/example_queue.py

from fastproc.fastqueue import JoinableQueue
from multiprocessing import Process
import time
import logging


def worker(q: JoinableQueue):
    while True:
        item = q.get()
        if item is None:
            q.task_done()
            break
        logging.info(f"Processing {item}")
        time.sleep(0.1)
        q.task_done()


if __name__ == "__main__":
    q = JoinableQueue()
    processes = [Process(target=worker, args=(q,)) for _ in range(4)]
    for p in processes:
        p.start()

    for i in range(10):
        q.put(f"Task {i}")

    for _ in processes:
        q.put(None)  # ارسال sentinel برای پایان دادن به workerها

    q.join()
    for p in processes:
        p.join()

    logging.info("All tasks complete.")
