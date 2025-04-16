# tests/test_queue_stress.py

import time
from multiprocessing import Process
from fastproc.fastqueue import Queue

N = 100_000


def stress_producer(q, N):
    for i in range(N):
        q.put(i)


def stress_consumer(q, N):
    for _ in range(N):
        q.get()


if __name__ == "__main__":
    q = Queue(maxsize=10**6)
    p1 = Process(target=stress_producer, args=(q, N))
    p2 = Process(target=stress_consumer, args=(q, N))
    start = time.time()
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    elapsed = time.time() - start
    print(f"Stress test completed in {elapsed:.3f} seconds")
