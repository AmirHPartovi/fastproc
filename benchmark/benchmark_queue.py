import time
import queue
from multiprocessing import Queue as MPQueue, Process
try:
    from fastqueue import Queue as FastQueue
    has_fastqueue = True
except ImportError:
    print("⚠️ Test fastqueue and multiprocessingQueue ")
    has_fastqueue = False


def benchmark_queue(q_class, count=100_000):
    q = q_class()
    start = time.perf_counter()
    for i in range(count):
        q.put(i)
    for _ in range(count):
        q.get()
    end = time.perf_counter()
    return end - start



def producer(q, count):
    for i in range(count):
        q.put(i)


def consumer(q, count):
    for _ in range(count):
        q.get()


def run_mp_benchmark(count=100_000):
    q = MPQueue()

    p1 = Process(target=producer, args=(q, count))
    p2 = Process(target=consumer, args=(q, count))

    start = time.perf_counter()
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    end = time.perf_counter()
    return end - start


def main():
    count = 100_000
    print(f"\n🔁 Running benchmark with {count:,} items...")

    print("\n📦 queue.Queue:")
    q_time = benchmark_queue(queue.Queue, count)
    print(f"⏱️  Time taken: {q_time:.4f} seconds")

    print("\n📦 multiprocessing.Queue:")
    mp_time = run_mp_benchmark(count)
    print(f"⏱️  Time taken: {mp_time:.4f} seconds")

    if has_fastqueue:
        print("\n📦 fastqueue.Queue:")
        fq_time = benchmark_queue(FastQueue, count)
        print(f"⏱️  Time taken: {fq_time:.4f} seconds")


if __name__ == "__main__":
    main()
