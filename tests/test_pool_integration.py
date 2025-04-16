import unittest
import time
from fastproc.fastpool import Pool, ThreadPool
from concurrent.futures import TimeoutError
import os


def cpu_intensive(x):
    return sum(i * i for i in range(x))


def io_intensive(x):
    time.sleep(0.1)
    return x


class TestPoolIntegration(unittest.TestCase):
    def test_process_pool_cpu_intensive(self):
        with Pool(processes=4) as pool:
            data = range(1000, 2000, 100)
            results = pool.map(cpu_intensive, data)
            expected = [cpu_intensive(x) for x in data]
            self.assertEqual(results, expected)

    def test_thread_pool_io_intensive(self):
        with ThreadPool(processes=4) as pool:
            data = range(10)
            results = pool.map(io_intensive, data)
            self.assertEqual(results, list(data))

    def test_mixed_workload(self):
        with Pool(processes=4) as pool:
            # Start multiple tasks of different types
            cpu_task = pool.apply_async(cpu_intensive, (1000,))
            io_task = pool.apply_async(io_intensive, (5,))
            map_task = pool.map_async(cpu_intensive, range(100))

            # Verify all results
            self.assertEqual(cpu_task.get(), cpu_intensive(1000))
            self.assertEqual(io_task.get(), 5)
            self.assertEqual(map_task.get(), [
                             cpu_intensive(x) for x in range(100)])

    def test_large_data_chunking(self):
        data = range(1000)
        chunk_sizes = [1, 10, 100]

        for chunksize in chunk_sizes:
            with Pool(processes=2) as pool:
                results = pool.map(cpu_intensive, data, chunksize=chunksize)
                expected = [cpu_intensive(x) for x in data]
                self.assertEqual(results, expected)
