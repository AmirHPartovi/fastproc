import unittest
import time
import random
from fastproc.fastpool import Pool
import multiprocessing






def stress_cpu(x):
    return sum(i * i for i in range(x))


def random_sleep(x):
    time.sleep(random.uniform(0.001, 0.01))
    return x


class TestPoolStress(unittest.TestCase):
    def setUp(self):
        self.pool = Pool(processes=multiprocessing.cpu_count())

    def tearDown(self):
        self.pool.terminate()
        self.pool.join()

    def test_concurrent_massive_tasks(self):
        num_tasks = 1000
        data = range(num_tasks)
        results = self.pool.map(stress_cpu, data)
        self.assertEqual(len(results), num_tasks)

    def test_rapid_task_submission(self):
        for _ in range(100):
            async_results = [
                self.pool.apply_async(random_sleep, (i,))
                for i in range(100)
            ]
            results = [r.get() for r in async_results]
            self.assertEqual(results, list(range(100)))

    def test_memory_usage(self):
        # Test with large data
        large_data = [(i, "x" * 10000) for i in range(1000)]

        def process_large_data(data):
            idx, content = data
            return idx, len(content)

        results = self.pool.map(process_large_data, large_data)
        self.assertEqual(len(results), 1000)

    def test_error_recovery(self):
        def random_error(x):
            if random.random() < 0.1:  # 10% chance of error
                raise ValueError(f"Random error for {x}")
            return x

        results = []
        for i in range(100):
            try:
                result = self.pool.apply(random_error, (i,))
                results.append(result)
            except ValueError:
                continue

        self.assertTrue(len(results) > 0)
        self.assertTrue(len(results) < 100)

    def test_long_running_tasks(self):
        def long_task(seconds):
            time.sleep(seconds)
            return seconds

        tasks = [random.uniform(0.1, 0.5) for _ in range(20)]
        start_time = time.time()
        results = self.pool.map(long_task, tasks)
        total_time = time.time() - start_time

        # Verify parallel execution
        self.assertTrue(total_time < sum(tasks))
