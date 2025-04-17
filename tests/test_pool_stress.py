# tests/test_pool_stress.py

import unittest
import multiprocessing
import random
import time

# Force spawn start method
try:
    multiprocessing.set_start_method("spawn", force=True)
except RuntimeError:
    pass

from fastproc.fastpool import Pool


class TestPoolStress(unittest.TestCase):
    def setUp(self):
        # smaller pool for stress but many tasks
        self.pool = Pool(processes=4)

    def tearDown(self):
        self.pool.close()
        self.pool.join()

    def test_stress_map(self):
        N = 10_000
        data = [random.randint(1, 100) for _ in range(N)]
        # time the operation to ensure it completes under a reasonable bound
        start = time.perf_counter()
        results = self.pool.map(lambda x: x * 2, data, chunksize=1000)
        duration = time.perf_counter() - start
        # verify correctness
        self.assertEqual(results, [x * 2 for x in data])
        # should finish within 5 seconds on typical modern hardware
        self.assertLess(duration, 5.0)

    def test_stress_apply_async(self):
        N = 5000
        futures = [self.pool.apply_async(
            lambda x: x + 1, args=(i,)) for i in range(N)]
        # poll results
        results = [f.get(timeout=2) for f in futures]
        self.assertEqual(results, list(range(1, N + 1)))


if __name__ == "__main__":
    unittest.main()
