# tests/test_pool_integration.py

import unittest
import multiprocessing
import time

# Force spawn start method
try:
    multiprocessing.set_start_method("spawn", force=True)
except RuntimeError:
    pass

from fastproc.fastpool import Pool


def cpu_intensive(x: int) -> int:
    # trivial CPU work
    s = 0
    for i in range(1, x % 50 + 1):
        s += i * i
    return s


class TestPoolIntegration(unittest.TestCase):
    def test_large_data_chunking(self):
        data = list(range(100))
        for chunksize in (1, 10, 25):
            with Pool(processes=2) as pool:
                results = pool.map(cpu_intensive, data, chunksize=chunksize)
            expected = [cpu_intensive(x) for x in data]
            self.assertEqual(results, expected)

    def test_parallel_apply_multiple(self):
        with Pool(processes=3) as pool:
            futures = [pool.apply_async(
                lambda x, y: x + y, args=(i, i)) for i in range(5)]
            results = [f.get(timeout=1) for f in futures]
        self.assertEqual(results, [i + i for i in range(5)])
