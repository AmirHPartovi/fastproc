# tests/test_pool_unit.py

import unittest
import multiprocessing


# Force spawn start method for pytest compatibility
try:
    multiprocessing.set_start_method("spawn", force=True)
except RuntimeError:
    pass

from fastproc.fastpool import Pool, PoolState


class TestPoolUnit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pool = Pool(processes=2)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()
        cls.pool.join()

    def test_pool_initialization(self):
        self.assertEqual(self.pool._state, PoolState.RUN)
        self.assertEqual(len(self.pool._pool), 2)

    def test_apply_and_apply_async(self):
        def square(x):
            return x * x

        # apply
        result = self.pool.apply(square, args=(5,))
        self.assertEqual(result, 25)

        # apply_async
        ar = self.pool.apply_async(square, args=(6,))
        self.assertEqual(ar.get(timeout=1), 36)

    def test_map_and_map_async(self):
        data = [1, 2, 3, 4]
        # synchronous map
        results = self.pool.map(lambda x: x + 1, data, chunksize=2)
        self.assertEqual(results, [2, 3, 4, 5])

        # async map
        mar = self.pool.map_async(lambda x: x * 3, data, chunksize=2)
        self.assertEqual(mar.get(timeout=1), [3, 6, 9, 12])

    def test_close_and_join(self):
        # idempotent close/join
        self.pool.close()
        self.pool.join()
        self.assertEqual(self.pool._state, PoolState.CLOSE)