import unittest
import time
# This line needs to changeimport time
from fastproc.fastpool import Pool, PoolState  # Corrected import
from concurrent.futures import TimeoutError
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def simple_func(x):
    return x * 2


def slow_func(x):
    time.sleep(0.1)
    return x * 2


def error_func(x):
    raise ValueError(f"Error processing {x}")


class TestPoolUnit(unittest.TestCase):
    def setUp(self):
        self.pool = Pool(processes=2)

    def tearDown(self):
        self.pool.terminate()
        self.pool.join()

    def test_pool_initialization(self):
        self.assertEqual(self.pool._state, PoolState.RUN)
        self.assertEqual(len(self.pool._pool), 2)

    def test_apply(self):
        result = self.pool.apply(simple_func, (5,))
        self.assertEqual(result, 10)

    def test_apply_async(self):
        async_result = self.pool.apply_async(simple_func, (5,))
        result = async_result.get()
        self.assertEqual(result, 10)

    def test_map(self):
        data = [1, 2, 3, 4, 5]
        results = self.pool.map(simple_func, data)
        self.assertEqual(results, [2, 4, 6, 8, 10])

    def test_map_async(self):
        data = [1, 2, 3, 4, 5]
        async_result = self.pool.map_async(simple_func, data)
        results = async_result.get()
        self.assertEqual(results, [2, 4, 6, 8, 10])

    def test_timeout(self):
        async_result = self.pool.apply_async(slow_func, (5,))
        with self.assertRaises(TimeoutError):
            async_result.get(timeout=0.05)

    def test_error_handling(self):
        async_result = self.pool.apply_async(error_func, (5,))
        with self.assertRaises(ValueError):
            async_result.get()

    def test_close_and_join(self):
        self.pool.close()
        self.pool.join()
        self.assertEqual(self.pool._state, PoolState.CLOSE)

    def test_context_manager(self):
        with Pool(processes=2) as pool:
            result = pool.apply(simple_func, (5,))
            self.assertEqual(result, 10)
    
    # def test_pool_initialization(self):
    # logger.debug(f"Pool state: {self.pool._state}")
    # logger.debug(f"Pool internals: {vars(self.pool)}")
    # self.assertEqual(self.pool._state, PoolState.RUN)
