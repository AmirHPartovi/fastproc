# tests/test_queue_unit.py

import unittest
import time
from fastproc.fastqueue import Queue, JoinableQueue
from queue import Empty, Full


class TestQueueUnit(unittest.TestCase):
    def test_put_and_get(self):
        q = Queue(maxsize=2)
        q.put(10)
        q.put(20)
        self.assertEqual(q.qsize(), 2)
        self.assertEqual(q.get(), 10)
        self.assertEqual(q.get(), 20)
        self.assertTrue(q.empty())

    def test_full_and_timeout(self):
        q = Queue(maxsize=1)
        q.put(42)
        with self.assertRaises(Full):
            q.put(100, timeout=0.1)
        self.assertTrue(q.full())
        q.get()
        with self.assertRaises(Empty):
            q.get(timeout=0.1)

    def test_joinable_queue(self):
        q = JoinableQueue(maxsize=2)
        q.put("task1")
        self.assertEqual(q.get(), "task1")
        q.task_done()


if __name__ == "__main__":
    unittest.main()
