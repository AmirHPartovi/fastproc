# tests/test_process.py
from fastproc.process import Process


def dummy():
    pass


def test_process_lifecycle():
    p = Process(target=dummy)
    p.start()
    p.join()
