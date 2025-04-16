# examples/simple_process.py

from fastproc import Process
import os
import logging


def work(n):
    logging.info(f"[PID {os.getpid()}] Working on {n}")


if __name__ == "__main__":
    p = Process(target=work, args=(5,))
    p.start()
    p.join()
