# fastproc/__init__.py

"""
fastproc: A high-performance multiprocessing framework for Python.
Provides Process and Queue implementations with a modern, extensible API.
"""

from .process import Process
from fastproc.fastqueue import Queue, JoinableQueue, SimpleQueue

__all__ = ["Process", "Queue", "JoinableQueue", "SimpleQueue"]
