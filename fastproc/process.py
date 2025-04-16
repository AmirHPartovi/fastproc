# fastproc/process.py
import logging
from multiprocessing import Process as MPProcess


class Process(MPProcess):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start(self):
        logging.debug("Starting process with PID: %s", self.pid)
        super().start()
