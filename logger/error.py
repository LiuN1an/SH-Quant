import logging
import multiprocessing
from datetime import datetime


class ErrorLog:
    is_print = True

    def __init__(self, filename):
        self.filename = filename
        self.queue = multiprocessing.Queue()
        self.process = multiprocessing.Process(target=self._write_to_file)

    def start(self):
        self.process.start()

    def enablePrint(self):
        self.is_print = True

    def stop(self):
        self.queue.put(None)
        self.process.join()

    def print(self, error_msg: str):
        if self.is_print:
            print(error_msg)
            self.queue.put(
                error_msg + " 时间戳: " + datetime.now().strftime("%Y%m%d %H:%M:%S")
            )

    def _write_to_file(self):
        logging.basicConfig(
            filename=self.filename, level=logging.ERROR, encoding="utf-8"
        )

        while True:
            error_msg = self.queue.get()
            if error_msg is None:
                break
            logging.error(error_msg)
