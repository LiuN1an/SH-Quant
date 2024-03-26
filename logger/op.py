import logging
import multiprocessing


class OpLog:
    def __init__(self, filename, is_print=True, is_store=False, is_sync=False):
        self.filename = filename
        self.is_store = is_store
        if self.is_store:
            self.queue = multiprocessing.Queue()
            self.process = multiprocessing.Process(target=self._write_to_file)
        self.is_print = is_print
        self.is_sync = is_sync

    def start(self):
        if self.is_store:
            self.process.start()

    def stop(self):
        if self.is_store:
            self.queue.put(None)
            self.process.join()

    def print(self, op_msg: str):
        if self.is_print:
            print(op_msg)
        if self.is_store:
            if self.is_sync:
                self._write_to_file()
            else:
                self.queue.put(op_msg)

    def _write_to_file_sync(self, msg: str):
        with open(self.filename, mode="a+") as file:
            file.write(msg)
            file.write("\n")

    def _write_to_file(self):
        logging.basicConfig(
            filename=self.filename, level=logging.DEBUG, encoding="utf-8"
        )

        while True:
            op_msg = self.queue.get()
            if op_msg is None:
                break
            logging.debug(op_msg)
