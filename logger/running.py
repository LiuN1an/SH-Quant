import multiprocessing
import os
import json


class RunningLog:
    def __init__(self, filename):
        self.filename = filename
        self.queue = multiprocessing.Queue()
        self.process = multiprocessing.Process(target=self._write_to_file)

    def start(self):
        self.process.start()

    def stop(self):
        self.queue.put(None)
        self.process.join()

    def print(self, pos: dict):
        self.queue.put(json.dumps(pos))

    def _write_to_file(self):
        while True:
            msg = self.queue.get()
            if msg is None:
                break
            with open(self.filename, mode="w+", encoding="utf-8") as file:
                json.dump(json.loads(msg), file, indent=4, ensure_ascii=False)
