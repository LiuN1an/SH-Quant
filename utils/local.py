import multiprocessing
from typing import Dict
import json


class LocalStore:
    def __init__(self, path: str = "", is_print = False) -> None:
        self.dict = {}
        self.path = path
        self.is_print = is_print
        if self.is_print:
            self.queue = multiprocessing.Queue()
            self.process = multiprocessing.Process(target=self._write_to_file)

    def __getitem__(self, key: str):
        return self.dict.get(key, None)

    def __setitem__(self, key, value):
        self.dict[key] = value
        if self.is_print:
            self.queue.put(True)

    def __contains__(self, item):
        return item in self.dict

    def add(self, key: str, value):
        self.dict[key] = value
        if self.is_print:
            self.queue.put(True)

    def init(self, config: Dict = {}):
        # 基础参数设置
        for key, value in iter(config.items()):
            self.add(key, value)

    def _write_to_file(self):
        while True:
            self.queue.get()
            _json = self.dict.copy()
            with open(self.path, "w+") as file:
                json.dump(_json, file)

    def prob(self):
        if self.is_print:
            self.process.start()

    def load(self, path=""):
        # 加载本地的文件
        store_path = path if path != "" else self.path
