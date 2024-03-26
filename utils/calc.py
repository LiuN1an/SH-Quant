from typing import List
from datetime import datetime, timedelta
from utils.time import getPrevK, getNextK, mme, mms, afs, me, ns, afe
from config import FORMAT
from functools import cmp_to_key
import math


def compare(a, b):
    if a["time"] < b["time"]:
        return -1
    elif a["time"] > b["time"]:
        return 1
    else:
        return 0


class KList:
    def __init__(self, unit=1) -> None:
        self.dict = {}
        self.unit = unit

    def __contains__(self, item):
        return item in self.dict

    def add(self, time: str, k: List[float]):
        self.dict[time] = k

    def __getitem__(self, key: str):
        return self.dict.get(key, None)

    def __setitem__(self, key, value):
        self.dict[key] = value

    def __len__(self):
        return len(self.dict.keys())

    def prev(self, ref_time: datetime, step: int, unit=1, is_print=False, _type="p"):
        rest = ref_time.minute % unit
        final_time = ref_time - timedelta(minutes=rest)
        [ymd, _] = final_time.strftime(FORMAT).split(" ")
        if final_time == mme(ymd):
            final_time = mms(ymd)
        elif final_time == afs(ymd):
            final_time = me(ymd)
        elif final_time == ns(ymd):
            final_time = afe(ymd)

        while step > 0:
            final_time = getPrevK(final_time, unit=unit, type=_type)
            step -= 1
        time_str = final_time.strftime(FORMAT)
        return self.dict.get(time_str, None)

    def get_by_day(self, day: str):
        start = datetime.strftime("f{day} 09:00:00")
        end = datetime.strftime("f{day} 23:00:00")
        result = []
        for _, key in enumerate(self.dict.keys()):
            current = datetime.strptime(key, FORMAT)
            if start <= current and end >= current:
                k = self.dict[key]
                result.append({"time": key, "value": k})
        return sorted(list(result, key=cmp_to_key(compare)))

    def loop(self, start_time: datetime, end_time: datetime, unit=1, type="p"):
        rest = start_time.minute % unit
        ref_time = start_time - timedelta(minutes=rest)
        k_list = []
        while ref_time <= end_time:
            k = self.dict.get(ref_time.strftime("%Y%m%d %H:%M:%S"), None)
            k_list.append({"time": ref_time.strftime("%Y%m%d %H:%M:%S"), "k": k})
            ref_time = getNextK(ref_time, unit=unit, type=type)
        return k_list


class MAList:
    def __init__(self, unit=1, ma=5) -> None:
        self.dict = {}
        self.unit = unit
        self.ma = ma

    def __contains__(self, item):
        return item in self.dict

    def add(self, time: str, k: float):
        self.dict[time] = k

    def __getitem__(self, key: str):
        return self.dict.get(key, None)

    def __setitem__(self, key, value):
        self.dict[key] = value

    def __len__(self):
        return len(self.dict.keys())

    def prev(self, ref_time: datetime, step: int, unit=1, _type="p"):
        # 只支持60分钟之内
        rest = ref_time.minute % unit
        final_time = ref_time - timedelta(minutes=rest)
        [ymd, _] = final_time.strftime(FORMAT).split(" ")
        if final_time == mme(ymd):
            final_time = mms(ymd)
        elif final_time == afs(ymd):
            final_time = me(ymd)
        elif final_time == ns(ymd):
            final_time = afe(ymd)

        while step > 0 and final_time is not None:
            final_time = getPrevK(final_time, unit=unit, type=_type)
            step -= 1
        if final_time is None:
            return None
        time_str = final_time.strftime(FORMAT)
        rsp = self.dict.get(time_str, None)
        if type(rsp) is float and not math.isnan(rsp):
            return rsp

    def get_by_day(self, day: str):
        start = datetime.strftime("f{day} 09:00:00")
        end = datetime.strftime("f{day} 23:00:00")
        result = []
        for _, key in enumerate(self.dict.keys()):
            current = datetime.strptime(key, FORMAT)
            if start <= current and end >= current:
                value = self.dict[key]
                result.append({"time": key, "value": value})
        return sorted(list(result, key=cmp_to_key(compare)))
