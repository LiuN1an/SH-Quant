import sys
import os

parent_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(parent_dir, ".."))

import os
import pandas as pd
from typing import List
from datetime import datetime, timedelta
from utils.time import getTradesByRange, mme
from utils.base import KList, MAList
from multiprocessing import cpu_count, Pool
from multiprocessing.pool import AsyncResult
from history.params import get
from config import FORMAT
from typing import Dict
from utils.local import LocalStore
from utils.base import plus
from order.index import CTP

from components.Strategy.new import start as _exec

from logger.op import OpLog
from history.analysis import analysis, volatility
import json
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--start", type=str, default="20230601")
parser.add_argument("--end", type=str, default="20231130")
parser.add_argument("--t", type=str, default="p")
parser.add_argument("--u", type=str, default="1,5")
parser.add_argument("--ma", type=str, default="5,10")

parser.add_argument("--bar", action="store_true")
parser.add_argument("--no-bar", dest="local", action="store_false")
parser.set_defaults(bar=False)


args = parser.parse_args()
start = args.start
end = args.end
t = args.t
k_unit_str: str = args.u
k_unit = k_unit_str.split(",")
bar = args.bar  # 是否基于bar回测
MA_RANGE = list(filter(lambda x: int(x), args.ma.split(",")))


original_start_dt = datetime.strptime(start, "%Y%m%d")
original_end_dt = datetime.strptime(end, "%Y%m%d")


# 这里拿原油来举例
_TYPE = {
    "sc": {
        "contract": "sc",  # 品种的代号
        "unit": 100,  # 每一跳的价格
        "jump_unit": 0.1,  # 每一跳的单位
    },
}

CONTRACT = _TYPE[t]["contract"]

UNIT = _TYPE[t]["unit"]

JUMP_UNIT = _TYPE[t]["jump_unit"]

RATIO = _TYPE[t]["margin_ratio"]

DATA_PATH = f"data_{CONTRACT}"

MAX_DAY_PROCESSING = 5  # 最大并发的天数, 其上限取决于本地CPU的速度

MAX_RUN_PARAMS = 100  # 最大运行的参数排列组合数量, 其上限取决于本地CPU的速度

vision_folder = os.path.join(
    os.getcwd(), DATA_PATH, "details"
)  # 将写入日内开仓的详细信息

if not os.path.exists(vision_folder):
    os.mkdir(vision_folder)


def simulate_process(
    data,
    ctp: CTP,
    local: LocalStore,
    k_lists: Dict[str, KList],
    ma_lists: Dict[str, MAList],
    is_k=False,
    unit=1,
):
    global bar
    # start_time = time.perf_counter()
    if bar:
        ctp.probBar(data)
    else:
        ctp.prob(data)  # 监听盘口
    # msg的格式为{ time, price, volume }
    # 获取整点的datetime时间戳
    if is_k:
        # value = k_list[data["time"]]
        dt = datetime.strptime(data["time"], FORMAT)
        dt = dt - timedelta(seconds=dt.second)
        local["op_logger"].print(
            "------------------- " + dt.strftime(FORMAT) + " -------------------"
        )
        # print(data["time"])
        _exec(time=dt, k_lists=k_lists, ma_lists=ma_lists, local=local, unit=unit)
    # end_time = time.perf_counter()
    # print("cost ", (end_time - start_time) * 1000, " 毫秒 ", data["time"])


def progress_bar(current, total, bar_length=20, prefix=""):
    fraction = current / total
    arrow = int(fraction * bar_length - 1) * "-" + ">"
    padding = int(bar_length - len(arrow)) * " "
    ending = "\n" if current == total else "\r"
    print(
        f"[ {prefix} ] Progress: [{arrow}{padding}] {int(fraction*100)}%",
        end=ending,
    )


def is_new_k(last_dt: datetime, current_dt: datetime, unit: int = 1):
    if last_dt is not None:
        if current_dt.hour == 13 and current_dt.minute == 30:
            return False
        if current_dt.hour == 21 and current_dt.minute == 00:
            return False
        if current_dt.hour == 9 and current_dt.minute == 00:
            return False
        if current_dt.hour == 10 and current_dt.minute == 30:
            return False
        return (
            last_dt.hour != current_dt.hour or last_dt.minute != current_dt.minute
        ) and (current_dt.minute % unit == 0)
    else:
        return False


def format_order_time(dt: datetime):
    if dt.hour == 8:
        return datetime.strptime(f"{dt.strftime('%Y%m%d')} 09:00:00", "%Y%m%d %H:%M:%S")
    if (dt - timedelta(seconds=dt.second)) == mme(dt.strftime("%Y%m%d")):
        return datetime.strptime(f"{dt.strftime('%Y%m%d')} 10:15:00", "%Y%m%d %H:%M:%S")
    if dt.hour == 15:
        return datetime.strptime(f"{dt.strftime('%Y%m%d')} 15:00:00", "%Y%m%d %H:%M:%S")
    if dt.hour == 20:
        return datetime.strptime(f"{dt.strftime('%Y%m%d')} 21:00:00", "%Y%m%d %H:%M:%S")
    if t != "sc":
        if dt.hour == 23:
            return datetime.strptime(
                f"{dt.strftime('%Y%m%d')} 23:00:00", "%Y%m%d %H:%M:%S"
            )

    return dt


def func(
    param: Dict,
    day: str,
    order_list: list,
    k_lists: Dict[str, KList],
    ma_lists: Dict[str, MAList],
    is_print: bool = False,
):
    local_store = LocalStore()
    local_store.init(param)
    local_store["op_logger"]: OpLog = OpLog(
        filename=os.path.join(os.getcwd(), DATA_PATH, "_log", "h_op.log"),
        is_print=is_print,
        is_store=False,
        is_sync=True,
    )
    ctp = CTP(local=local_store, unit=JUMP_UNIT)
    ctp.unreal()
    ctp.setToday(day)
    local_store.add("ctp", ctp)  # ‼️ 这里初始化的变量将会被系统的其他很多地方用到
    local_store.add("t", t)
    local_store.add("volume", 1)
    last_dt: datetime = None
    if bar:
        ks = k_lists[k_unit[0]]
        start_time = datetime.strptime(f"{day} 09:05:00", "%Y%m%d %H:%M:%S")
        end_time = datetime.strptime(f"{day} 23:00:00", "%Y%m%d %H:%M:%S")
        for k in ks.loop(start_time=start_time, end_time=end_time, unit=int(k_unit[0])):
            simulate_process(
                data=k,
                ctp=ctp,
                local=local_store,
                k_lists=k_lists,
                ma_lists=ma_lists,
                is_k=True,
                unit=int(k_unit[0]),
            )
    else:
        for order in order_list:
            # 订单流处理成{ time, price, volume }的格式, 其中time是"%Y%m%d %H:%M:%S"字符串
            if order.get("Date", None) is None:
                value = list(order.values())
                date = value[0]
                price = value[1]
                volume = value[2]
            else:
                date = order["Date"]
                price = order["Price"]
                volume = order["Volume"]
            current_dt: datetime = format_order_time(datetime.strptime(date, FORMAT))
            simulate_process(
                data={
                    "time": current_dt.strftime(FORMAT),
                    "price": price,
                    "volume": volume,
                },
                ctp=ctp,
                local=local_store,
                k_lists=k_lists,
                ma_lists=ma_lists,
                is_k=is_new_k(last_dt, current_dt, unit=int(k_unit[0])),
            )
            last_dt = current_dt

    return {
        "real_pos": ctp.stats(),
        "day": day,
        "name": "-".join(list(map(lambda x: str(x), list(param.values())))),
        "param": param,
    }


def getList(start_str: str, end_str: str, is_bar: bool):
    # 订单流数据
    order_path = os.path.join(os.getcwd(), DATA_PATH, "TICK")
    order_list = []
    if not is_bar:
        for item in os.listdir(order_path):
            item_path = os.path.join(order_path, item)
            day = item[:-4]
            if day >= start_str and day <= end_str:
                df = pd.read_csv(item_path)
                order_list += df.to_dict("records")

    k_lists = {}
    for k in k_unit:
        # k线数据
        k_path = os.path.join(os.getcwd(), DATA_PATH, "K", f"{k}m")
        k_list = KList()
        for item in os.listdir(k_path):
            item_path = os.path.join(k_path, item)
            day = item[:-4]
            if day >= start_str and day <= end_str:
                df = pd.read_csv(item_path)
                for _, record in df.iterrows():
                    k_list.add(
                        record["Date"],
                        [
                            record["Open"],
                            record["Close"],
                            record["High"],
                            record["Low"],
                        ],
                    )
        k_lists[k] = k_list

    ma_k_lists = {}
    for k in k_unit:
        ma_lists = {}
        for ma_unit in MA_RANGE:
            # 均线数据
            ma_path = os.path.join(os.getcwd(), DATA_PATH, "MA", f"{k}m", str(ma_unit))
            ma_list = MAList(ma=ma_unit)
            for item in os.listdir(ma_path):
                item_path = os.path.join(ma_path, item)
                day = item[:-4]
                if day >= start_str and day <= end_str:
                    df = pd.read_csv(item_path)
                    for _, record in df.iterrows():
                        ma_list.add(record["Date"], record["Price"])
            ma_lists[str(ma_unit)] = ma_list
        ma_k_lists[k] = ma_lists

    return {"order": order_list, "k": k_lists, "ma": ma_k_lists}


def start(start_time: datetime, end_time: datetime):
    global t, bar
    available_days: List[str] = getTradesByRange(start_time, end_time)
    all_count = len(available_days)
    is_print = start_time == end_time

    current_day_index = 0
    all_strategy = {}

    def combine(result):
        if result["name"] in all_strategy:
            all_strategy[result["name"]][result["day"]] = result
        else:
            all_strategy[result["name"]] = {result["day"]: result}

    while True:
        chunk_available_days = available_days[
            current_day_index : current_day_index + MAX_DAY_PROCESSING
        ]

        for ymd in chunk_available_days:
            pool = Pool(cpu_count() - 2)
            result = getList(ymd, ymd, bar)
            order_list = result["order"]
            k_lists = result["k"]
            ma_lists = result["ma"]
            all_params = get(t)
            param_index = 0
            while param_index < len(all_params):
                turn_end_index = min(param_index + MAX_RUN_PARAMS, len(all_params))
                result_queue: List[AsyncResult] = []
                while param_index < turn_end_index:
                    result_queue.append(
                        pool.apply_async(
                            func,
                            (
                                all_params[param_index],
                                ymd,
                                order_list,
                                k_lists,
                                ma_lists,
                                is_print,
                            ),
                        )
                    )
                    param_index += 1
                results = []
                for _, item in enumerate(result_queue):
                    rsp = item.get()
                    results.append(rsp)
                for result in results:
                    combine(result)
            pool.close()

        current_day_index += MAX_DAY_PROCESSING
        progress_bar(current_day_index, all_count, prefix="Process Days")
        if current_day_index >= all_count:
            break

    saves = []
    for key, value in all_strategy.items():
        print("------", key, "------")
        total = 0
        port_positives = []
        port_negtives = []
        positive = []
        negtive = []
        all_money = 100000
        days = []
        path = os.path.join(vision_folder, key)
        if not os.path.exists(path):
            os.mkdir(path)
        for day, vv in value.items():
            days.append(day)
            with open(
                os.path.join(path, f"{day}.json"), mode="w+", encoding="utf-8"
            ) as file:
                json.dump(vv, file, indent=4, ensure_ascii=False)

            rsp = analysis(vv, all_money, day, UNIT, JUMP_UNIT, RATIO)
            t = rsp["total"]
            all_money = rsp["all_money"]
            port_positives += rsp["positives"]
            port_negtives += rsp["negtives"]
            total = plus(total, t)

        print("总盈利点数: ", total)
        if len(port_negtives) == 0:
            print("开仓盈利胜率占比: 100%")
        else:
            print(
                "开仓盈利胜率占比:",
                len(port_positives) / (len(port_positives) + len(port_negtives)) * 100,
                "%",
            )
        print(
            "日盈利胜率占比: ",
            len(positive) / (len(positive) + len(negtive)) * 100,
            "%",
        )

        if len(positive):
            print("盈利日平均盈利: ", sum(positive) / len(positive))
        if len(negtive):
            print("亏损日平均亏损: ", sum(negtive) / len(negtive))
            if len(negtive) != 1:
                print("亏损日波动: ", volatility(negtive))
        print("实际复利:", all_money)

        saves.append(
            {
                "总盈利点数": total,
            }
        )
        print("############")

    print("\nFinish")
    save_df = pd.DataFrame(saves)
    writer = pd.ExcelWriter(
        os.path.join(os.getcwd(), DATA_PATH, "analysis", "index.xlsx"),
        engine="xlsxwriter",
    )

    save_df.to_excel(
        writer,
        sheet_name="分析",
        index=False,
    )
    writer.close()


if __name__ == "__main__":
    start(start_time=original_start_dt, end_time=original_end_dt)
