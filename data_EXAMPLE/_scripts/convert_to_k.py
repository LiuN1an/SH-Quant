import sys
import os

# 获取当前文件所在目录的父目录
parent_dir = os.path.dirname(os.path.abspath(__file__))

# 将虚拟环境的父目录添加到sys.path中
sys.path.append(os.path.join(parent_dir, ".."))

from datetime import datetime, timedelta
import pandas as pd
import os
from config import FORMAT
from utils.time import getNextK, getPrevK
from typing import List

TYPE = "data_EXAMPLE"

order_path = os.path.join(os.getcwd(), TYPE, "ORDER")

k_path = os.path.join(os.getcwd(), TYPE, "K")

auction_path = os.path.join(os.getcwd(), TYPE, "AUCTION")


def mapDatetime(dt: datetime):
    ymd = dt.strftime("%Y%m%d")
    if dt == datetime.strptime(f"{ymd} 10:29:00", FORMAT):
        return datetime.strptime(f"{ymd} 10:15:00", FORMAT)
    if dt == datetime.strptime(f"{ymd} 11:59:00", FORMAT):
        return datetime.strptime(f"{ymd} 11:30:00", FORMAT)
    if dt == datetime.strptime(f"{ymd} 15:59:00", FORMAT):
        return datetime.strptime(f"{ymd} 15:00:00", FORMAT)
    if dt == datetime.strptime(f"{ymd} 23:59:00", FORMAT):
        return datetime.strptime(f"{ymd} 23:00:00", FORMAT)
    return dt


def getValidRange(ymd: str):
    ranges = []
    current_dt = datetime.strptime(f"{ymd} 09:01:00", FORMAT)
    while True:
        if current_dt.hour == 9 and current_dt.minute == 1:
            ranges.append([datetime.strptime(f"{ymd} 08:30:00", FORMAT), current_dt])
        elif current_dt.hour == 10 and current_dt.minute == 15:
            ranges.append(
                [
                    datetime.strptime(f"{ymd} 10:14:00", FORMAT),
                    datetime.strptime(f"{ymd} 10:29:00", FORMAT),
                ]
            )
        elif current_dt.hour == 10 and current_dt.minute == 31:
            ranges.append([datetime.strptime(f"{ymd} 10:30:00", FORMAT), current_dt])
        elif current_dt.hour == 11 and current_dt.minute == 30:
            ranges.append(
                [
                    datetime.strptime(f"{ymd} 11:29:00", FORMAT),
                    datetime.strptime(f"{ymd} 11:59:00", FORMAT),
                ]
            )
        elif current_dt.hour == 13 and current_dt.minute == 31:
            ranges.append([datetime.strptime(f"{ymd} 13:30:00", FORMAT), current_dt])
        elif current_dt.hour == 15 and current_dt.minute == 0:
            ranges.append(
                [
                    datetime.strptime(f"{ymd} 14:59:00", FORMAT),
                    datetime.strptime(f"{ymd} 15:59:00", FORMAT),
                ]
            )
        elif current_dt.hour == 21 and current_dt.minute == 1:
            ranges.append(
                [
                    datetime.strptime(f"{ymd} 20:59:01", FORMAT),
                    datetime.strptime(f"{ymd} 21:01:00", FORMAT),
                ]
            )
        elif current_dt.hour == 23 and current_dt.minute == 0:
            ranges.append(
                [
                    datetime.strptime(f"{ymd} 22:59:00", FORMAT),
                    datetime.strptime(f"{ymd} 23:59:00", FORMAT),
                ]
            )
            break
        else:
            ranges.append([getPrevK(current_dt), current_dt])
        current_dt = getNextK(current_dt, type="p")

    return ranges


for item_day_path in os.listdir(order_path):
    # print(item_day_path)
    if item_day_path.endswith(".csv"):
        df = pd.read_csv(os.path.join(order_path, item_day_path))
        current_day = item_day_path[:-4]
        valid_range = getValidRange(current_day)
        temp = []
        min_1 = []
        range_index = 0
        print(current_day)
        df_index = 0

        while df_index < len(df):
            # print(df_index)
            item = df.iloc[df_index]
            [start_dt, end_dt] = valid_range[range_index]
            l = item.to_numpy()
            time = l[0]
            if not time.startswith(current_day):
                continue
            price = l[1]
            dt = datetime.strptime(time, FORMAT)
            if dt.hour == 20 and dt.minute == 59 and dt.second == 0:
                ymd = dt.strftime("%Y%m%d")
                new_pd = pd.DataFrame(
                    [{"Date": dt + timedelta(minutes=1), "Price": price}],
                )
                new_pd.to_csv(
                    os.path.join(auction_path, f"{ymd}.csv"), mode="w+", index=False
                )
                df_index += 1
            else:
                if start_dt <= dt and end_dt > dt:
                    temp.append(price)
                    df_index += 1
                elif end_dt <= dt:
                    min_1.append(
                        {
                            "Date": mapDatetime(end_dt).strftime(FORMAT),
                            **(
                                {
                                    "Open": temp[0],
                                    "Close": temp[-1],
                                    "High": max(temp),
                                    "Low": min(temp),
                                }
                                if len(temp) != 0
                                else {}
                            ),
                        }
                    )
                    temp = []
                    range_index += 1

        min_1.append(
            {
                "Date": mapDatetime(end_dt).strftime(FORMAT),
                **(
                    {
                        "Open": temp[0],
                        "Close": temp[-1],
                        "High": max(temp),
                        "Low": min(temp),
                    }
                    if len(temp) != 0
                    else {}
                ),
            }
        )
        df = pd.DataFrame(min_1)
        _dir = os.path.join(os.getcwd(), TYPE, "K", "1m")
        if not os.path.exists(_dir):
            os.mkdir(_dir)

        df.to_csv(os.path.join(_dir, f"{current_day}.csv"), index=False)
