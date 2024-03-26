import sys
import os

parent_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(parent_dir, ".."))

from datetime import datetime
import pandas as pd
from config import FORMAT
from utils.time import getLastTradeDay
import os

TYPE = "data_EXAMPLE"

order_path = os.path.join(os.getcwd(), TYPE, "ORDER")


datas = []
indexes = []
for file_path in os.listdir(order_path):
    if file_path.endswith(".csv"):
        print(file_path)
        df = pd.read_csv(os.path.join(order_path, file_path), encoding="utf-8")
        del_indexs = []
        night_items = []
        today = datetime.strptime(file_path[:-4], "%Y%m%d")
        last_day = getLastTradeDay(today)

        for i, item in df.iterrows():
            dt = datetime.strptime(item["Date"], FORMAT)
            if dt.hour >= 20 and dt.hour <= 23:
                del_indexs.append(i)
                night_items.append(
                    {
                        "Date": f"{last_day.strftime('%Y%m%d')} {dt.strftime('%H:%M:%S')}",
                        "Price": item["Price"],
                        "Volume": item["Volume"],
                    }
                )
        indexes.append(
            {"path": os.path.join(order_path, file_path), "value": del_indexs}
        )
        datas.append(
            {
                "path": os.path.join(order_path, f"{last_day.strftime('%Y%m%d')}.csv"),
                "value": night_items,
            }
        )

for index, item in enumerate(indexes):
    del_indexs = indexes[index]["value"]
    df = pd.read_csv(indexes[index]["path"])
    print(indexes[index]["path"], "开始删除")
    df = df.drop(df.index[del_indexs])
    df.to_csv(indexes[index]["path"], index=False)
    print(indexes[index]["path"], "删除成功")

for index, item in enumerate(datas):
    night_items = datas[index]
    path = night_items["path"]
    prev_day_df = pd.DataFrame(night_items["value"])
    print(night_items["path"], "开始追加")
    if os.path.exists(path):
        df = pd.read_csv(path)
        df = pd.concat([df, prev_day_df], ignore_index=False)
        df.to_csv(path, index=False)
    else:
        prev_day_df.to_csv(path, index=False)
    print(night_items["path"], "追加成功")
