import math
import numpy as np
from typing import Dict, List
from utils.base import KList, plus, subtract, divide, multiply


def analysis(
    dic: Dict, all_money: float, day, unit: int, jump_unit: float, ratio: float
):
    total = 0
    counts = 0
    positives = []
    negtives = []
    longs = []
    shorts = []
    pos_info = dic["real_pos"]
    # print(len(pos_info.keys()))

    for key in pos_info.keys():
        item = pos_info[key]
        if type(item) == dict:
            status = item.get("status", None)
            behavior = item.get("behavior", None)
            if behavior == "close" and status == "RtnTrade":
                close_price = item.get("real_price", None)
                volume = item.get("real_volume", None)
                origin_direction = item.get("direction", None)
                relative_open_ref = item.get("relative_open_ref", None)
                if pos_info[relative_open_ref].get("status", None) == "RtnTrade":
                    open_price = pos_info[relative_open_ref].get("real_price")
                    direction = pos_info[relative_open_ref].get("direction")
                    if origin_direction != direction:
                        print("direction不同: ", day)
                    result = 0
                    # hands = math.ceil(divide(all_money, open_price))
                    hands = math.floor(all_money / (open_price * ratio))
                    # print(all_money, open_price * ratio, hands)
                    if direction == "short":
                        jump = subtract(open_price, close_price)
                        result = plus(result, jump)
                        # all_money = plus(
                        #     all_money,
                        #     multiply(hands, multiply(divide(jump, jump_unit), unit)),
                        # )
                        # jump = open_price - close_price
                        # result += jump
                        all_money += hands * (jump / jump_unit) * unit
                        # shorts.append(jump)
                    else:
                        # jump = close_price - open_price
                        # result += jump
                        jump = subtract(close_price, open_price)
                        result = plus(result, jump)
                        # all_money = plus(
                        #     all_money,
                        #     multiply(hands, multiply(divide(jump, jump_unit), unit)),
                        # )
                        all_money += hands * (jump / jump_unit) * unit
                        longs.append(jump)
                    counts += 1
                    total = plus(total, result)
                    if result >= 0:
                        positives.append(result)
                    else:
                        negtives.append(result)
                else:
                    # print(pos_info["time"])
                    pass
    # print("all_money: ", all_money, total, day)
    return {
        "total": total,
        "positives": positives,
        "negtives": negtives,
        "counts": counts,
        "all_money": all_money if all_money > 0 else 0,
        "longs": longs,
        "shorts": shorts,
    }


def volatility(l: List[float]):
    prices = np.array(l)

    # 计算价格的对数收益率
    returns = np.log(prices[1:] / prices[:-1])

    # 计算对数收益率的标准差
    volatility = np.std(returns)

    return volatility
