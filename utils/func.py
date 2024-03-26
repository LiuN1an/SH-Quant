from utils.local import LocalStore
from utils.base import plus, subtract
from config import FORMAT
from logger.op import OpLog
from logger.message import messager
from typing import List, Callable, Dict
from order.index import CTP
from datetime import datetime
from functools import cmp_to_key

"""
该文件主要用于基于ctp(下单接口)来自行封装在盘中的监控价格下单, 监控价格止盈止损, 撤单等基本操作
注意: LocalStore是一个贯穿本系统始终的一个类, 其用于在各个环节来传递可被共享的所有变量
"""

def empty(*args, **kwargs):
    pass


# 下单开仓
def openPos(
    local: LocalStore,
    price: float,
    volume: int,
    direction: str,
    onTrade: Callable, # 成交后回调
    onOrder: Callable, # 报单被接收后回调
    time: str = "",
    pos_extra: Dict = {},
):
    ctp: CTP = local["ctp"]

    def cb(stage, order):
        if stage == "OnRtnTrade":
            onTrade(order)

        if stage == "OnRtnOrder":
            onOrder(order)

    return ctp.open(
        price=price,
        direction=direction,
        volume=volume,
        callback=cb,
        time=time,
        behavior="buy" if direction == "long" else "sell",
        pos_extra=pos_extra,
    )


# 下单平仓
def close(
    local: LocalStore,
    price: float,
    volume: int,
    direction: str,
    relative_open_ref: str,
    onEnd: Callable,
    time: str = "",
    type: str = "limit",
    pos_extra: Dict = {},
):
    # 下止盈单的同时需要监听盘口价格是否达到止盈价
    g_ctp: CTP = local["ctp"]
    OpLogger: OpLog = local["op_logger"]

    def callback(stage: str, info: Dict):
        # TODO: 在止盈未成交的时候观察止损盘口, 止损盘口产生信号时撤掉止盈单,下一个止损单
        nonlocal relative_open_ref, onEnd, g_ctp
        direction = info["direction"]
        price = info["price"]
        volume = info["volume"]
        time = info["time"]

        if stage == "OnRtnTrade":
            OpLogger.print(
                f"{info['id']} 平仓成交: 方向「{direction}」 价格「{price}」 数量「{volume}」 时间:{time}"
            )
            messager.send(
                {
                    "text": f"{info['id']} 平仓成交: 方向「{direction}」 价格「{price}」 数量「{volume}」 时间:{time}"
                }
            )
            # 当限价止盈止损时, 止盈单成交后应该立马清除止损仓位观察
            g_ctp.removeTriggerByRelativeOpenRef(
                relative_open_ref=relative_open_ref, time=time
            )
            g_ctp.removeTriggerByRef(ref=relative_open_ref, time=time)
            onEnd(info)

        if stage == "OnRtnOrder":
            OpLogger.print(
                f"{info['id']} 平仓正在候选: {stage} 方向「{direction}」 价格「{price}」 数量「{volume}」 时间:{time}"
            )
            messager.send(
                {
                    "text": f"{info['id']} 平仓正在候选(未成交): {stage} 方向「{direction}」 价格「{price}」 数量「{volume}」 时间:{time}"
                }
            )
            status = info.get("status", None)
            if status == 3 or status == "3":
                OpLogger.print(f"{price} 未成交 时间: {time}")

    # 下面展示的是限价单的接口和市价单的接口
    if type == "limit":
        return g_ctp.close(
            price=price,
            direction=direction,
            volume=volume,
            callback=callback,
            relative_open_ref=relative_open_ref,
            time=time,
            behavior="sell" if direction == "long" else "buy",
            pos_extra=pos_extra,
        )
    elif type == "any":
        return g_ctp.anyClose(
            direction=direction,
            volume=volume,
            callback=callback,
            relative_open_ref=relative_open_ref,
            time=time,
            behavior="sell" if direction == "long" else "buy",
            pos_extra=pos_extra,
        )


# 监控价格下单, 调用后会在盘口形成一个监控器, 当捕捉到对应条件时会立马调用onEnd回调函数
def pureWatch(
    local: LocalStore,
    price: float,
    compare: str,
    onEnd: Callable,
    time: str,
    extra: Dict = {},
    direction: str = "",
):
    ctp: CTP = local["ctp"]

    def cb(data):
        onEnd(data)

    return ctp.watch(price=price, compare=compare, callback=cb, time=time, extra=extra)


# 撤回下单并直接平仓, 注意这里需要传递撤回的那个单的id
def revokeAndClose(
    local: LocalStore,
    with_draw_id: str,
    price: float,
    volume: int,
    direction: str,
    relative_open_ref: str,
    onEnd: Callable,
    time: str,
    type: str = "limit",
):
    ctp: CTP = local["ctp"]
    OpLogger: OpLog = local["op_logger"]

    def withDrawProfit(_, order):
        nonlocal direction, relative_open_ref, price, volume, local, onEnd
        status = order.get("status", None)
        if status == 5 or status == "5":
            OpLogger.print(f"立即撤单的回调 撤单的仓位id: {with_draw_id}")
            messager.send({"text": f"立即撤单的回调 撤单的仓位id: {with_draw_id}"})
            ctp.removeTriggerByWithDraw(relative_open_ref=relative_open_ref)
            close(
                local=local,
                price=price,
                volume=volume,
                direction=direction,
                relative_open_ref=relative_open_ref,
                onEnd=onEnd,
                time=order["time"],
                type=type,
            )

    ctp.withDraw(id=with_draw_id, callback=withDrawProfit, time=time)


# 工具函数, 用来获取盘中没有关闭的所有仓位
def getUnCompletePos(local: LocalStore, time: str):
    # 获取到盘中时段所有的未能关闭的仓位
    pos: Dict = local["ctp"].position_info
    v_pos = list(pos.values())
    keys = list(pos.keys())
    refs = []
    for key in keys:
        current_pos = pos[key]
        if current_pos.get("behavior", None) == "open":
            refs.append(current_pos.get("id"))
    uncomplete = []  # 包含未成交的,撤单的
    base_dt = datetime.strptime(time, FORMAT)

    for ref in refs:
        _find = []
        for index, item in enumerate(v_pos):
            if (
                item.get("behavior", None) == "close"
                and item.get("relative_open_ref", None) == ref
            ):
                _find.append(index)
        if len(_find) == 0:
            # TODO: 这里会走到么，因为只要开仓，一定会下一个止盈单，这个单子就会存储在仓位信息里
            # 如果没有平仓的相关仓位记录，那证明要不就是开仓中，要不就是开仓撤单
            current_dt = datetime.strptime(pos[ref]["time"], FORMAT)
            if current_dt > base_dt:
                uncomplete.append(pos[ref])
        else:
            if len(_find) > 1:  # 只会存在于止损的情况下，止盈撤单，止损不一定成交
                find_pos = list(map(lambda x: v_pos[x], _find))

                def compared(a, b):
                    a_t = a["time"]
                    b_t = b["time"]
                    if a_t < b_t:
                        return -1
                    elif a_t > b_t:
                        return 1
                    else:
                        return 0

                finds = sorted(find_pos, key=cmp_to_key(compared))
                p = finds[-1]
                uncomplete.append(p)
            elif (
                len(_find) == 1
            ):  # 只有一笔止盈记录，你还需要观察这个记录是否被撤单，1、如果成交就证明该仓位结束，2、如果未成交，则证明没有打到止损需要立即止损闭仓，3、如果撤回，就证明一定会有一个止损仓位信息
                current_dt = datetime.strptime(v_pos[_find[0]]["time"], FORMAT)
                if current_dt > base_dt:
                    uncomplete.append(v_pos[_find[0]])
    return uncomplete
