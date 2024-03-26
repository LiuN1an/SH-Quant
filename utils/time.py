from config import FORMAT
from datetime import datetime, timedelta


trades = []
with open("trade_day.txt", "r") as file:
    for line in file:
        trades.append(line.strip())


def ms(ymd):
    return datetime.strptime(f"{ymd} 09:00:00", FORMAT)


def mms(ymd):
    return datetime.strptime(f"{ymd} 10:15:00", FORMAT)


def mme(ymd):
    return datetime.strptime(f"{ymd} 10:30:00", FORMAT)


def me(ymd):
    return datetime.strptime(f"{ymd} 11:30:00", FORMAT)


def afs(ymd):
    return datetime.strptime(f"{ymd} 13:30:00", FORMAT)


def afe(ymd):
    return datetime.strptime(f"{ymd} 15:00:00", FORMAT)


def ns(ymd):
    return datetime.strptime(f"{ymd} 21:00:00", FORMAT)


def ne(ymd, type="p"):
    if type == "p":
        return datetime.strptime(f"{ymd} 23:00:00", FORMAT)
    if type == "sc":
        ymd = (datetime.strptime(f"{ymd}", "%Y%m%d") + timedelta(days=1)).strftime(
            "%Y%m%d"
        )
        return datetime.strptime(f"{ymd} 02:30:00", FORMAT)


def formatDate(dt: datetime):
    return dt.strftime(FORMAT)


# gap目前不要跨大时间段
def getNextK(dt: datetime, base_unit=60, unit=1, type="p"):
    gap = base_unit * unit
    [ymd, _] = formatDate(dt).split(" ")
    if type == "sc":
        if dt.hour <= 2:
            # 证明过夜了, 日期应该减一天
            ymd = (datetime.strptime(f"{ymd}", "%Y%m%d") - timedelta(days=1)).strftime(
                "%Y%m%d"
            )
    nxt_k = dt + timedelta(seconds=gap)

    if dt >= ms(ymd) and dt <= mms(ymd):
        if nxt_k > mms(ymd):
            _gap_dt = mms(ymd) - dt
            return mme(ymd) + timedelta(seconds=gap - _gap_dt.seconds)
        else:
            return nxt_k
    elif dt > mme(ymd) and dt <= me(ymd):
        if nxt_k > me(ymd):
            _gap_dt = me(ymd) - dt
            return afs(ymd) + timedelta(seconds=gap - _gap_dt.seconds)
        else:
            return nxt_k
    elif dt >= afs(ymd) and dt <= afe(ymd):
        if nxt_k > afe(ymd):
            _gap_dt = afe(ymd) - dt
            nxt_dt = getNextTradeDay(dt)
            if nxt_dt:
                _g = nxt_dt - dt
                if _g.days <= 3 and _g.days != 2:
                    return ns(ymd) + timedelta(seconds=gap - _gap_dt.seconds)
                else:
                    [_ymd, _] = formatDate(nxt_dt).split(" ")
                    return ms(_ymd) + timedelta(seconds=gap - _gap_dt.seconds)
        else:
            return nxt_k
    elif dt >= ns(ymd) and dt <= ne(ymd, type=type):
        if nxt_k > ne(ymd, type=type):
            _gap_dt = ne(ymd, type=type) - dt
            if type == "sc":
                if dt.hour <= 2:
                    dt = dt - timedelta(days=1)
                    nxt_dt = getNextTradeDay(dt)
                else:
                    nxt_dt = getNextTradeDay(dt)
            if type == "p":
                nxt_dt = getNextTradeDay(dt)
            if nxt_dt:
                [nxt_ymd, _] = formatDate(nxt_dt).split(" ")
                return ms(nxt_ymd) + timedelta(seconds=gap - _gap_dt.seconds)
        else:
            return nxt_k


def getPrevK(dt: datetime, base_unit=60, unit=1, type="p"):
    gap = base_unit * unit
    [ymd, _] = formatDate(dt).split(" ")
    if type == "sc":
        if dt.hour <= 2:
            ymd = (datetime.strptime(f"{ymd}", "%Y%m%d") - timedelta(days=1)).strftime(
                "%Y%m%d"
            )
    prev_k = dt - timedelta(seconds=gap)

    if dt > ns(ymd) and dt <= ne(ymd, type=type):
        if prev_k < ns(ymd):
            _gap_dt = dt - ns(ymd)
            return afe(ymd) - timedelta(seconds=gap - _gap_dt.seconds)
        elif prev_k == ns(ymd):
            return afe(ymd)
        else:
            return prev_k
    elif dt > afs(ymd) and dt <= afe(ymd):
        if prev_k < afs(ymd):
            _gap_dt = dt - afs(ymd)
            return me(ymd) - timedelta(seconds=gap - _gap_dt.seconds)
        elif prev_k == afs(ymd):
            return me(ymd)
        else:
            return prev_k
    elif dt > mme(ymd) and dt <= me(ymd):
        if prev_k < mme(ymd):
            _gap_dt = dt - mme(ymd)
            return mms(ymd) - timedelta(seconds=gap - _gap_dt.seconds)
        elif prev_k == mme(ymd):
            return mms(ymd)
        else:
            return prev_k
    elif dt > ms(ymd) and dt <= mms(ymd):
        if prev_k <= ms(ymd):
            _gap_dt = dt - ms(ymd)
            last_dt = getLastTradeDay(dt)
            if last_dt:
                _g = dt - last_dt
                [last_ymd, _] = formatDate(last_dt).split(" ")
                if _g.days <= 3 and _g.days != 2:
                    return ne(last_ymd, type=type) - timedelta(
                        seconds=gap - _gap_dt.seconds
                    )
                else:
                    return afe(last_ymd) - timedelta(seconds=gap - _gap_dt.seconds)
        else:
            return prev_k


def getTradesByRange(start_dt: datetime, end_dt: datetime):
    global trades
    start_day = start_dt.strftime("%Y-%m-%d")
    end_day = end_dt.strftime("%Y-%m-%d")
    _reals = []
    for trade in trades:
        if trade >= start_day and trade <= end_day:
            _reals.append(datetime.strptime(trade, "%Y-%m-%d").strftime("%Y%m%d"))
    return _reals


def getNextTradeDay(dt: datetime):
    [current_ymd, current_hms] = dt.strftime("%Y-%m-%d %H:%M:%S").split(" ")
    valid_trade_index = trades.index(current_ymd) if current_ymd in trades else -1
    if valid_trade_index > -1:
        valid_ymd = trades[valid_trade_index + 1]
        return dt.strptime(f"{valid_ymd} {current_hms}", "%Y-%m-%d %H:%M:%S")
    else:
        return None


def getLastTradeDay(dt: datetime):
    [current_ymd, current_hms] = dt.strftime("%Y-%m-%d %H:%M:%S").split(" ")
    valid_trade_index = trades.index(current_ymd) if current_ymd in trades else -1
    if valid_trade_index > -1:
        if valid_trade_index == 0:
            return None
        valid_ymd = trades[valid_trade_index - 1]
        return dt.strptime(f"{valid_ymd} {current_hms}", "%Y-%m-%d %H:%M:%S")
    else:
        return None
