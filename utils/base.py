from decimal import Decimal, getcontext

getcontext().prec = 10


def plus(*args):
    sum = Decimal("0")
    for arg in args:
        sum += Decimal(str(arg))
    return float(sum)


def subtract(*args):
    sum = None
    for arg in args:
        if sum is None:
            sum = Decimal(str(arg))
        else:
            sum -= Decimal(str(arg))
    return float(sum)


def multiply(*args):
    sum = None
    for arg in args:
        if sum is None:
            sum = Decimal(str(arg))
        else:
            sum *= Decimal(str(arg))
    return float(sum)


def divide(*args):
    sum = None
    for arg in args:
        if sum is None:
            sum = Decimal(str(arg))
        else:
            sum /= Decimal(str(arg))
    return float(sum)
