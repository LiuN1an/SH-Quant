ref = 0


def reset(i: int):
    global ref
    if i is not None:
        ref = int(i)


def g_id():
    global ref
    ref += 1
    return str(ref)


def resetId():
    global ref
    ref = 0


condition_ref = 0


def increaseCondition():
    global condition_ref
    condition_ref += 1
    return condition_ref


def resetCondition():
    global condition_ref
    condition_ref = 0
