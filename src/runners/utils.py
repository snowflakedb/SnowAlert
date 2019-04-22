from itertools import zip_longest

NO_FILL = object()


def groups_of(n, iterable, fillvalue=NO_FILL):
    args = [iter(iterable)] * n
    rets = zip_longest(*args, fillvalue=fillvalue)
    return (tuple(l for l in ret if l is not NO_FILL) for ret in rets)
