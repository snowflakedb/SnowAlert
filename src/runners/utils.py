from itertools import zip_longest


def groups_of(n, iterable, fill=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fill)
