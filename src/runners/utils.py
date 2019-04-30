from itertools import zip_longest
import traceback

NO_FILL = object()


def groups_of(n, iterable, fillvalue=NO_FILL):
    args = [iter(iterable)] * n
    rets = zip_longest(*args, fillvalue=fillvalue)
    return (tuple(l for l in ret if l is not NO_FILL) for ret in rets)


def format_exception(e):
    return ''.join(traceback.format_exception(type(e), e, e.__traceback__))


def format_exception_only(e):
    return ''.join(traceback.format_exception_only(type(e), e)).strip()


def json_dumps(obj):
    import json

    def default(x):
        if isinstance(x, Exception):
            return {
                "traceback": format_exception(x),
                "exception": format_exception_only(x),
                "exceptionName": x.__class__.__title__,
                "exceptionArgs": x.args,
            }
        else:
            type_name = x.__class__.__name__
            raise TypeError(f"Object of type '{type_name}' is not JSON serializable")

    json.dump(obj, default=default)
