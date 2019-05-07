from datetime import date, datetime
import inspect
from itertools import zip_longest
import json
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
    def default_json_dumps(x):
        if isinstance(x, Exception):
            return {
                "traceback": format_exception(x),
                "exception": format_exception_only(x),
                "exceptionName": x.__class__.__name__,
                "exceptionArgs": x.args,
            }

        if isinstance(x, (date, datetime)):
            return x.isoformat()

        if hasattr(x, 'raw'):
            return default_json_dumps(x.raw)

        if callable(getattr(x, 'to_json', None)):
            return json.parse(x.to_json())

        return repr(x)

    return json.dumps(obj, default=default_json_dumps)


def apply_some(f, **kwargs):
    spec = inspect.getfullargspec(f)
    defaults = dict(zip(reversed(spec.args), reversed(spec.defaults or ())))
    passed_in = {arg: kwargs[arg] for arg in spec.args if arg in kwargs}
    defaults.update(passed_in)
    return f(**defaults)
