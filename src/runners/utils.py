from datetime import date, datetime
import inspect
from itertools import zip_longest
import json
import traceback
from types import GeneratorType
import yaml


yaml.add_representer(
    str,
    lambda dumper, data: dumper.represent_scalar(
        'tag:yaml.org,2002:str', data, style='|' if '\n' in data else None
    ),
)


NO_FILL = object()


def groups_of(n, iterable, fillvalue=NO_FILL):
    args = [iter(iterable)] * n
    rets = zip_longest(*args, fillvalue=fillvalue)
    return (tuple(l for l in ret if l is not NO_FILL) for ret in rets)


def format_exception(e):
    return ''.join(traceback.format_exception(type(e), e, e.__traceback__))


def format_exception_only(e):
    return ''.join(traceback.format_exception_only(type(e), e)).strip()


def json_dumps(obj, **kwargs):
    kwargs.setdefault('sort_keys', True)

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

        # e.g. requests.Response
        if callable(getattr(x, 'json', None)):
            try:
                return x.json()
            except:
                pass

        # e.g. pandas.DataFrame
        if callable(getattr(x, 'to_json', None)):
            return json.parse(x.to_json())

        if hasattr(x, 'raw'):
            return default_json_dumps(x.raw)

        if type(x) is GeneratorType:
            return list(x)

        return repr(x)

    return json.dumps(obj, default=default_json_dumps, **kwargs)


def apply_some(f, **kwargs):
    spec = inspect.getfullargspec(f)
    defaults = dict(zip(reversed(spec.args), reversed(spec.defaults or ())))
    passed_in = {arg: kwargs[arg] for arg in spec.args if arg in kwargs}
    defaults.update(passed_in)
    return f(**defaults)
