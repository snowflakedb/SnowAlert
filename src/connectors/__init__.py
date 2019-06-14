from . import cloudtrail
from . import okta
from . import azure_log

__all__ = ['cloudtrail', 'okta']

connectors = {
    'cloudtrail': cloudtrail,
    'okta': okta,
    'azure log': azure_log,
}

CONNECTION_OPTIONS = [
    {
        'connector': name,
        'options': getattr(connector, 'CONNECTION_OPTIONS', {}),
        'docstring': connector.__doc__,
        'finalize': callable(getattr(connector, 'finalize', None)),
    } for name, connector in connectors.items()
]
