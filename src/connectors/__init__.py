from . import cloudtrail
from . import okta

__all__ = ['cloudtrail', 'okta']

connectors = {
    'cloudtrail': cloudtrail,
    'okta': okta,
}

CONNECTION_OPTIONS = [
    {
        'connector': name,
        'options': getattr(connector, 'CONNECTION_OPTIONS', {}),
        'docstring': connector.__doc__,
        'finalize': callable(getattr(cloudtrail, 'finalize', None)),
    } for name, connector in connectors.items()
]
