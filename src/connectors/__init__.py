from . import cloudtrail
from . import okta
from . import azure_log

__all__ = ['cloudtrail', 'okta', 'azure_log']

connectors = {
    'cloudtrail': cloudtrail,
    'okta': okta,
    'azure_log': azure_log,
}

CONNECTION_OPTIONS = [
    {
        'connector': name,
        'options': getattr(connector, 'CONNECTION_OPTIONS'),
        'docstring': connector.__doc__,
        'finalize': callable(getattr(connector, 'finalize', None)),
    } for name, connector in connectors.items() if (
        getattr(connector, 'CONNECTION_OPTIONS', [{}])[0].get('name')
        and callable(getattr(connector, 'connect', None))
    )
]
