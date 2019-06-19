from . import cloudtrail
from . import okta
from . import azure

__all__ = ['cloudtrail', 'okta', 'azure']

connectors = {
    'cloudtrail': cloudtrail,
    'okta': okta,
    'azure': azure,
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
