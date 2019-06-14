from . import cloudtrail
from . import okta
from . import azure_ad_audit
from . import azure_ad_signin
from . import azure_operation

__all__ = ['cloudtrail', 'okta']

connectors = {
    'cloudtrail': cloudtrail,
    'okta': okta,
    'azure ad audit': azure_ad_audit,
    'azure ad signin': azure_ad_signin,
    'azure operation': azure_operation,
}

CONNECTION_OPTIONS = [
    {
        'connector': name,
        'options': getattr(connector, 'CONNECTION_OPTIONS', {}),
        'docstring': connector.__doc__,
        'finalize': callable(getattr(connector, 'finalize', None)),
    } for name, connector in connectors.items()
]
