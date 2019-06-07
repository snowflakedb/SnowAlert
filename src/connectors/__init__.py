from . import cloudtrail
from . import okta

__all__ = ['cloudtrail', 'okta']

CONNECTION_OPTIONS = [
    {
        'connector': 'okta',
        'options': okta.CONNECTION_OPTIONS,
        'docstring': okta.__doc__
    },
    {
        'connector': 'cloudtrail',
        'options': cloudtrail.CONNECTION_OPTIONS,
        'finalize': True,
        'docstring': cloudtrail.__doc__
    },
]
