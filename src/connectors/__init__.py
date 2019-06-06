from . import cloudtrail
from . import okta

CONNECTION_OPTIONS = [
    {'connector': 'cloudtrail', 'options': cloudtrail.CONNECTION_OPTIONS, 'docstring': cloudtrail.__doc__},
    {'connector': 'okta', 'options': okta.CONNECTION_OPTIONS, 'docstring': okta.__doc__}
]
