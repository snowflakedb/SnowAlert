from . import cloudtrail
from . import okta
from . import azure
from . import aws_config
from . import aws_asset_ingest

__all__ = ['cloudtrail', 'okta', 'azure', 'aws_asset_ingest', 'aws_config']

connectors = {
    'cloudtrail': cloudtrail,
    'okta': okta,
    'azure': azure,
    'aws_config': aws_config,
    'aws_asset_ingest': aws_asset_ingest
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
