from . import aws_cloudtrail
from . import okta
from . import azure_log
from . import azure_subscription
from . import aws_config
from . import aws_asset_ingest

__all__ = [
    'aws_asset_ingest',
    'aws_cloudtrail',
    'aws_config',
    'azure_log',
    'azure_subscription',
    'okta',
]

connectors = {
    'aws_asset_ingest': aws_asset_ingest,
    'aws_cloudtrail': aws_cloudtrail,
    'aws_config': aws_config,
    'azure_log': azure_log,
    'azure_subscription': azure_subscription,
    'okta': okta,
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
