from . import aws_cloudtrail
from . import aws_config
from . import aws_inventory
from . import azure_log
from . import azure_subscription
from . import azure_vm
from . import gsuite_logs
from . import okta
from . import tenable_settings

__all__ = [
    'aws_inventory',
    'aws_cloudtrail',
    'aws_config',
    'azure_log',
    'azure_subscription',
    'azure_vm',
    'gsuite_logs',
    'okta',
    'tenable_settings',
]

connectors = {
    'aws_cloudtrail': aws_cloudtrail,
    'aws_config': aws_config,
    'aws_inventory': aws_inventory,
    'azure_log': azure_log,
    'azure_subscription': azure_subscription,
    'azure_vm': azure_vm,
    'gsuite_logs': gsuite_logs,
    'okta': okta,
    'tenable_settings': tenable_settings,
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
