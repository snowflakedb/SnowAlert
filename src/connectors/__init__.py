from . import aws_accounts
from . import aws_cloudtrail
from . import aws_config
from . import aws_flow_log
from . import aws_inventory
from . import azure_log
from . import azure_subscription
from . import azure_vm
from . import github_webhooks_s3
from . import gsuite_logs
from . import okta
from . import osquery_log
from . import tenable_settings
from . import crowdstrike_devices
from . import cisco_umbrella
from . import meraki_devices
from . import assetpanda
from . import nginx_log
from . import ldap_log

__all__ = [
    'aws_inventory',
    'aws_cloudtrail',
    'aws_config',
    'aws_accounts',
    'aws_flow_log',
    'azure_log',
    'azure_subscription',
    'azure_vm',
    'github_webhooks_s3',
    'gsuite_logs',
    'meraki_devices',
    'okta',
    'osquery_log',
    'tenable_settings',
    'crowdstrike_devices',
    'cisco_umbrella',
    'assetpanda',
    'ldap_log',
    'nginx_log',
]

connectors = {
    'aws_accounts': aws_accounts,
    'aws_cloudtrail': aws_cloudtrail,
    'aws_config': aws_config,
    'aws_flow_log': aws_flow_log,
    'aws_inventory': aws_inventory,
    'azure_log': azure_log,
    'azure_subscription': azure_subscription,
    'azure_vm': azure_vm,
    'github_webhooks_s3': github_webhooks_s3,
    'gsuite_logs': gsuite_logs,
    'meraki_devices': meraki_devices,
    'okta': okta,
    'osquery_log': osquery_log,
    'tenable_settings': tenable_settings,
    'crowdstrike_devices': crowdstrike_devices,
    'cisco_umbrella': cisco_umbrella,
    'assetpanda': assetpanda,
    'nginx_log': nginx_log,
    'ldap_log': ldap_log,
}

CONNECTION_OPTIONS = [
    {
        'connector': name,
        'options': getattr(connector, 'CONNECTION_OPTIONS'),
        'docstring': connector.__doc__,
        'finalize': callable(getattr(connector, 'finalize', None)),
    }
    for name, connector in connectors.items()
    if (
        getattr(connector, 'CONNECTION_OPTIONS', [{}])[0].get('name')
        and callable(getattr(connector, 'connect', None))
    )
]
