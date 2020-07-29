from . import aws_cloudtrail
from . import aws_collect
from . import aws_flow_log
from . import azure_collect
from . import azure_log
from . import duo_collect
from . import github_webhooks_s3
from . import gsuite_logs
from . import okta
from . import osquery_log
from . import tenable_io
from . import crowdstrike_devices
from . import cisco_umbrella
from . import meraki_devices
from . import assetpanda
from . import nginx_log
from . import ldap_log
from . import airwatch_devices
from . import salesforce_event_log

__all__ = [
    'aws_cloudtrail',
    'aws_collect',
    'aws_flow_log',
    'azure_collect',
    'azure_log',
    'duo_collect',
    'github_webhooks_s3',
    'gsuite_logs',
    'meraki_devices',
    'okta',
    'osquery_log',
    'tenable_io',
    'crowdstrike_devices',
    'cisco_umbrella',
    'assetpanda',
    'ldap_log',
    'nginx_log',
    'airwatch_devices',
    'salesforce_event_log',
]

connectors = {
    'aws_cloudtrail': aws_cloudtrail,
    'aws_collect': aws_collect,
    'aws_flow_log': aws_flow_log,
    'azure_collect': azure_collect,
    'azure_log': azure_log,
    'duo_collect': duo_collect,
    'github_webhooks_s3': github_webhooks_s3,
    'gsuite_logs': gsuite_logs,
    'meraki_devices': meraki_devices,
    'okta': okta,
    'osquery_log': osquery_log,
    'tenable_io': tenable_io,
    'crowdstrike_devices': crowdstrike_devices,
    'cisco_umbrella': cisco_umbrella,
    'assetpanda': assetpanda,
    'nginx_log': nginx_log,
    'ldap_log': ldap_log,
    'airwatch_devices': airwatch_devices,
    'salesforce_event_log': salesforce_event_log,
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
