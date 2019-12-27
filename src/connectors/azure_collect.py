"""Azure Inventory and Configuration
Load Inventory and Configuration of accounts using Service Principals
"""

from collections import defaultdict
from dateutil.parser import parse as parse_date
import requests
from urllib.parse import urlencode

from azure.common.credentials import ServicePrincipalCredentials

from connectors.utils import updated
from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE
from .utils import yaml_dump


CLIENT = ''
TENANT = ''
SECRET = ''


class KeyedDefaultDict(defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
            return ret


CREDS = KeyedDefaultDict(  # type: ignore
    lambda xs: ServicePrincipalCredentials(  # type: ignore
        client_id=xs[0], tenant=xs[1], secret=xs[2], resource=f'https://{xs[3]}'
    )
)

sid = 'f4b00c5f-f6bf-41d6-806b-e1cac4f1f36f'


CONNECTION_OPTIONS = [
    {
        'name': 'credentials',
        'title': "Azure Auditor Service Principals",
        'prompt': "JSON list of {client, tenant, secret} objects",
        'type': 'json',
        'placeholder': """[{"client": "...", "tenant": "...", "secret": "..."}, ...]""",
        'required': True,
        'secret': True,
    }
]


# https://docs.microsoft.com/en-us/rest/api/resources/subscriptions/list#subscription
LANDING_TABLE_COLUMNS = [
    ('recorded_at', 'TIMESTAMP_LTZ'),
    ('tenant_id', 'VARCHAR(50)'),
    ('id', 'VARCHAR(100)'),
    ('subscription_id', 'VARCHAR(50)'),
    ('display_name', 'VARCHAR(500)'),
    ('state', 'VARCHAR(50)'),
    ('subscription_policies', 'VARIANT'),
    ('authorization_source', 'VARCHAR(50)'),
    ('managed_by_tenants', 'VARIANT'),
]


SUPPLEMENTARY_TABLES = {
    # https://docs.microsoft.com/en-us/graph/api/resources/credentialuserregistrationdetails#properties
    'reports_credential_user_registration_details': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('auth_methods', 'VARIANT'),
        ('id', 'STRING'),
        ('is_capable', 'BOOLEAN'),
        ('is_enabled', 'BOOLEAN'),
        ('is_mfa_registered', 'BOOLEAN'),
        ('is_registered', 'BOOLEAN'),
        ('user_display_name', 'STRING'),
        ('user_principal_name', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/resources/subscriptions/listlocations#location
    'subscriptions_locations': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('display_name', 'VARCHAR(500)'),
        ('id', 'VARCHAR(100)'),
        ('latitude', 'VARCHAR(100)'),
        ('longitude', 'VARCHAR(100)'),
        ('name', 'VARCHAR(1000)'),
        ('subscription_id', 'VARCHAR(50)'),
    ],
    # https://docs.virtual_machinesmicrosoft.com/en-us/rest/api/compute/virtualmachines/listall#virtualmachine
    'virtual_machines': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('identity', 'VARIANT'),
        ('location', 'VARCHAR(100)'),
        ('name', 'VARCHAR(100)'),
        ('plan', 'VARIANT'),
        ('properties', 'VARIANT'),
        ('resources', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
        ('zones', 'VARIANT'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/aks/managedclusters/list#response
    'managed_clusters': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('location', 'VARCHAR(500)'),
        ('name', 'VARCHAR(500)'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/keyvault/vaults/list
    'vaults': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('location', 'VARCHAR(500)'),
        ('name', 'VARCHAR(500)'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
}


def connect(connection_name, options):
    table_name_part = '' if connection_name == 'default' else f'_{connection_name}'
    table_prefix = f'azure_collect{table_name_part}'
    landing_table_name = f'data.{table_prefix}_connection'
    comment = yaml_dump(module='azure_collect', **options)

    db.create_table(
        name=landing_table_name,
        cols=LANDING_TABLE_COLUMNS,
        comment=comment,
        rw_role=SA_ROLE,
    )

    for table_postfix, cols in SUPPLEMENTARY_TABLES.items():
        supp_table = f'data.{table_prefix}_{table_postfix}'
        db.create_table(name=supp_table, cols=cols, rw_role=SA_ROLE)

    return {
        'newStage': 'finalized',
        'newMessage': 'Azure Collect landing tables created.',
    }


API_SPECS = {
    'subscriptions': {
        'request': {'url': 'subscriptions', 'api-version': '2019-06-01'},
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'error': 'error',
            'id': 'id',
            'subscriptionId': 'subscription_id',
            'displayName': 'display_name',
            'state': 'state',
            'subscriptionPolicies': 'subscription_policies',
            'authorizationSource': 'authorization_source',
            'managedByTenants': 'managed_by_tenants',
        },
        'children': [
            {
                'name': [
                    'subscriptions_locations',
                    'managed_clusters',
                    'virtual_machines',
                    'vaults',
                ],
                'args': {'subscriptionId': 'subscription_id'},
            }
        ],
    },
    'reports_credential_user_registration_details': {
        'request': {
            'url': '/beta/reports/credentialUserRegistrationDetails',
            'host': 'graph.microsoft.com',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'error': 'error',
            'id': 'id',
            'userPrincipalName': 'user_principal_name',
            'userDisplayName': 'user_display_name',
            'authMethods': 'auth_methods',
            'isRegistered': 'is_registered',
            'isEnabled': 'is_enabled',
            'isCapable': 'is_capable',
            'isMfaRegistered': 'is_mfa_registered',
        },
    },
    'subscriptions_locations': {
        'request': {
            'url': 'subscriptions/{subscription_id}/locations',
            'api-version': '2019-06-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'displayName': 'display_name',
            'id': 'id',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'name': 'name',
            'subscriptionId': 'subscription_id',
        },
    },
    'virtual_machines': {
        'request': {
            'url': 'subscriptions/{subscription_id}/providers/Microsoft.Compute/virtualMachines',
            'api-version': '2019-03-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'identity': 'identity',
            'location': 'location',
            'name': 'name',
            'plan': 'plan',
            'properties': 'properties',
            'resources': 'resources',
            'tags': 'tags',
            'type': 'type',
            'zones': 'zones',
        },
    },
    'managed_clusters': {
        'request': {
            'url': 'subscriptions/{subscription_id}/providers/Microsoft.ContainerService/managedClusters',
            'api-version': '2019-08-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'vaults': {
        'request': {
            'url': 'subscriptions/{subscription_id}/resources',
            'qparams': {'$filter': 'resourceType eq \'Microsoft.KeyVault/vaults\''},
            'api-version': '2018-02-14',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'location': 'location',
            'name': 'name',
            'tags': 'tags',
            'type': 'type',
        },
    },
}


def GET(kind, params, host=None, cred=''):
    sid = params.get('subscription_id')
    spec = API_SPECS[kind]
    host = spec.get('host', 'management.azure.com')
    request_spec = spec['request']
    path = request_spec['url'].format(**params)
    api_version = request_spec.get('api-version')
    qparams = '?' + urlencode(
        updated({'api-version': api_version}, request_spec.get('params'))
    )
    bearer_token = CREDS[(CLIENT, TENANT, SECRET, cred or host)].token['access_token']
    result = requests.get(
        f'https://{host}/{path}{qparams}',
        headers={
            'Authorization': 'Bearer ' + bearer_token,
            'Content-Type': 'application/json',
        },
    )
    response = result.json()
    # empty lists of values are recorded as empty rows
    # error values are recorded as rows with error but values empty
    # normal values are recorded with populated values and an empty error
    values = [
        updated(
            v,
            {'subscriptionId': sid} if sid else {},
            headerDate=parse_date(result.headers['Date']),
            tenantId=TENANT,
        )
        for v in response.get('value', [response]) or [{}]
    ]
    return [{spec['response'][k]: v for k, v in x.items()} for x in values]


def ingest(table_name, options):
    global CLIENT
    global TENANT
    global SECRET

    for cred in options['credentials']:
        CLIENT = cred['client']
        TENANT = cred['tenant']
        SECRET = cred['secret']

        connection_name = options['name']
        table_name_part = '' if connection_name == 'default' else f'_{connection_name}'
        table_prefix = f'data.azure_collect{table_name_part}'

        def load_table(kind, params={}):
            values = GET(kind, params)
            kind = 'connection' if kind == 'subscriptions' else kind
            db.insert(f'{table_prefix}_{kind}', values)
            return values

        subs = load_table('subscriptions')

        for s in subs:
            sid = s.get('subscription_id')
            if sid is None:
                log.debug('subscription without id', s)
                continue

            load_table('subscriptions_locations', {'subscription_id': sid})
            load_table('virtual_machines', {'subscription_id': sid})
            load_table('managed_clusters', {'subscription_id': sid})
            load_table('vaults', {'subscription_id': sid})

        load_table('reports_credential_user_registration_details')
