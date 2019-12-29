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
    ('subscription_id', 'VARCHAR(50)'),
    ('id', 'VARCHAR(100)'),
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
    # https://docs.microsoft.com/en-us/rest/api/keyvault/getkeys/getkeys#keyitem
    'vaults_keys': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('kid', 'VARCHAR(500)'),
        ('error', 'VARIANT'),
        ('attributes', 'VARIANT'),
        ('kid', 'VARCHAR(1000)'),
        ('managed', 'BOOLEAN'),
        ('tags', 'VARIANT'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/keyvault/getsecrets/getsecrets#secretitem
    'vaults_secrets': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('id', 'VARCHAR(500)'),
        ('error', 'VARIANT'),
        ('attributes', 'VARIANT'),
        ('kid', 'VARCHAR(1000)'),
        ('managed', 'BOOLEAN'),
        ('tags', 'VARIANT'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/resources/resourcegroups/list#resourcegroup
    'resource_groups': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('location', 'VARCHAR(500)'),
        ('managed_by', 'VARCHAR(1000)'),
        ('name', 'VARCHAR(1000)'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/appservice/appserviceenvironments/list#appserviceenvironmentresource
    'hosting_environments': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('kind', 'VARCHAR(500)'),
        ('location', 'VARCHAR(500)'),
        ('name', 'VARCHAR(1000)'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/appservice/appserviceenvironments/list#appserviceenvironmentresource
    'webapps': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('kind', 'VARCHAR(500)'),
        ('location', 'VARCHAR(500)'),
        ('name', 'VARCHAR(1000)'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/storagerp/storageaccounts/list#storageaccount
    'storage_accounts': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('identity', 'VARIANT'),
        ('kind', 'VARCHAR(50)'),
        ('location', 'VARCHAR(500)'),
        ('name', 'VARCHAR(1000)'),
        ('properties', 'VARIANT'),
        ('sku', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/monitor/logprofiles/list#logprofileresource
    'log_profiles': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('identity', 'STRING'),
        ('kind', 'VARCHAR(50)'),
        ('location', 'STRING'),
        ('name', 'STRING'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'STRING'),
    ]
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
        'request': {'path': '/subscriptions', 'api-version': '2019-06-01'},
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
            'path': '/beta/reports/credentialUserRegistrationDetails',
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
            'path': '/subscriptions/{subscriptionId}/locations',
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
        },
    },
    'virtual_machines': {
        'request': {
            'path': '/subscriptions/{subscriptionId}/providers/Microsoft.Compute/virtualMachines',
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
            'path': '/subscriptions/{subscriptionId}/providers/Microsoft.ContainerService/managedClusters',
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
            'path': '/subscriptions/{subscriptionId}/resources',
            'params': {'$filter': 'resourceType eq \'Microsoft.KeyVault/vaults\''},
            'api-version': '2019-11-01',
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
        'children': [{'name': 'vaults_keys', 'args': {'vaultName': 'name'}}],
    },
    'vaults_keys': {
        'request': {
            'host': '{vaultName}.vault.azure.net',
            'path': '/keys',
            'params': {'maxresults': '25'},
            'api-version': '7.0',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'vaultName': 'vault_name',
            'error': 'error',
            'attributes': 'attributes',
            'kid': 'kid',
            'managed': 'managed',
            'tags': 'tags',
        },
    },
    'vaults_secrets': {
        'request': {
            'host': '{vaultName}.vault.azure.net',
            'path': '/secrets',
            'params': {'maxresults': '25'},
            'api-version': '7.0',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'vaultName': 'vault_name',
            'error': 'error',
            'attributes': 'attributes',
            'contentType': 'content_type',
            'id': 'id',
            'managed': 'managed',
            'tags': 'tags',
        },
    },
    'resource_groups': {
        'request': {
            'path': '/subscriptions/{subscriptionId}/resourcegroups',
            'api-version': '2019-08-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'vaultName': 'vault_name',
            'error': 'error',
            'id': 'id',
            'location': 'location',
            'managedBy': 'managed_by',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'hosting_environments': {
        'request': {
            'path': '/subscriptions/{subscriptionId}/providers/Microsoft.Web/hostingEnvironments',
            'api-version': '2019-08-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'vaultName': 'vault_name',
            'error': 'error',
            'id': 'id',
            'kind': 'kind',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'webapps': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/resourceGroups/{resourceGroupName}'
                '/providers/Microsoft.Web/hostingEnvironments/{name}'
                '/sites'
            ),
            'api-version': '2019-08-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'vaultName': 'vault_name',
            'error': 'error',
            'id': 'id',
            'kind': 'kind',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'storage_accounts': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/providers/Microsoft.Storage/storageAccounts'
            ),
            'api-version': '2019-06-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'identity': 'identity',
            'kind': 'kind',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'sku': 'sku',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'log_profiles': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/providers/microsoft.insights/logprofiles'
            ),
            'api-version': '2016-03-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'identity': 'identity',
            'kind': 'kind',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
}


def GET(kind, params):
    spec = API_SPECS[kind]
    request_spec = spec['request']
    path = request_spec['path'].format(**params)
    host = request_spec.get('host', 'management.azure.com').format(**params)
    api_version = request_spec.get('api-version')
    query_params = '?' + urlencode(
        updated({}, request_spec.get('params'), {'api-version': api_version})
    )
    aud = 'vault.azure.net' if host.endswith('vault.azure.net') else host
    bearer_token = CREDS[(CLIENT, TENANT, SECRET, aud)].token['access_token']
    url = f'https://{host}{path}{query_params}'
    log.debug(f'GET {url}')
    result = requests.get(
        url,
        headers={
            'Authorization': 'Bearer ' + bearer_token,
            'Content-Type': 'application/json',
        },
    )
    log.debug(f'<- {result.status_code}')
    response = result.json()
    # empty lists of values are recorded as empty rows
    # error values are recorded as rows with error and empty value cols
    # normal values are recorded with populated values and an empty error col
    values = [
        updated(
            {}, v, params, headerDate=parse_date(result.headers['Date']), tenantId=TENANT
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

        def load_table(kind, **params):
            values = GET(kind, params)
            kind = 'connection' if kind == 'subscriptions' else kind
            db.insert(f'{table_prefix}_{kind}', values)
            return values

        for s in load_table('subscriptions'):
            sid = s.get('subscription_id')
            if sid is None:
                log.debug('subscription without id', s)
                continue

            load_table('log_profiles', subscriptionId=sid)

            for henv in load_table('hosting_environments', subscriptionId=sid):
                if 'properties' in henv:
                    rg_name = henv['properties']['resourceGroup']
                    load_table(
                        'webapps',
                        subscriptionId=sid,
                        resourceGroupName=rg_name,
                        name=henv['name'],
                    )

            load_table('storage_accounts', subscriptionId=sid)

            for rg in load_table('resource_groups', subscriptionId=sid):
                if 'name' in rg:
                    pass

            load_table('subscriptions_locations', subscriptionId=sid)
            load_table('virtual_machines', subscriptionId=sid)
            load_table('managed_clusters', subscriptionId=sid)
            for v in load_table('vaults', subscriptionId=sid):
                if 'name' in v:
                    load_table('vaults_keys', subscriptionId=sid, vaultName=v['name'])
                    load_table(
                        'vaults_secrets', subscriptionId=sid, vaultName=v['name']
                    )

        load_table('reports_credential_user_registration_details')
