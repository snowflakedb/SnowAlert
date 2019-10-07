"""Azure Subscription Inventory
Collect Azure Subscriptions using a Service Principal (SP)
"""

from dateutil.parser import parse

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE
from .utils import yaml_dump

from azure.mgmt.subscription import SubscriptionClient
from azure.common.client_factory import get_client_from_json_dict


CONNECTION_OPTIONS = [
    {
        'name': 'tenant_id',
        'title': "Tenant ID",
        'type': 'str',
        'prompt': "Identifies the Client's Azure Tenant",
        'placeholder': "48bbefee-7db1-4459-8f83-085fddf063b",
        'required': True,
    },
    {
        'name': 'client_id',
        'type': 'str',
        'title': "Client ID",
        'prompt': "Usename identifying the client",
        'placeholder': "90d501a2-7c37-4036-af29-1e7e087437",
        'required': True,
    },
    {
        'name': 'client_secret',
        'title': "Client Secret",
        'prompt': "Secret access key authenticating the client",
        'type': 'str',
        'secret': 'true',
        'required': True,
    },
    {
        'type': 'str',
        'name': 'cloud_type',
        'options': [
            {'value': 'reg', 'label': "Azure Cloud"},
            {'value': 'gov', 'label': "Azure Gov Cloud"},
        ],
        'title': "Cloud Type",
        'placeholder': "Choose Cloud Type",
        'prompt': "Azure provides two types of clouds: regular and government",
        'required': True,
    },
]

LANDING_TABLE_COLUMNS = [
    ('event_time', 'TIMESTAMP_LTZ'),
    ('tenant_id', 'VARCHAR(50)'),
    ('raw', 'VARIANT'),
    ('id', 'VARCHAR(100)'),
    ('subscription_id', 'VARCHAR(50)'),
    ('display_name', 'VARCHAR(500)'),
    ('state', 'VARCHAR(50)'),
    ('subscription_policies', 'VARIANT'),
    ('authorization_source', 'VARCHAR(50)'),
]


API_ENDPOINTS = {
    'reg': {
        'activeDirectoryEndpointUrl': 'https://login.microsoftonline.com/',
        'resourceManagerEndpointUrl': 'https://management.azure.com/',
        'managementEndpointUrl': 'https://management.core.windows.net/',
    },
    'gov': {
        'activeDirectoryEndpointUrl': 'https://login.microsoftonline.us',
        'resourceManagerEndpointUrl': 'https://management.usgovcloudapi.net',
        'managementEndpointUrl': 'https://management.core.usgovcloudapi.net',
    },
}


def get_subscription_service(options, cloud_type='reg'):
    options.update(API_ENDPOINTS[cloud_type])
    return get_client_from_json_dict(SubscriptionClient, options).subscriptions


def connect(connection_name, options):
    base_name = f"azure_subscription_{connection_name}"

    comment = yaml_dump(module='azure_subscription', **options)

    db.create_table(
        name=f'data.{base_name}_connection', cols=LANDING_TABLE_COLUMNS, comment=comment
    )

    db.execute(f'GRANT INSERT, SELECT ON data.{base_name}_connection TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': 'Landing table created for collectors to populate.',
    }


def ingest(table_name, options):
    tenant_id = options['tenant_id']
    client_id = options['client_id']
    client_secret = options['client_secret']
    cloud_type = options.get('cloud_type', 'reg')

    subscriptions_service = get_subscription_service(
        {"tenantId": tenant_id, "clientId": client_id, "clientSecret": client_secret},
        cloud_type,
    )

    subscription_list = subscriptions_service.list()
    subscriptions = [s.as_dict() for s in subscription_list]

    db.insert(
        f'data.{table_name}',
        values=[
            (
                parse(subscription_list.raw.response.headers['Date']).isoformat(),
                tenant_id,
                row,
                row['id'],
                row['subscription_id'],
                row['display_name'],
                row['state'],
                row['subscription_policies'],
                row['authorization_source'],
            )
            for row in subscriptions
        ],
        select=(
            'column1',
            'column2',
            'PARSE_JSON(column3)',
            'column4',
            'column5',
            'column6',
            'column7',
            'PARSE_JSON(column8)',
            'column9',
        ),
    )

    yield len(subscriptions)
