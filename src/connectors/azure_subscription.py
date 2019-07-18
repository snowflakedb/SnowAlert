"""Azure Subscriptions List
Collect Azure Subscriptions List
"""

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE

from azure.mgmt.subscription.subscription_client import SubscriptionClient
from azure.common.client_factory import get_client_from_json_dict


CONNECTION_OPTIONS = [
    {
        'name': 'tenant_id',
        'title': "Tenant ID",
        'type': 'str',
        'prompt': "Identifies the client's tenancy",
        'placeholder': "48bbefee-7db1-4459-8f83-085fddf063b",
        'required': True
    },
    {
        'name': 'client_id',
        'type': 'str',
        'title': "Client ID",
        'prompt': "Usename identifying the client",
        'placeholder': "90d501a2-7c37-4036-af29-1e7e087437",
        'required': True
    },
    {
        'name': 'client_secret',
        'title': "Client Secret",
        'prompt': "Shared secret password authenticating the client",
        'type': 'str',
        'secret': 'true',
        'required': True
    },
]

LANDING_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('id', 'VARCHAR(50)'),
    ('subscription_id', 'VARCHAR(50)'),
    ('display_name', 'VARCHAR(500)'),
    ('state', 'VARCHAR(50)'),
    ('subscription_policies', 'VARIANT'),
    ('authorization_source', 'VARCHAR(50)'),
]


def connect(connection_name, options):
    base_name = f"azure_subscription_{connection_name}"
    tenant_id = options['tenant_id']
    client_id = options['client_id']
    client_secret = options['client_secret']

    comment = f'''
---
module: azure_subscription
client_id: {client_id}
tenant_id: {tenant_id}
client_secret: {client_secret}
'''

    db.create_table(
        name=f'data.{base_name}_connection',
        cols=LANDING_TABLE_COLUMNS,
        comment=comment
    )

    db.execute(f'GRANT INSERT, SELECT ON data.{base_name}_connection TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': 'Landing table created for collectors to populate.'
    }


def ingest(table_name, options):
    tenant_id = options['tenant_id']
    client_id = options['client_id']
    client_secret = options['client_secret']

    subscriptions_service = get_client_from_json_dict(SubscriptionClient, {
        "tenantId": tenant_id,
        "clientId": client_id,
        "clientSecret": client_secret,
        "activeDirectoryEndpointUrl": "https://login.microsoftonline.com",
        "resourceManagerEndpointUrl": "https://management.azure.com/",
        "managementEndpointUrl": "https://management.core.windows.net/",
    }).subscriptions

    subscriptions = [s.as_dict() for s in subscriptions_service.list()]

    db.insert(
        f'data.{table_name}',
        values=[(
            row,
            row['id'],
            row['subscription_id'],
            row['display_name'],
            row['state'],
            row['subscription_policies'],
            row['authorization_source'],
        ) for row in subscriptions],
        select=(
            'PARSE_JSON(column1), column2, column3,'
            'column4, column5, PARSE_JSON(column6), column7'
        )
    )

    yield len(subscriptions)
