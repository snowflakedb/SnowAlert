"""Azure VM Inventory
Collect Azure VM Inventory using an SP and Subscription Inventory
"""

from azure.common.client_factory import get_client_from_json_dict
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient

from runners.config import RUN_ID
from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE
from runners.utils import groups_of
from .utils import create_metadata_table
from .azure_subscription import API_ENDPOINTS

from datetime import datetime
import itertools

AZURE_COLLECTION_METADATA = 'data.azure_collection_metadata'

CONNECTION_OPTIONS = [
    {
        'name': 'tenant_id',
        'title': "Tenant ID",
        'prompt': "Identifies the Client's Azure Tenant",
        'placeholder': "48bbefee-7db1-4459-8f83-085fddf063b",
        'type': 'str',
        'required': True,
    },
    {
        'name': 'client_id',
        'title': "Client ID",
        'prompt': "Usename identifying the Service Principal",
        'placeholder': "90d501a2-7c37-4036-af29-1e7e087437",
        'type': 'str',
        'required': True,
    },
    {
        'name': 'client_secret',
        'title': "Client Secret",
        'prompt': "Secret access key authenticating the client",
        'type': 'str',
        'required': True,
        'secret': True,
    },
    {
        'name': 'subscription_connection_name',
        'title': 'Azure Subscription Table Designator',
        'prompt': 'The custom name you provided when setting up the Azure Subscription Connector',
        'type': 'str',
        'required': True,
        'default': 'default',
    },
]

LANDING_TABLE_COLUMNS = [
    ('EVENT_TIME', 'TIMESTAMP_LTZ'),
    ('RAW', 'VARIANT'),
    ('HARDWARE_PROFILE', 'VARIANT'),
    ('ID', 'VARCHAR(500)'),
    ('LOCATION', 'VARCHAR(100)'),
    ('NAME', 'VARCHAR(200)'),
    ('NETWORK_PROFILE', 'VARIANT'),
    ('OS_PROFILE', 'VARIANT'),
    ('PROVISIONING_STATE', 'VARCHAR(100)'),
    ('STORAGE_PROFILE', 'VARIANT'),
    ('SUBSCRIPTION_ID', 'VARCHAR(500)'),
    ('TAGS', 'VARIANT'),
    ('TYPE', 'VARCHAR(100)'),
    ('VM_ID', 'VARCHAR(200)'),
]


GET_SUBSCRIPTION_IDS_SQL = """
SELECT DISTINCT subscription_id
FROM data.azure_subscription_{0}_connection
WHERE event_time
  BETWEEN DATEADD(minute, -15, CURRENT_TIMESTAMP)
      AND CURRENT_TIMESTAMP
;
"""


def get_vms(options):
    cli = get_client_from_json_dict(ComputeManagementClient, options)
    vms = [vm.as_dict() for vm in cli.virtual_machines.list_all()]
    for vm in vms:
        vm['subscription_id'] = options['subscriptionId']
    return vms


def get_nics(options):
    cli = get_client_from_json_dict(NetworkManagementClient, options)
    return [nic.as_dict() for nic in cli.network_interfaces.list_all()]


def enrich_vm_with_nics(vm, nics):
    for nic in nics:
        for vm_int in vm['network_profile']['network_interfaces']:
            if nic['id'] == vm_int['id']:
                vm_int['details'] = nic
                break


def connect(connection_name, options):
    base_name = f'AZURE_VM_{connection_name}'
    client_id = options['client_id']
    client_secret = options['client_secret']
    tenant_id = options['tenant_id']
    subscription_connection_name = options['subscription_connection_name']
    comment = (
        f'---',
        f'module: azure_vm',
        f'client_id: {client_id}',
        f'client_secret: {client_secret}',
        f'tenant_id: {tenant_id}',
        f'subscription_connection_name: {subscription_connection_name}',
    )
    db.create_table(
        name=f'data.{base_name}_CONNECTION', cols=LANDING_TABLE_COLUMNS, comment=comment
    )
    db.execute(f'GRANT INSERT, SELECT ON data.{base_name}_CONNECTION TO ROLE {SA_ROLE}')

    cols = [
        ('SNAPSHOT_AT', 'TIMESTAMP_LTZ'),
        ('RUN_ID', 'STRING(100)'),
        ('SUBSCRIPTION_ID', 'STRING(500)'),
        ('VM_INSTANCE_COUNT', 'NUMBER'),
    ]
    create_metadata_table(AZURE_COLLECTION_METADATA, cols, cols[3])

    return {
        'newStage': 'finalized',
        'newMessage': 'Landing and metadata tables created for collectors to populate.',
    }


def ingest(table_name, options):
    table_name = f'data.{table_name}'
    now = datetime.utcnow()
    subscription_connection_name = options['subscription_connection_name']
    cloud_type = options.get('cloud_type', 'reg')
    creds = {
        'clientId': options['client_id'],
        'clientSecret': options['client_secret'],
        'tenantId': options['tenant_id'],
    }

    virtual_machines = []
    for sub in db.fetch(GET_SUBSCRIPTION_IDS_SQL.format(subscription_connection_name)):
        options = creds.copy()
        options.update(API_ENDPOINTS[cloud_type])
        options['subscriptionId'] = sub['SUBSCRIPTION_ID']
        vms = get_vms(options)
        db.insert(
            table=AZURE_COLLECTION_METADATA,
            values=[(now, RUN_ID, options['subscriptionId'], len(vms))],
            columns=['SNAPSHOT_AT', 'RUN_ID', 'SUBSCRIPTION_ID', 'VM_INSTANCE_COUNT'],
        )
        nics = get_nics(options)
        for vm in vms:
            enrich_vm_with_nics(vm, nics)
        virtual_machines.append(vms)

    virtual_machines = [
        (
            now,
            elem,
            elem.get('hardware_profile'),
            elem.get('id'),
            elem.get('location'),
            elem.get('name'),
            elem.get('network_profile'),
            elem.get('os_profile'),
            elem.get('provisioning_state'),
            elem.get('storage_profile'),
            elem.get('subscription_id'),
            elem.get('tags'),
            elem.get('type'),
            elem.get('vm_id'),
        )
        for elem in itertools.chain(*virtual_machines)
    ]

    for group in groups_of(15000, virtual_machines):
        db.insert(
            table_name,
            group,
            select=(
                'column1',
                'PARSE_JSON(column2)',
                'PARSE_JSON(column3)',
                'column4',
                'column5',
                'column6',
                'PARSE_JSON(column7)',
                'PARSE_JSON(column8)',
                'column9',
                'PARSE_JSON(column10)',
                'column11',
                'PARSE_JSON(column12)',
                'column13',
                'column14',
            ),
        )

    yield len(virtual_machines)
