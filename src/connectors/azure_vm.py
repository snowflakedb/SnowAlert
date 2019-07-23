"""Azure VM Inventory
Collect Azure VM inventory from Subscriptions
"""

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient

from runners.config import RUN_ID
from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE
from runners.utils import groups_of
from .utils import create_metadata_table

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
        'required': True
    },
    {
        'name': 'client_id',
        'title': "Client ID",
        'prompt': "Usename identifying the Service Principal",
        'placeholder': "90d501a2-7c37-4036-af29-1e7e087437",
        'type': 'str',
        'required': True
    },
    {
        'name': 'client_secret',
        'title': "Client Secret",
        'prompt': "Secret access key authenticating the client",
        'type': 'str',
        'required': True,
        'secret': True
    },
    {
        'name': 'subscription_connection_name',
        'title': 'Azure Subscription Table Designator',
        'prompt': 'The custom name you provided when setting up the Azure Subscription Connector',
        'type': 'str',
        'required': True,
        'default': 'default'
    }

]

LANDING_TABLE_COLUMNS = [
    ('EVENT_TIME', 'TIMESTAMP_LTZ'),
    ('RAW', 'VARIANT'),
    ('HARDWARE_PROFILE', 'VARIANT'),
    ('ID', 'VARCHAR(100)'),
    ('LOCATION', 'VARCHAR(100)'),
    ('NAME', 'VARCHAR(100)'),
    ('NETWORK_PROFILE', 'VARIANT'),
    ('OS_PROFILE', 'VARIANT'),
    ('PROVISIONING_STATE', 'VARCHAR(100)'),
    ('STORAGE_PROFILE', 'VARIANT'),
    ('SUBSCRIPTION_ID', 'VARCHAR(100)'),
    ('TAGS', 'VARIANT'),
    ('TYPE', 'VARCHAR(100)'),
    ('VM_ID', 'VARCHAR(100)'),
]


def get_vms(creds, sub):
    vms = [vm.as_dict() for vm in ComputeManagementClient(creds, sub).virtual_machines.list_all()]
    for vm in vms:
        vm['subscription_id'] = sub
    return vms


def get_nics(creds, sub):
    return [nic.as_dict() for nic in NetworkManagementClient(creds, sub).network_interfaces.list_all()]


def enrich_vms_with_nic(vms, nic):
    for vm in vms:
        for vm_int in vm['network_profile']['network_interfaces']:
            if nic['id'] == vm_int['id']:
                vm_int['details'] = nic
                break
    return vms


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
        name=f'data.{base_name}_CONNECTION',
        cols=LANDING_TABLE_COLUMNS,
        comment=comment,
    )
    db.execute(f'GRANT INSERT, SELECT ON data.{base_name}_CONNECTION TO ROLE {SA_ROLE}')

    cols = [
        ('SNAPSHOT_AT', 'TIMESTAMP_LTZ'),
        ('RUN_ID', 'STRING(100)'),
        ('SUBSCRIPTION_ID', 'STRING(500)'),
        ('VM_INSTANCE_COUNT', 'NUMBER')
    ]
    create_metadata_table(AZURE_COLLECTION_METADATA, cols, cols[2])

    return {
        'newStage': 'finalized',
        'newMessage': 'Landing and subscription data tables created for collectors to populate.'
    }


def ingest(table_name, options):
    table_name = f'data.{table_name}'
    now = datetime.utcnow()
    client_id = options['client_id']
    secret = options['client_secret']
    tenant = options['tenant_id']
    subscription_connection_name = options['subscription_connection_name']

    creds = ServicePrincipalCredentials(client_id=client_id, secret=secret, tenant=tenant)

    subscription_table = f'AZURE_SUBSCRIPTION_{subscription_connection_name}_CONNECTION'

    virtual_machines = []
    for sub in db.fetch(f"SELECT * FROM data.{subscription_table}"):
        sub_id = sub['SUBSCRIPTION_ID']
        vms = get_vms(creds, sub_id)
        db.insert(
            table=AZURE_COLLECTION_METADATA,
            values=[(now, RUN_ID, sub_id, len(vms))],
            columns=['SNAPSHOT_AT', 'RUN_ID', 'SUBSCRIPTION_ID', 'VM_INSTANCE_COUNT'],
        )
        for nic in get_nics(creds, sub_id):
            enrich_vms_with_nic(vms, nic)
        virtual_machines.append(vms)

    virtual_machines = [(
        elem,
        now,
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
    ) for elem in itertools.chain(*virtual_machines)]

    for group in groups_of(15000, virtual_machines):
        db.insert(
            table_name,
            group,
            select=(
                'PARSE_JSON(column1)',
                'column2',
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
            )
        )

    yield len(virtual_machines)
