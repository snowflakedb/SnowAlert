"""Azure VM Inventory
Collect Azure VM inventory from subscriptions
"""

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient

from runners.helpers import db
from runners.utils import groups_of
from runners.helpers.dbconfig import ROLE as SA_ROLE
from .utils import create_metadata_table

from datetime import datetime
import itertools

METADATA_TABLE = 'data.azure_subscription_information'

CONNECTION_OPTIONS = [
    {
        'name': 'sp_username',
        'title': "Service Principal Username",
        'prompt': "Service Principal Username",
        'type': 'str',
        'required': True
    },
    {
        'name': 'sp_password',
        'title': "Service Principal Password",
        'prompt': "Service Principal Password",
        'type': 'str',
        'required': True,
        'secret': True
    },
    {
        'name': 'sp_tenant',
        'title': "Service Principal Tenant",
        'prompt': "The external id required to assume the destination role.",
        'type': 'str',
        'required': True
    },
    {
        'name': 'subscription_table',
        'title': 'Azure Subscription Table Designator',
        'prompt': 'The optional designator you provided when setting up the Azure Subscription Connector',
        'type': 'str',
        'required': True,
        'default': 'default'
    }

]

LANDING_TABLE_COLUMNS = [
    ('RAW', 'VARIANT'),
    ('EVENT_TIME', 'TIMESTAMP_LTZ'),
    ('HARDWARE_PROFILE', 'VARIANT'),
    ('ID', 'STRING'),
    ('LOCATION', 'STRING'),
    ('NAME', 'STRING'),
    ('NETWORK_PROFILE', 'VARIANT'),
    ('OS_PROFILE', 'VARIANT'),
    ('PROVISIONING_STATE', 'STRING'),
    ('STORAGE_PROFILE', 'VARIANT'),
    ('SUBSCRIPTION_ID', 'STRING'),
    ('TAGS', 'VARIANT'),
    ('TYPE', 'STRING'),
    ('VM_ID', 'STRING'),
]


def get_subscriptions(designator):
    table_name = f'DATA.AZURE_SUBSCRIPTION_{designator}_CONNECTION'
    sql = f"SELECT * FROM {table_name}"
    rows = db.fetch(sql)
    return rows


def get_vms(creds, sub):
    vms = [vm.as_dict() for vm in ComputeManagementClient(creds, sub).virtual_machines.list_all()]
    for vm in vms:
        vm['subscription_id'] = sub

    return vms


def get_nics(creds, sub):
    return [nic.as_dict() for nic in NetworkManagementClient(creds, sub).network_interfaces.list_all()]


def join(vms, nic):
    for vm in vms:
        # print(vm['id'])
        for vm_int in vm['network_profile']['network_interfaces']:
            if nic['id'] == vm_int['id']:
                vm_int['details'] = nic
                break
    return vms


def connect(connection_name, options):
    base_name = f'AZURE_VM_{connection_name}'
    sp_username = options['sp_username']
    sp_password = options['sp_password']
    sp_tenant = options['sp_tenant']
    subscription_table = options['subscription_table']
    comment = f'''
---
module: azure_vm
sp_username: {sp_username}
sp_password: {sp_password}
sp_tenant: {sp_tenant}
subscription_table: {subscription_table}
'''
    db.create_table(name=f'data.{base_name}_CONNECTION', cols=LANDING_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON data.{base_name}_CONNECTION TO ROLE {SA_ROLE}')

    cols = [
        ('SNAPSHOT_AT', 'TIMESTAMP_LTZ'),
        ('SUBSCRIPTION_ID', 'VARCHAR'),
        ('VM_INSTANCE_COUNT', 'NUMBER')
    ]
    create_metadata_table(METADATA_TABLE, cols, cols[2])

    return {
        'newStage': 'finalized',
        'newMessage': 'Landing table created for collectors to populate.'
    }


def ingest(table_name, options):
    table_name = f'data.{table_name}'
    timestamp = datetime.utcnow()
    username = options['sp_username']
    password = options['sp_password']
    tenant = options['sp_tenant']
    subscription_table = options['subscription_table']

    creds = ServicePrincipalCredentials(client_id=username, secret=password, tenant=tenant)

    subs = get_subscriptions(subscription_table)

    virtual_machines = []
    for sub in subs:
        vms = get_vms(creds, sub['SUBSCRIPTION_ID'])
        db.insert(table=METADATA_TABLE, values=[(timestamp, sub['SUBSCRIPTION_ID'], len(vms))], columns=['SNAPSHOT_AT', 'SUBSCRIPTION_ID', 'VM_INSTANCE_COUNT'])
        nics = get_nics(creds, sub['SUBSCRIPTION_ID'])
        for nic in nics:
            virtual_machines.append(
                join(vms, nic)
            )
    virtual_machines = list(itertools.chain(*virtual_machines))
    virtual_machines = [(elem,
                         timestamp,
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
                         elem.get('vm_id')
                         ) for elem in virtual_machines]

    data = groups_of(15000, virtual_machines)
    print('starting insert')
    for group in data:
        db.insert(table_name,
                  group,
                  select="""PARSE_JSON(column1),
                            column2,
                            PARSE_JSON(column3),
                            column4,
                            column5,
                            column6,
                            PARSE_JSON(column7),
                            PARSE_JSON(column8),
                            column9,
                            PARSE_JSON(column10),
                            column11,
                            PARSE_JSON(column12),
                            column13,
                            column14
                  """)
