"""Meraki Devices
Collect Meraki Device information using an API Token
"""

from runners.helpers import log
from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE

from datetime import datetime

import requests
from urllib.error import HTTPError
from .utils import yaml_dump


PAGE_SIZE = 5

CONNECTION_OPTIONS = [
    {
        'name': 'api_token',
        'title': "Meraki API Token",
        'prompt': "Your Meraki API Token",
        'type': 'str',
        'secret': True,
        'required': True,
    },
    {
        'name': 'network_id_whitelist',
        'title': "Meraki Network Ids Whitelist",
        'prompt': "Whitelist of Network Ids",
        'type': 'list',
        'secret': False,
        'required': True,
    },
]

LANDING_TABLE_COLUMNS_CLIENT = [
    ('insert_id', 'NUMBER IDENTITY START 1 INCREMENT 1'),
    ('snapshot_at', 'TIMESTAMP_LTZ(9)'),
    ('raw', 'VARIANT'),
    ('id', 'VARCHAR(256)'),
    ('mac', 'VARCHAR(256)'),
    ('description', 'VARCHAR(256)'),
    ('mdns_name', 'VARCHAR(256)'),
    ('dhcp_hostname', 'VARCHAR(256)'),
    ('ip', 'VARCHAR(256)'),
    ('switchport', 'VARCHAR(256)'),
    ('vlan', 'INT'),
    ('usage_sent', 'INT'),
    ('usage_recv', 'INT'),
    ('serial', 'VARCHAR(256)'),
]

LANDING_TABLE_COLUMNS_DEVICE = [
    ('insert_id', 'NUMBER IDENTITY START 1 INCREMENT 1'),
    ('snapshot_at', 'TIMESTAMP_LTZ(9)'),
    ('raw', 'VARIANT'),
    ('serial', 'VARCHAR(256)'),
    ('address', 'VARCHAR(256)'),
    ('name', 'VARCHAR(256)'),
    ('network_id', 'VARCHAR(256)'),
    ('model', 'VARCHAR(256)'),
    ('mac', 'VARCHAR(256)'),
    ('lan_ip', 'VARCHAR(256)'),
    ('wan_1_ip', 'VARCHAR(256)'),
    ('wan_2_ip', 'VARCHAR(256)'),
    ('tags', 'VARCHAR(256)'),
    ('lng', 'FLOAT'),
    ('lat', 'FLOAT'),
]


def get_data(url: str, token: str, params: dict = {}) -> dict:
    headers: dict = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Cisco-Meraki-API-Key": f"{token}",
    }
    try:
        log.debug(f"Preparing GET: url={url} with params={params}")
        req = requests.get(url, params=params, headers=headers)
        req.raise_for_status()
    except HTTPError as http_err:
        log.error(f"Error GET: url={url}")
        log.error(f"HTTP error occurred: {http_err}")
        raise
    log.debug(req.status_code)
    return req.json()


def connect(connection_name, options):
    landing_table_client = f'data.meraki_devices_{connection_name}_client_connection'
    landing_table_device = f'data.meraki_devices_{connection_name}_device_connection'

    comment = yaml_dump(module='meraki_devices', **options)

    db.create_table(
        name=landing_table_client, cols=LANDING_TABLE_COLUMNS_CLIENT, comment=comment
    )
    db.execute(f'GRANT INSERT, SELECT ON {landing_table_client} TO ROLE {SA_ROLE}')

    db.create_table(
        name=landing_table_device, cols=LANDING_TABLE_COLUMNS_DEVICE, comment=comment
    )
    db.execute(f'GRANT INSERT, SELECT ON {landing_table_device} TO ROLE {SA_ROLE}')
    return {'newStage': 'finalized', 'newMessage': "Meraki ingestion tables created!"}


def ingest(table_name, options):
    ingest_type = 'client' if table_name.endswith('_CLIENT_CONNECTION') else 'device'
    landing_table = f'data.{table_name}'

    timestamp = datetime.utcnow()
    api_token = options['api_token']
    whitelist = set(options['network_id_whitelist'])

    organizations = get_data(f"https://api.meraki.com/api/v0/organizations", api_token)

    for organization in organizations:
        organization_id = organization.get('id')
        log.debug(f'Processing Meraki organization id {organization_id}')
        if not organization_id:
            continue

        networks = get_data(
            f"https://api.meraki.com/api/v0/organizations/{organization_id}/networks",
            api_token,
        )
        network_ids = {network.get('id') for network in networks}

        if whitelist:
            network_ids = network_ids.intersection(whitelist)

        for network in network_ids:
            log.debug(f'Processing Meraki network {network}')
            try:
                devices = get_data(
                    f"https://api.meraki.com/api/v0/networks/{network}/devices",
                    api_token,
                )
            except requests.exceptions.HTTPError as e:
                log.error(f"{network} not accessible, ")
                log.error(e)
                continue

            if ingest_type == 'device':
                db.insert(
                    landing_table,
                    values=[
                        (
                            timestamp,
                            device,
                            device.get('serial'),
                            device.get('address'),
                            device.get('name'),
                            device.get('networkId'),
                            device.get('model'),
                            device.get('mac'),
                            device.get('lanIp'),
                            device.get('wan1Ip'),
                            device.get('wan2Ip'),
                            device.get('tags'),
                            device.get('lng'),
                            device.get('lat'),
                        )
                        for device in devices
                    ],
                    select=db.derive_insert_select(LANDING_TABLE_COLUMNS_DEVICE),
                    columns=db.derive_insert_columns(LANDING_TABLE_COLUMNS_DEVICE),
                )
                log.info(f'Inserted {len(devices)} rows ({landing_table}).')
                yield len(devices)

            else:
                for device in devices:
                    serial_number = device['serial']

                    try:
                        clients = get_data(
                            f"https://api.meraki.com/api/v0/devices/{serial_number}/clients",
                            api_token,
                        )
                    except requests.exceptions.HTTPError as e:
                        log.error(f"{network} not accessible, ")
                        log.error(e)
                        continue

                    db.insert(
                        landing_table,
                        values=[
                            (
                                timestamp,
                                client,
                                client.get('id'),
                                client.get('mac'),
                                client.get('description'),
                                client.get('mdnsName'),
                                client.get('dhcpHostname'),
                                client.get('ip'),
                                client.get('switchport'),
                                # vlan sometimes set to ''
                                client.get('vlan') or None,
                                client.get('usage', {}).get('sent') or None,
                                client.get('usage', {}).get('recv') or None,
                                serial_number,
                            )
                            for client in clients
                        ],
                        select=db.derive_insert_select(LANDING_TABLE_COLUMNS_CLIENT),
                        columns=db.derive_insert_columns(LANDING_TABLE_COLUMNS_CLIENT),
                    )
                    log.info(f'Inserted {len(clients)} rows ({landing_table}).')
                    yield len(clients)
