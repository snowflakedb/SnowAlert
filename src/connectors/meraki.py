"""Meraki Devices
Collect Meraki Device information using ___
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from datetime import datetime

import requests
from urllib.error import HTTPError
# from .utils import yaml_dump

PAGE_SIZE = 5

# COME BACK TO THIS!
CONNECTION_OPTIONS = [
    {
        'name': 'organization_id',
        'title': "Meraki Organization ID",
        'prompt': "Your Meraki Organization ID",
        'type': 'str',
        'required': True,
    },
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
        'type': 'str',
        'secret': True,
        'required': True,
    },
]

LANDING_TABLE_COLUMNS_CLIENT = [
    ('INSERT_ID', 'NUMBER IDENTITY START 1 INCREMENT 1'),
    ('SNAPSHOT_AT', 'TIMESTAMP_LTZ(9)'),
    ('RAW', 'VARIANT'),
    ('ID', 'VARCHAR(256)'),
    ('MAC', 'VARCHAR(256)'),
    ('DESCRIPTION', 'VARCHAR(256)'),
    ('MDNS_NAME', 'VARCHAR(256)'),
    ('DHCP_HOSTNAME', 'VARCHAR(256)'),
    ('IP', 'VARCHAR(256)'),
    ('VLAN', 'VARCHAR(256)'), #ORIGINALLY INT
    ('SWITCHPORT', 'VARCHAR(256)'),
    ('USAGE_SENT', 'VARCHAR(256)'), #ORIGINALLY INT
    ('USAGE_RECV', 'VARCHAR(256)'), #ORIGINALLY INT
    ('SERIAL', 'VARCHAR(256)'),
]

LANDING_TABLE_COLUMNS_DEVICE = [
    ('INSERT_ID', 'NUMBER IDENTITY START 1 INCREMENT 1'),
    ('SNAPSHOT_AT', 'TIMESTAMP_LTZ(9)'),
    ('RAW', 'VARIANT'),
    ('SERIAL', 'VARCHAR(256)'),
    ('ADDRESS', 'VARCHAR(256)'),
    ('NAME', 'VARCHAR(256)'),
    ('NETWORK_ID', 'VARCHAR(256)'),
    ('MODEL', 'VARCHAR(256)'),
    ('MAC', 'VARCHAR(256)'),
    ('LAN_IP', 'VARCHAR(256)'),
    ('WAN_1_IP', 'VARCHAR(256)'),
    ('WAN_2_IP', 'VARCHAR(256)'),
    ('TAGS', 'VARCHAR(256)'),
    ('LNG', 'FLOAT'),
    ('LAT', 'FLOAT'),
]


def get_col_transform_client(idx: int) -> str:
    column = f'column{idx+1}'
    if LANDING_TABLE_COLUMNS_CLIENT[idx][1] == "VARIANT":
        return f'PARSE_JSON({column})'
    if LANDING_TABLE_COLUMNS_CLIENT[idx][1] == "TIMESTAMP_LTZ(9)":
        return f'TO_TIMESTAMP({column})'
    return column


def get_col_transform_device(idx: int) -> str:
    column = f'column{idx+1}'
    if LANDING_TABLE_COLUMNS_DEVICE[idx][1] == "VARIANT":
        return f'PARSE_JSON({column})'
    if LANDING_TABLE_COLUMNS_DEVICE[idx][1] == "TIMESTAMP_LTZ(9)":
        return f'TO_TIMESTAMP({column})'
    return column


SELECT_CLIENT = ",".join(
    map(get_col_transform_client, range(1, len(LANDING_TABLE_COLUMNS_CLIENT))))

COLUMNS_CLIENT = [col[0] for col in LANDING_TABLE_COLUMNS_CLIENT[1:]]

SELECT_DEVICE = ",".join(
    map(get_col_transform_device, range(1, len(LANDING_TABLE_COLUMNS_DEVICE))))

COLUMNS_DEVICE = [col[0] for col in LANDING_TABLE_COLUMNS_DEVICE[1:]]


# Perform a generic API call
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
        raise http_err
    try:
        log.debug(req.status_code)
        json = req.json()
    except Exception as json_error:
        log.debug(f"JSON error occurred: {json_error}")
        log.debug(f"requests response {req}")
        json = {}
    return json


def connect(connection_name, options):
    table_name_client = f'meraki_devices_{connection_name}_connection_client'
    landing_table_client = f'data.{table_name_client}'
    table_name_device = f'meraki_devices_{connection_name}_connection_device'
    landing_table_device = f'data.{table_name_device}'

    # comment = yaml_dump(
    #     module='meraki_devices', **options)

    db.create_table(name=landing_table_client,
                    cols=LANDING_TABLE_COLUMNS_CLIENT, comment="Meraki")
    db.execute(f'GRANT INSERT, SELECT ON {landing_table_client} TO ROLE {SA_ROLE}')
    db.create_table(name=landing_table_device,
                    cols=LANDING_TABLE_COLUMNS_DEVICE, comment="Meraki")
    db.execute(f'GRANT INSERT, SELECT ON {landing_table_device} TO ROLE {SA_ROLE}')
    return {
        'newStage': 'finalized',
        'newMessage': "Meraki ingestion tables created!",
    }


def ingest(table_name_client, landing_table_device, options):
    landing_table_client = f'data.{table_name_client}'
    landing_table_device = f'data.{landing_table_device}'
    timestamp = datetime.utcnow()
    api_key = options['api_key']
    whitelist = options['network_id_whitelist']
    
    for network in whitelist:
        devices = get_data(f"https://api.meraki.com/api/v0/networks/{network}/devices", api_key)
        
        for device in devices:
            serial_number = device["serial"]
            clients = get_data(f"https://api.meraki.com/api/v0/devices/{serial_number}/clients", api_key)
            for client in clients:
                client['serial'] = serial_number

            db.insert(
                landing_table_client,
                values=[(
                    None,
                    timestamp,
                    client,
                    client.get('id'),
                    client.get('mac'),
                    client.get('description'),
                    client.get('mdnsName'),
                    client.get('dhcpHostname'),
                    client.get('ip'),
                    client.get('vlan'),
                    client.get('switchport'),
                    client.get('usage').get('sent'),
                    client.get('usage').get('recv'),
                    client.get('serial'),
                ) for client in clients],
                select=SELECT_CLIENT,
                columns=COLUMNS_CLIENT
            )
            log.info(f'Inserted {len(clients)} rows (clients).')
            yield len(clients)

        db.insert(
            landing_table_device,
            values=[(
                None,
                timestamp,
                device,
                device.get('serial_number'),
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
            ) for device in devices],
            select=SELECT_DEVICE,
            columns=COLUMNS_DEVICE
        )
        log.info(f'Inserted {len(devices)} rows (devices).')
        yield len(devices)
