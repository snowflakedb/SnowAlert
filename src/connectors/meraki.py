"""Meraki
Collect Meraki Device information using an API Token
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from datetime import datetime

import snowflake
import requests
from urllib.error import HTTPError
# from .utils import yaml_dump

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
    ('SWITCHPORT', 'VARCHAR(256)'),
    ('VLAN', 'INT'), 
    ('USAGE_SENT', 'INT'),
    ('USAGE_RECV', 'INT'),
    ('SERIAL', 'VARCHAR(256)'),
    ('NETWORK_RAW', 'VARIANT')
]

# LANDING_TABLE_COLUMNS_DEVICE = [
#     ('INSERT_ID', 'NUMBER IDENTITY START 1 INCREMENT 1'),
#     ('SNAPSHOT_AT', 'TIMESTAMP_LTZ(9)'),
#     ('RAW', 'VARIANT'),
#     ('SERIAL', 'VARCHAR(256)'),
#     ('ADDRESS', 'VARCHAR(256)'),
#     ('NAME', 'VARCHAR(256)'),
#     ('NETWORK_ID', 'VARCHAR(256)'),
#     ('MODEL', 'VARCHAR(256)'),
#     ('MAC', 'VARCHAR(256)'),
#     ('LAN_IP', 'VARCHAR(256)'),
#     ('WAN_1_IP', 'VARCHAR(256)'),
#     ('WAN_2_IP', 'VARCHAR(256)'),
#     ('TAGS', 'VARCHAR(256)'),
#     ('LNG', 'FLOAT'),
#     ('LAT', 'FLOAT'),
# ]


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
    log.debug(req.status_code)
    return req.json()


def connect(connection_name, options):
    landing_table_client = f'data.meraki_{connection_name}_connection_client'
    # landing_table_device = f'data.meraki_{connection_name}_connection_device'
    options['network_id_whitelist'] = options.get('network_id_whitelist', '').split(',')

    # comment = yaml_dump(module='meraki', **options)

    db.create_table(name=landing_table_client,
                    cols=LANDING_TABLE_COLUMNS_CLIENT, comment="comment") # TODO: Change back to comment=comment
    db.execute(f'GRANT INSERT, SELECT ON {landing_table_client} TO ROLE {SA_ROLE}')
    db.create_table(name=landing_table_device,
                    cols=LANDING_TABLE_COLUMNS_DEVICE, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table_device} TO ROLE {SA_ROLE}')
    return {
        'newStage': 'finalized',
        'newMessage': "Meraki ingestion tables created!",
    }


def parse_number(value):
    if value:
        return value
    return None


def ingest(table_name_client, landing_table_device, options):
    landing_table_client = f'data.{table_name_client}'
    landing_table_device = f'data.{landing_table_device}'
    timestamp = datetime.utcnow()
    api_key = options['api_key']
    whitelist = options['network_id_whitelist']

    for network in whitelist:
        print("network: ", network)
        try:
            devices = get_data(f"https://api.meraki.com/api/v0/networks/{network}/devices", api_key)
        except requests.exceptions.HTTPError as e:
            log.error(f"{network} not accessible, ")
            log.error(e)
            continue
        
        if (len(devices)) == 0:
            print("network with 0 devices:", network)


        for device in devices:
            serial_number = device['serial']
            clients = get_data(f"https://api.meraki.com/api/v0/devices/{serial_number}/clients", api_key)
        
            db.insert(
                landing_table_client,
                values=[(
                    timestamp,
                    client,
                    client.get('id','None'),
                    client.get('mac','None'),
                    client.get('description','None'),
                    client.get('mdnsName','None'),
                    client.get('dhcpHostname','None'),
                    client.get('ip','None'),
                    parse_number(client.get('vlan','None')),
                    client.get('switchport','None'),
                    parse_number(client.get('usage','None').get('sent','None')),
                    parse_number(client.get('usage','None').get('recv','None')),
                    serial_number,
                    device
                ) for client in clients],
                select=db.derive_insert_select(LANDING_TABLE_COLUMNS_CLIENT),
                columns=db.derive_insert_columns(LANDING_TABLE_COLUMNS_CLIENT)
            )
            log.info(f'Inserted {len(clients)} rows (clients).')
            yield len(clients)

        # db.insert(
        #     landing_table_device,
        #     values=[(
        #         timestamp,
        #         device,
        #         device.get('serial','None'),
        #         device.get('address','None'),
        #         device.get('name','None'),
        #         device.get('networkId','None'),
        #         device.get('model','None'),
        #         device.get('mac','None'),
        #         device.get('lanIp','None'),
        #         device.get('wan1Ip','None'),
        #         device.get('wan2Ip','None'),
        #         device.get('tags','None'),
        #         device.get('lng','None'),
        #         device.get('lat','None'),
        #     ) for device in devices],
        #     select=db.derive_insert_select(LANDING_TABLE_COLUMNS_DEVICE),
        #     columns=db.derive_insert_columns(LANDING_TABLE_COLUMNS_DEVICE)
        # )
        # log.info(f'Inserted {len(devices)} rows (devices).')
        # yield len(devices)
