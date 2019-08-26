"""Meraki Devices
Collect Meraki Device information using ___
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from datetime import datetime

import requests
from .utils import yaml_dump

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

LANDING_TABLE_COLUMNS = [
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


def get_col_transform(idx: int) -> str:
    column = f'column{idx+1}'
    if LANDING_TABLE_COLUMNS[idx][1] == "VARIANT":
        return f'PARSE_JSON({column})'
    if LANDING_TABLE_COLUMNS[idx][1] == "TIMESTAMP_LTZ(9)":
        return f'TO_TIMESTAMP({column})'
    return column


SELECT = ",".join(
    map(get_col_transform, range(1, len(LANDING_TABLE_COLUMNS))))

COLUMNS = [col[0] for col in LANDING_TABLE_COLUMNS[1:]]


# Perform a generic API call
def get_data(organization_id: str, token: str, params: dict = {}) -> dict:
    url = f"https://api.meraki.com/api/v0/organizations/{organization_id}/networks"
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


def is_valid_white_list(organization_id: str, token: str, whitelist: list) -> bool:
    url_network = f"https://api.meraki.com/api/v0/organizations/{organization_id}/networks"
    networks_list: list = get_data(url_network, token)
    log.info(f"found {len(networks_list)} networks")
    references_id = [e["id"] for e in networks_list]
    for e in whitelist:
        if e not in references_id:
            log.error(f"{e}:from settings is not present into Meraki Network")
            raise ValueError(f"{e}:from settings is not present into Meraki Network")
    return True


def connect(connection_name, options):
    # CHANGE THIS
    table_name = f'meraki_devices_{connection_name}_connection'
    landing_table = f'data.{table_name}'

    comment = yaml_dump(
        module='meraki_devices', **options)

    db.create_table(name=landing_table,
                    cols=LANDING_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')
    return {
        'newStage': 'finalized',
        'newMessage': "Meraki ingestion table created!",
    }


def ingest(table_name, options):
    # CHANGE THIS
    landing_table = f'data.{table_name}'
    timestamp = datetime.utcnow()
    organization_id = options['organization_id']
    api_key = options['api_key']
    serial_device = options['serial_device']
    network_id = options['network_id']
    whitelist = options['whitelist']
    url_network = f"https://api.meraki.com/api/v0/organizations/{organization_id}/networks"
    url_client = f"https://api.meraki.com/api/v0/devices/{serial_device}/clients"
    url_device = f"https://api.meraki.com/api/v0/networks/{network_id}/devices"

    params: dict = {
        "limit": PAGE_SIZE,
        "validateWhiteList": False,
        "networksIdTodo": [],
        "page": 1,
    }

    params.setdefault(
        "validateWhiteList",
        is_valid_white_list(url_network, api_key, whitelist),
    )

    params.setdefault(
        "networksIdTodo",
        [{"id": e, "done": False} for e in whitelist],
    )

    while 1:
        devices: dict = get_data(
            organization_id, api_key, params
        )
        params["page"] += 1

        if len(devices) == 0:
            break

        db.insert(
            landing_table,
            values=[(
                None,
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
            ) for device in devices],
            select=SELECT,
            columns=COLUMNS
        )
        log.info(f'Inserted {len(devices)} rows.')
        yield len(devices)
