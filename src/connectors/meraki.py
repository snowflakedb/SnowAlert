"""Meraki Devices
Collect Meraki Device information using ___
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from datetime import datetime

import requests
from .utils import yaml_dump

PAGE_SIZE = 1000

meraki_AUTH_TOKEN_URL = 'https://api.meraki.com/oauth2/token'
meraki_DEVICES_BY_ID_URL = 'https://api.meraki.com/devices/queries/devices-scroll/v1'
meraki_DEVICE_DETAILS_URL = 'https://api.meraki.com/devices/entities/devices/v1'

# COME BACK TO THIS!
CONNECTION_OPTIONS = [
    {
        'name': 'client_id',
        'title': "Meraki API Client ID", 
        'prompt': "Your meraki Client ID",
        'type': 'str',
        'required': True,
        'secret': True,
    },
    {
        'name': 'client_secret',
        'title': "meraki API Client Secret",
        'prompt': "Your meraki API Client Secret",
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
    # CHANGE THIS
    table_name = f'meraki_devices_{connection_name}_connection'
    landing_table = f'data.{table_name}'

    client_id = options['client_id']
    client_secret = options['client_secret']

    comment = yaml_dump(
        module='meraki_devices', **options)

    db.create_table(name=landing_table,
                    cols=LANDING_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')
    return {
        'newStage': 'finalized',
        'newMessage': "meraki Devices ingestion table created!",
    }


def ingest(table_name, options):
    # CHANGE THIS
    landing_table = f'data.{table_name}'
    timestamp = datetime.utcnow()

    client_id = options['client_id']
    client_secret = options['client_secret']

    # Call the authorization endpoint so we can make subsequent calls to the API with an auth token
    token: str = get_token_basic(client_id, client_secret)

    offset = ""
    params_get_id_devices: dict = {
        "limit": PAGE_SIZE,
        "offset": offset,
    }

    while 1:
        dict_id_devices: dict = get_data(
            token, meraki_DEVICES_BY_ID_URL, params_get_id_devices
        )
        resources: list = dict_id_devices["resources"]
        params_get_id_devices["offset"] = get_offset_from_devices_results(
            dict_id_devices)

        if len(resources) == 0:
            break

        device_details_url_and_params: str = create_url_params_get_devices(
            meraki_DEVICE_DETAILS_URL, resources
        )

        dict_devices: dict = get_data(token, device_details_url_and_params)
        devices = dict_devices["resources"]

        db.insert(
            landing_table,
            values=[(
                None,
                timestamp,
                device,
                device.get('device_id'),
                device.get('first_seen', None),
                device.get('system_manufacturer', None),
                device.get('config_id_base', None),
                device.get('last_seen', None),
                device.get('policies', None),
                device.get('slow_changing_modified_timestamp', None),
                device.get('minor_version', None),
                device.get('system_product_name', None),
                device.get('hostname', None),
                device.get('mac_address', None),
                device.get('product_type_desc', None),
                device.get('platform_name', None),
                device.get('external_ip', None),
                device.get('agent_load_flags', None),
                device.get('group_hash', None),
                device.get('provision_status', None),
                device.get('os_version', None),
                device.get('groups', None),
                device.get('bios_version', None),
                device.get('modified_timestamp', None),
                device.get('local_ip', None),
                device.get('agent_version', None),
                device.get('major_version', None),
                device.get('meta', None),
                device.get('agent_local_time', None),
                device.get('bios_manufacturer', None),
                device.get('platform_id', None),
                device.get('device_policies', None),
                device.get('config_id_build', None),
                device.get('config_id_platform', None),
                device.get('cid', None),
                device.get('status', None),
                device.get('service_pack_minor', None),
                device.get('product_type', None),
                device.get('service_pack_major', None),
                device.get('build_number', None),
                device.get('pointer_size', None),
                device.get('site_name', None),
                device.get('machine_domain', None),
                device.get('ou', None),
            ) for device in devices],
            select=SELECT,
            columns=COLUMNS
        )
        log.info(f'Inserted {len(devices)} rows.')
        yield len(devices)