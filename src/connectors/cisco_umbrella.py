"""Cisco Umbrella
Collect Cisco Umbrella information using a Client ID and Secret
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

import requests

from datetime import datetime
from .utils import yaml_dump

PAGE_SIZE = 500

CONNECTION_OPTIONS = [
    {
        'name': 'api_key',
        'title': "Cisco Umbrella API Key",
        'prompt': "Your Cisco Umbrella API Key",
        'type': 'str',
        'required': True,
        'secret': True,
    },
    {
        'name': 'api_secret',
        'title': "Cisco Umbrella API Secret",
        'prompt': "Your Cisco Umbrella API Secret",
        'type': 'str',
        'secret': True,
        'required': True,
    },
    {
        'name': 'organization_id',
        'title': "Cisco Umbrella Organization Id",
        'prompt': "Your Cisco Umbrella Organization Id",
        'type': 'int',
        'required': True,
    },
]

LANDING_TABLE_COLUMNS = [
    ('INSERT_ID', 'NUMBER IDENTITY START 1 INCREMENT 1'),
    ('SNAPSHOT_AT', 'TIMESTAMP_LTZ(9)'),
    ('RAW', 'VARIANT'),
    ('DEVICE_ID', 'VARCHAR(256)'),
    ('OS_VERSION_NAME', 'VARCHAR(256)'),
    ('LAST_SYNC_STATUS', 'VARCHAR(256)'),
    ('TYPE', 'VARCHAR(256)'),
    ('VERSION', 'VARCHAR(256)'),
    ('LAST_SYNC', 'TIMESTAMP_LTZ(9)'),
    ('OS_VERSION', 'VARCHAR(256)'),
    ('NAME', 'VARCHAR(256)'),
    ('STATUS', 'VARCHAR(256)'),
    ('ORIGIN_ID', 'NUMBER(38,0)'),
    ('APPLIED_BUNDLE', 'NUMBER(38,0)'),
    ('HAS_IP_BLOCKING', 'BOOLEAN'),
]


def get_data(organization_id: int, key: str, secret: str, params: dict = {}) -> dict:
    url = f"https://management.api.umbrella.com/v1/organizations/{organization_id}/roamingcomputers"
    headers: dict = {"Content-Type": "application/json", "Accept": "application/json"}
    try:
        req = requests.get(
            url,
            params=params,
            headers=headers,
            auth=requests.auth.HTTPBasicAuth(key, secret),
        )
        req.raise_for_status()

    except requests.HTTPError as http_err:
        log.error(f"Error GET: url={url}")
        log.error(f"HTTP error occurred: {http_err}")
        raise

    try:
        log.debug(req.status_code)
        json = req.json()

    except Exception as json_error:
        log.error(f"JSON error occurred: {json_error}")
        log.debug(f"requests response {req}")
        raise

    return json


def connect(connection_name, options):
    table_name = f'cisco_umbrella_devices_{connection_name}_connection'
    landing_table = f'data.{table_name}'
    comment = yaml_dump(module='cisco_umbrella', **options)

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment=comment)

    db.execute(f'GRANT INSERT, SELECT ON data.{table_name} TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': "Cisco Umbrella ingestion table created!",
    }


def ingest(table_name, options):
    landing_table = f'data.{table_name}'
    timestamp = datetime.utcnow()
    organization_id = options['organization_id']
    api_secret = options['api_secret']
    api_key = options['api_key']

    params: dict = {"limit": PAGE_SIZE, "page": 1}  # API starts at 1

    while 1:
        devices: dict = get_data(organization_id, api_key, api_secret, params)
        params["page"] += 1

        if len(devices) == 0:
            break

        db.insert(
            landing_table,
            values=[
                (
                    timestamp,
                    device,
                    device.get('deviceId'),
                    device.get('osVersionName', None),
                    device.get('lastSyncStatus', None),
                    device.get('type', None),
                    device.get('version', None),
                    device.get('lastSync', None),
                    device.get('osVersion', None),
                    device.get('name', None),
                    device.get('status', None),
                    device.get('originId', None),
                    device.get('appliedBundle', None),
                    device.get('hasIpBlocking', None),
                )
                for device in devices
            ],
            select=db.derive_insert_select(LANDING_TABLE_COLUMNS),
            columns=db.derive_insert_columns(LANDING_TABLE_COLUMNS),
        )
        log.info(f'Inserted {len(devices)} rows.')
        yield len(devices)
