"""Cisco Umbrella
Collect Cisco Umbrella information using a Client ID and Secret
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from datetime import datetime

import requests
from .utils import yaml_dump

PAGE_SIZE = 1000

CISCO_UMBRELLA_AUTH_TOKEN_URL = ''
CISCO_UMBRELLA_DEVICES_BdY_ID_URL = ''
CISCO_UMBRELLA_DEVICE_DETAILS_URL = ''

CONNECTION_OPTIONS = [
    {
        'name': 'client_id',
        'title': "Cisco Umbrella",
        'prompt': "Your Cisco Umbrella API Client ID",
        'type': 'str',
        'required': True,
        'secret': True,
    },
    {
        'name': 'client_secret',
        'title': "Cisco Umbrella API Client Secret",
        'prompt': "Your Cisco Umbrella API Client Secret",
        'type': 'str',
        'secret': True,
        'required': True,
    },
]

LANDING_TABLE_COLUMNS = [
    ('DEVICE_ID', 'VARCHAR(256)'),
    ('OS_VERSION_NAME', 'VARCHAR(256)'),
    ('LAST_SYNC_STATUS', 'VARCHAR(256)'),
    ('TYPE', 'VARCHAR(256)'),
    ('VERSION', 'VARCHAR(256)'),
    ('LAST_SYNC', 'VARCHAR(256)'),
    ('OS_VERSION', 'VARCHAR(256)'),
    ('NAME', 'VARCHAR(256)'),
    ('STATUS', 'VARCHAR(256)'),
    ('ORIGIN_ID', 'NUMBER(38,0)'),
    ('APPLIED_BUNDLE', 'NUMBER(38,0)')
    ('HAS_IP_BLOCKING', 'NUMBER(38,0)')
]


def get_col_transform(idx: int) -> str:
    column = f'column{idx + 1}'
    if LANDING_TABLE_COLUMNS[idx][1] == "VARIANT":
        return f'PARSE_JSON({column})'
    if LANDING_TABLE_COLUMNS[idx][1] == "TIMESTAMP_LTZ(9)":
        return f'TO_TIMESTAMP({column})'
    return column


SELECT = ",".join(
    map(get_col_transform, range(1, len(LANDING_TABLE_COLUMNS))))

COLUMNS = [col[0] for col in LANDING_TABLE_COLUMNS[1:]]


# Perform the authorization call to create access token for subsequent API calls
def get_token_basic(client_id: str, client_secret: str) -> str:
    headers: dict = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    try:
        log.debug(f"Preparing POST: url={CISCO_UMBRELLA_AUTH_TOKEN_URL}")
        req: dict = requests.post(
            CISCO_UMBRELLA_AUTH_TOKEN_URL,
            headers=headers,
            auth=requests.auth.HTTPBasicAuth(client_id, client_secret),
        )
        req.raise_for_status()
    except requests.HTTPError as http_err:
        log.error(f"Error GET: url={CISCO_UMBRELLA_AUTH_TOKEN_URL}")
        log.error(f"HTTP error occurred: {http_err}")
        raise http_err
    try:
        credential = req.json()
    except Exception as json_error:
        log.debug(f"JSON error occurred: {json_error}")
        log.debug(f"requests response {req}")
        raise (json_error)
    try:
        access_token = credential["access_token"]
    except BaseException:
        log.error("error auth request token")
        raise AttributeError("error auth request token")
    return access_token


# Parse out the offset value from the result.
def get_offset_from_devices_results(result: dict) -> str:
    if not isinstance(result, dict):
        log.error("the result is not a dict")
        raise TypeError("the result from Cisco Umbrella is not a dict")
    try:
        offset: str = result["meta"]["pagination"]["offset"]
    except BaseException:
        log.error("the offset is not present")
        log.error(result.get("meta", "No meta"))
        raise AttributeError("the offset is not present inside meta")
    return offset


# Perform a generic API call
def get_data(token: str, url: str, params: dict = {}) -> dict:
    headers: dict = {"Authorization": f"Bearer {token}"}
    try:
        log.debug(f"Preparing GET: url={url} with params={params}")
        req = requests.get(url, params=params, headers=headers)
        req.raise_for_status()
    except HTTPError as http_err:
        log.error(f"Error GET: url={url}")
        log.error(f"Error GET: url={url}")
        log.error(f"HTTP error occurred: {http_err}")
        raise http_err
    try:
        json = req.json()

    except Exception as json_error:
        log.debug(f"JSON error occurred: {json_error}")
        log.debug(f"requests response {req}")
        json = {}
    return json


# Helper function to format the /devices endpoint parameters
def create_url_params_get_devices(url: str, resources: list) -> str:
    params_get_devices: str = "?"
    for id in resources:
        params_get_devices += "&ids=" + id
    return url + params_get_devices


def connect(connection_name, options):
    table_name = f'cisco_umbrella_{connection_name}_connection'
    landing_table = f'data.{table_name}'

    client_id = options['client_id']
    client_secret = options['client_secret']

    comment = yaml_dump(
        module='cisco_umbrella',
        **options)

    db.create_table(name=landing_table,
                    cols=LANDING_TABLE_COLUMNS, comment=comment)

    db.execute(f'GRANT INSERT, SELECT ON data.{table_name} TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': "Cisco Umbrella ingestion table created!",
    }


def ingest(table_name, options):
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
            token, CISCO_UMBRELLA_DEVICES_BY_ID_URL, params_get_id_devices
        )
        resources: list = dict_id_devices["resources"]
        params_get_id_devices["offset"] = get_offset_from_devices_results(
            dict_id_devices)

        if len(resources) == 0:
            break

        device_details_url_and_params: str = create_url_params_get_devices(
            CISCO_UMBRELLA_DEVICE_DETAILS_URL, resources
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
