"""Crowdstrike Devices
Collect Crowdstrike Device information using a Client ID and Secret
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from datetime import datetime

import requests
from .utils import yaml_dump

PAGE_SIZE = 1000

CROWDSTRIKE_AUTH_TOKEN_URL = 'https://api.crowdstrike.com/oauth2/token'
CROWDSTRIKE_DEVICES_BY_ID_URL = (
    'https://api.crowdstrike.com/devices/queries/devices-scroll/v1'
)
CROWDSTRIKE_DEVICE_DETAILS_URL = (
    'https://api.crowdstrike.com/devices/entities/devices/v1'
)

CONNECTION_OPTIONS = [
    {
        'name': 'client_id',
        'title': "Crowdstrike API Client ID",
        'prompt': "Your Crowdstrike Client ID",
        'type': 'str',
        'required': True,
        'secret': True,
    },
    {
        'name': 'client_secret',
        'title': "Crowdstrike API Client Secret",
        'prompt': "Your Crowdstrike API Client Secret",
        'type': 'str',
        'secret': True,
        'required': True,
    },
]

LANDING_TABLE_COLUMNS = [
    ('INSERT_ID', 'NUMBER IDENTITY START 1 INCREMENT 1'),
    ('SNAPSHOT_AT', 'TIMESTAMP_LTZ(9)'),
    ('RAW', 'VARIANT'),
    ('DEVICE_ID', 'STRING'),
    ('FIRST_SEEN', 'TIMESTAMP_LTZ(9)'),
    ('SYSTEM_MANUFACTURER', 'STRING'),
    ('CONFIG_ID_BASE', 'NUMBER'),
    ('LAST_SEEN', 'TIMESTAMP_LTZ(9)'),
    ('POLICIES', 'VARIANT'),
    ('SLOW_CHANGING_MODIFIED_TIMESTAMP', 'TIMESTAMP_LTZ(9)'),
    ('MINOR_VERSION', 'NUMBER'),
    ('SYSTEM_PRODUCT_NAME', 'STRING'),
    ('HOSTNAME', 'STRING'),
    ('MAC_ADDRESS', 'STRING'),
    ('PRODUCT_TYPE_DESC', 'STRING'),
    ('PLATFORM_NAME', 'STRING'),
    ('EXTERNAL_IP', 'STRING'),
    ('AGENT_LOAD_FLAGS', 'NUMBER'),
    ('GROUP_HASH', 'STRING'),
    ('PROVISION_STATUS', 'STRING'),
    ('OS_VERSION', 'STRING'),
    ('GROUPS', 'VARIANT'),
    ('BIOS_VERSION', 'STRING'),
    ('MODIFIED_TIMESTAMP', 'TIMESTAMP_LTZ(9)'),
    ('LOCAL_IP', 'STRING'),
    ('AGENT_VERSION', 'STRING'),
    ('MAJOR_VERSION', 'NUMBER'),
    ('META', 'VARIANT'),
    ('AGENT_LOCAL_TIME', 'TIMESTAMP_LTZ(9)'),
    ('BIOS_MANUFACTURER', 'STRING'),
    ('PLATFORM_ID', 'NUMBER'),
    ('DEVICE_POLICIES', 'VARIANT'),
    ('CONFIG_ID_BUILD', 'NUMBER'),
    ('CONFIG_ID_PLATFORM', 'NUMBER'),
    ('CID', 'STRING'),
    ('STATUS', 'STRING'),
    ('SERVICE_PACK_MINOR', 'NUMBER'),
    ('PRODUCT_TYPE', 'NUMBER'),
    ('SERVICE_PACK_MAJOR', 'NUMBER'),
    ('BUILD_NUMBER', 'NUMBER'),
    ('POINTER_SIZE', 'NUMBER'),
    ('SITE_NAME', 'STRING'),
    ('MACHINE_DOMAIN', 'STRING'),
    ('OU', 'VARIANT'),
]


# Perform the authorization call to create access token for subsequent API calls
def get_token_basic(client_id: str, client_secret: str) -> str:
    headers: dict = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    try:
        log.debug(f"Preparing POST: url={CROWDSTRIKE_AUTH_TOKEN_URL}")
        req = requests.post(
            CROWDSTRIKE_AUTH_TOKEN_URL,
            headers=headers,
            auth=requests.auth.HTTPBasicAuth(client_id, client_secret),
        )
        req.raise_for_status()
    except requests.HTTPError as http_err:
        log.error(f"Error GET: url={CROWDSTRIKE_AUTH_TOKEN_URL}")
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
        raise TypeError("the result from Crowdstrike is not a dict")
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
    except requests.HTTPError as http_err:
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
    table_name = f'crowdstrike_devices_{connection_name}_connection'
    landing_table = f'data.{table_name}'

    comment = yaml_dump(module='crowdstrike_devices', **options)

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')
    return {
        'newStage': 'finalized',
        'newMessage': "Crowdstrike Devices ingestion table created!",
    }


def ingest(table_name, options):
    landing_table = f'data.{table_name}'
    timestamp = datetime.utcnow()

    client_id = options['client_id']
    client_secret = options['client_secret']

    # Call the authorization endpoint so we can make subsequent calls to the API with an auth token
    token: str = get_token_basic(client_id, client_secret)

    offset = ""
    params_get_id_devices: dict = {"limit": PAGE_SIZE, "offset": offset}

    while 1:
        dict_id_devices: dict = get_data(
            token, CROWDSTRIKE_DEVICES_BY_ID_URL, params_get_id_devices
        )
        resources: list = dict_id_devices["resources"]
        params_get_id_devices["offset"] = get_offset_from_devices_results(
            dict_id_devices
        )

        if len(resources) == 0:
            break

        device_details_url_and_params: str = create_url_params_get_devices(
            CROWDSTRIKE_DEVICE_DETAILS_URL, resources
        )

        dict_devices: dict = get_data(token, device_details_url_and_params)
        devices = dict_devices["resources"]

        db.insert(
            landing_table,
            values=[
                (
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
                )
                for device in devices
            ],
            select=db.derive_insert_select(LANDING_TABLE_COLUMNS),
            columns=db.derive_insert_columns(LANDING_TABLE_COLUMNS),
        )
        log.info(f'Inserted {len(devices)} rows.')
        yield len(devices)
