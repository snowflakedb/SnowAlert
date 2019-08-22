"""Cisco Umbrella
Collect Cisco Umbrella information using a Client ID and Secret
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from datetime import datetime

import requests
from .utils import yaml_dump

PAGE_SIZE = 500
UMBRELLA_URL_DEVICE_BY_PAGE = "https://management.api.umbrella.com/v1/organizations/ORGANIZATION_ID/roamingcomputers"

CONNECTION_OPTIONS = [
    {
        'name': 'api_key',
        'title': "Cisco Umbrella",
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
        'name': 'organizations_id',
        'title': "Cisco Umbrella Organizations Id",
        'prompt': "Your Cisco Umbrella Organizations Id",
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
    ('APPLIED_BUNDLE', 'NUMBER(38,0)'),
    ('HAS_IP_BLOCKING', 'NUMBER(38,0)'),
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
def get_token_basic(organizations_id: str, api_secret: str, api_key: str) -> str:
    headers: dict = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    try:
        log.debug(f"Preparing POST: url={UMBRELLA_URL_DEVICE_BY_PAGE}")
        req: dict = requests.post(
            UMBRELLA_URL_DEVICE_BY_PAGE,
            headers=headers,
            auth=requests.auth.HTTPBasicAuth(organizations_id, api_secret, api_key),
        )
        req.raise_for_status()
    except requests.HTTPError as http_err:
        log.error(f"Error GET: url={UMBRELLA_URL_DEVICE_BY_PAGE}")
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


# Helper function to format the /devices endpoint parameters
def create_url_params_get_devices(url: str, resources: list) -> str:
    params_get_devices: str = "?"
    for id in resources:
        params_get_devices += "&ids=" + id
    return url + params_get_devices


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
def get_data(url: str, key: str, secret: str, params: dict = {}) -> dict:
    """
        return the results format to json
        """
    headers: dict = {"Content-Type": "application/json", "Accept": "application/json"}
    try:
        req = requests.get(
            url, params=params, headers=headers, auth=HTTPBasicAuth(key, secret)
        )
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
    table_name = f'cisco_umbrella_{connection_name}_connection'
    landing_table = f'data.{table_name}'

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

    organizations_id = options['organizations_id']
    api_secret = options['api_secret']
    api_key = options['api_key']

    # Call the authorization endpoint so we can make subsequent calls to the API with an auth token
    token: str = get_token_basic(organizations_id, api_secret, api_key)

    offset = ""
    params_get_id_devices: dict = {
        "limit": PAGE_SIZE,
        "offset": offset,
    }

    while 1:
        dict_id_devices: dict = get_data(
            UMBRELLA_URL_DEVICE_BY_PAGE, api_key, api_secret, params_get_id_devices
        )
        resources: list = dict_id_devices["resources"]
        params_get_id_devices["offset"] = get_offset_from_devices_results(
            dict_id_devices)

        if len(resources) == 0:
            break

        device_details_url_and_params: str = create_url_params_get_devices(
            UMBRELLA_URL_DEVICE_BY_PAGE, resources
        )

        dict_devices: dict = get_data(UMBRELLA_URL_DEVICE_BY_PAGE, api_key, api_secret, device_details_url_and_params)
        devices = dict_devices["resources"]

        db.insert(
            landing_table,
            values=[(
                None,
                timestamp,
                device,
                device.get('DEVICE_ID'),
                device.get('OS_VERSION_NAME', None),
                device.get('LAST_SYNC_STATUS', None),
                device.get('TYPE', None),
                device.get('VERSION', None),
                device.get('LAST_SYNC', None),
                device.get('OS_VERSION', None),
                device.get('NAME', None),
                device.get('STATUS', None),
                device.get('ORIGIN_ID', None),
                device.get('APPLIED_BUNDLE', None),
                device.get('HAS_IP_BLOCKING', None),
            ) for device in devices],
            select=SELECT,
            columns=COLUMNS
        )
        log.info(f'Inserted {len(devices)} rows.')
        yield len(devices)
