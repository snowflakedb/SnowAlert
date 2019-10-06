"""Asset Panda
Collect asset information from Asset Panda using an API token
"""

from datetime import datetime
from functools import reduce
import re
import requests
from typing import Tuple
from urllib.error import HTTPError

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from .utils import yaml_dump


PAGE_SIZE = 1000

CONNECTION_OPTIONS = [
    {
        'name': 'asset_entity_id',
        'title': 'AssetPanda Entity ID',
        'prompt': 'Your AssetPanda Asset Entity ID',
        'type': 'int',
        'required': True,
        'secret': False,
    },
    {
        'name': 'token',
        'title': 'AssetPanda API Token',
        'prompt': 'Your AssetPanda API Token',
        'type': 'str',
        'secret': True,
    },
]

LANDING_TABLE_COLUMNS = [
    ('INSERT_ID', 'NUMBER IDENTITY START 1 INCREMENT 1'),
    ('RAW', 'VARIANT'),
    ('ID', 'VARCHAR(256)'),
    ('INSERT_AT', 'TIMESTAMP_LTZ(9)'),
]


###
# Helper Functions
###


def get_list_objects_and_total_from_get_object(result: dict) -> Tuple[list, int]:
    """Retrieve the values needed from the results objects"""
    try:
        list_object: list = result["objects"]
        total_object_count: int = result["totals"]["objects"]
    except BaseException:
        raise AttributeError("error format result")
    return (list_object, total_object_count)


def reduce_fields(accumulated_value: dict, field: dict) -> dict:
    """Because AssetPanda has custom fields that are named via free-text in the tool we need to perform cleanup
    on the user input data. We will reduce the fields down to just alpha numeric key strings so we can use
    them as the keys in our final JSON data."""
    cleaner_name = "_".join(re.findall(r"[a-zA-Z]+", field["name"]))
    field_key = field["key"]
    accumulated_value[field_key] = cleaner_name
    return accumulated_value


def replace_device_key(list_device: list, replace_key: dict):
    """This method will iterate through the data and replace the data that matches against keys in the replace_keys
    collection.

    It will convert data like:

        {
            "field_144": {
                "value": "00:0a:95:9d:68:16",
                "name": "MAC Address"
            }
        }

    to {"MAC_Address": "00:0a:95:9d:68:16"}
    """
    for key, value in replace_key.items():
        for device in list_device:
            if device.get(key, False):
                if device.get(value) is not None:
                    number = 2
                    while device.get(f"{value}_{number}") is not None:
                        number += 1
                    value = f"{value}_{number}"
                device[value] = device.pop(key)
    return list_device


###
# Main Functions
###


def get_data(token: str, url: str, params: dict = {}) -> dict:
    headers: dict = {"Authorization": f"Bearer {token}"}
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
    landing_table = f'data.assetpanda_{connection_name}_connection '

    comment = yaml_dump(module='assetpanda', **options)

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment=comment)

    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': "AssetPanda ingestion tables created!",
    }


def ingest(table_name, options):
    landing_table = f'data.{table_name}'

    token = options['token']
    asset_entity_id = options['asset_entity_id']

    general_url = (
        f"https://api.assetpanda.com:443//v2/entities/{asset_entity_id}/objects"
    )
    fields_url = f"https://api.assetpanda.com:443//v2/entities/{asset_entity_id}"

    params = {"offset": 0, "limit": PAGE_SIZE}

    total_object_count = 0

    insert_time = datetime.utcnow()

    while params['offset'] <= total_object_count:

        log.debug("total_object_count: ", total_object_count)

        assets = get_data(token=token, url=general_url, params=params)

        list_object, total_object_count = get_list_objects_and_total_from_get_object(
            assets
        )

        dict_fields = get_data(token, fields_url, params=params)
        list_field = dict_fields["fields"]

        # Stripping down the metadata to remove unnecessary fields. We only really care about the following:
        # {"field_140": "MAC_Address", "field_135" :"IP"}
        clear_fields: dict = reduce(reduce_fields, list_field, {})

        # replace every key "field_NO" by the value of the clear_field["field_NO"]
        list_object_without_field_id = replace_device_key(list_object, clear_fields)

        db.insert(
            landing_table,
            values=[
                (entry, entry.get('id', None), insert_time)
                for entry in list_object_without_field_id
            ],
            select=db.derive_insert_select(LANDING_TABLE_COLUMNS),
            columns=db.derive_insert_columns(LANDING_TABLE_COLUMNS),
        )

        log.info(
            f'Inserted {len(list_object_without_field_id)} rows ({landing_table}).'
        )
        yield len(list_object_without_field_id)

        # increment the offset to get new entries each iteration in the while loop
        params["offset"] += PAGE_SIZE
