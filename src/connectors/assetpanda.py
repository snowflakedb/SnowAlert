"""
Asset Panda 
Collect asset information from Asset Panda using an API token 
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from datetime import datetime

import snowflake
import requests
from urllib.error import HTTPError
from .utils import yaml_dump

import hashlib
import json
from functools import reduce
import re
import copy


PAGE_SIZE = 50

CONNECTION_OPTIONS = [
    {  
        'name': 'asset_entity_id',
        'title': 'AssetPanda Entity ID',
        'prompt': 'Your AssetPanda Asset Entity ID',
        'type': 'int',
        'required': True,
        'secret': False
    },
    {
        'name': 'token',
        'title': 'AssetPanda API Token',
        'prompt': 'Your AssetPanda API Token',
        'type': 'str',
        'secret': True
    }
]

LANDING_TABLE_COLUMNS = [
    ('INSERT_ID', 'NUMBER IDENTITY START 1 INCREMENT 1'),
    ('RAW', 'VARIANT'),
    ('ID', 'VARCHAR(256)'),
    ('IS_LOCKED','BOOLEAN'),
    ('DATE_ADDED', 'TIMESTAMP_TZ(9)'),
    ('STORAGE_CAPACITY', 'VARCHAR(256)'),
    ('ASSET_TAG_NUMBER', 'VARCHAR(256)'),
    ('IS_DELETABLE', 'BOOLEAN'),
    ('HAS_AUDIT_HISTORY', 'BOOLEAN'),
    ('PURCHASE_FROM', 'VARCHAR(256)'),
    ('DEPARTMENT', 'VARIANT'),
    ('DISPLAY_WITH_SECONDARY', 'VARCHAR(256)'),
    ('ASSET_PANDA_NUMBER', 'NUMBER(38,0)'),
    ('OBJECT_APPRECIATION', 'BOOLEAN'),
    ('STATUS', 'VARIANT'),
    ('PURCHASE_DATE', 'DATE'),
    ('YUBIKEY_IDENTIFIER', 'NUMBER(38,0)'),
    ('DISPLAY_NAME', 'VARCHAR(256)'),
    ('BRAND', 'VARCHAR(256)'),
    ('ASSIGNED_TO', 'VARIANT'),
    ('SHARE_URL', 'VARCHAR(256)'),
    ('OBJECT_VERSION_IDS', 'NUMBER(38,0)'),
    ('CREATION_DATE', 'DATE'),
    ('CREATED_BY', 'VARCHAR(256)'),
    ('PURCHASE_PRICE', 'FLOAT'),
    ('NEXT_SERVICE', 'DATE'),
    ('BUILDING', 'VARIANT'),
    ('CATEGORY', 'VARIANT'),
    ('DESCRIPTION', 'VARCHAR(256)'),
    ('CHANGED_BY', 'VARCHAR(256)'),
    ('WIRELESS_STATUS', 'VARCHAR(256)'),
    ('CREATED_AT', 'TIMESTAMP_TZ(9)'),
    ('GPS_COORDINATES', 'VARIANT'),
    ('UPDATED_AT', 'TIMESTAMP_TZ(9)'),
    ('LOANER_POOL', 'BOOLEAN'),
    ('DEFAULT_ATTACHMENT', 'VARIANT'),
    ('ROOM', 'VARIANT'),
    ('NOTES', 'VARCHAR(256)'),
    ('OBJECT_DEPRECIATION', 'BOOLEAN'),
    ('IS_EDITABLE', 'BOOLEAN'),
    ('WIFI_MAC_ADDRESS', 'VARCHAR(256)'),
    ('CHANGE_DATE', 'DATE'),
    ('DISPLAY_SIZE', 'VARCHAR(256)'),
    ('OPERATING_SYSTEM', 'VARCHAR(256)'),
    ('SERIAL', 'VARCHAR(256)'),
    ('END_OF_LIFE_DATE', 'DATE'),
    ('IMEI_MEID', 'VARCHAR(256)'),
    ('MODEL', 'VARCHAR(256)'),
    ('MAC_ADDRESS', 'VARCHAR(256)'),
    ('ENTITY', 'VARIANT'),
    ('PO', 'VARCHAR(256)'),
]

### Helper Functions ###

# Retrieve the values needed from the results objects
def get_list_objects_and_total_from_get_object(result: dict) -> (list, int):
    try:
        list_object: list = result["objects"]
        total_object_count: int = result["totals"]["objects"]
    except BaseException:
        raise AttributeError("error format result")
    return (list_object, total_object_count)


# Because AssetPanda has custom fields that are named via free-text in the tool we need to perform cleanup
# on the user input data. We will reduce the fields down to just alpha numeric key strings so we can use
# them as the keys in our final JSON data.
def reduce_fields(accumulators: dict, field: dict) -> str:
    # Take every word and join them by an underscore for create the new name
    cleaner_name = "_".join(re.findall(r"[a-zA-Z]+", field["name"]))
    field_key = field["key"]
    accumulators[field_key] = cleaner_name
    return accumulators


# This method will iterate through the data and replace the data that matches against keys in the replace_keys
# collection. It will convert data like the following example:
# {"field_144": {"value": "00:0a:95:9d:68:16", "name": "MAC Address"}} => {"MAC_Address": "00:0a:95:9d:68:16"}
def replace_device_key(list_device: list, replace_key: dict):
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


def parse_number(value):
    """
    If the value is '', None, or 0, return None
    Otherwise, return the original value
    """
    if value:
        return value
    return None


### Main Functions ###

def get_data(token: str, url: str, params: dict = {}) -> dict:
    headers: dict = {
        "Authorization": f"Bearer {token}"
    }
    try:
        log.debug(f"Preparing GET: url={url} with params={params}")
        req = requests.get(url, params=params, headers=headers)
        print("req: ", req)
        req.raise_for_status()
    except HTTPError as http_err:
        log.error(f"Error GET: url={url}")
        log.error(f"HTTP error occurred: {http_err}")
        raise http_err
    log.debug(req.status_code)

    return req.json()
    
    
def connect(connection_name, options):
    landing_table = f'data.assetpanda_{connection_name}_connection ' # creates table in snowalert
    
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

    general_url = f"https://api.assetpanda.com:443//v2/entities/{asset_entity_id}/objects"
    fields_url  = f"https://api.assetpanda.com:443//v2/entities/{asset_entity_id}"

    params = {
        "offset": 0,
        "limit": PAGE_SIZE,
    }

    total_object_count = 0 

    while params['offset'] <= total_object_count:

        log.debug("total_object_count: ", total_object_count)

        assets = get_data(token=token, url= general_url, params=params)
        
        list_object, total_object_count = get_list_objects_and_total_from_get_object(assets)

        dict_fields = get_data(token, fields_url, params=params)
        list_field = dict_fields["fields"]


        # Stripping down the metadata to remove unnecessary fields. We only really care about the following:
        # {"field_140": "MAC_Address", "field_135" :"IP"}
        clear_fields: list = reduce(reduce_fields, list_field, {})        

        # replace every key "field_NO" by the value of the clear_field["field_NO"]
        list_object_without_field_id = replace_device_key(list_object, clear_fields)

        db.insert(
            landing_table,
            values=[(
                entry,
                parse_number(  entry.get('id', None)  ),
                parse_number(  entry.get('is_locked', None)  ),
                parse_number(  entry.get('Date_Added', None)  ),
                parse_number(  entry.get('Storage Capacity', None)  ),
                parse_number(  entry.get('Asset_Tag_Number', None)  ),
                parse_number(  entry.get('is_deletable', None)  ),
                parse_number(  entry.get('has_audit_history', None)  ),
                parse_number(  entry.get('Purchase_From', None)  ),
                parse_number(  entry.get('Department', None)  ),
                parse_number(  entry.get('display_with_secondary', None)  ),
                parse_number(  entry.get('Asset_Panda_Number', None)  ), 
                parse_number(  entry.get('object_appreciation', None)  ),
                parse_number(  entry.get('Status', None)  ),
                parse_number(  entry.get('Purchase_date', None)  ),
                parse_number(  entry.get('Yubikey_Identifier', None)  ),
                parse_number(  entry.get('display_name', None)  ),
                parse_number(  entry.get('Brand', None)  ),
                parse_number(  entry.get('Assigned_To', None)  ),
                parse_number(  entry.get('share_url', None)  ),
                parse_number(  entry.get('object_version_ids', None)  ),
                parse_number(  entry.get('Creation_Date', None)  ),
                parse_number(  entry.get('Created_By', None)  ),
                parse_number(  entry.get('purchase_price', None)  ),
                parse_number(  entry.get('next_service', None)  ),
                parse_number(  entry.get('building', None)  ),
                parse_number(  entry.get('category', None)  ),
                parse_number(  entry.get('description', None)  ),
                parse_number(  entry.get('changed_by', None)  ),
                parse_number(  entry.get('wireless_status', None)  ),
                parse_number(  entry.get('created_at', None)  ),
                parse_number(  entry.get('gps_coordinates', None)  ),
                parse_number(  entry.get('updated_at', None)  ),
                parse_number(  entry.get('loaner_pool', None)  ),
                parse_number(  entry.get('default_attachment', None)  ),
                parse_number(  entry.get('room', None)  ),
                parse_number(  entry.get('notes', None)  ),
                parse_number(  entry.get('object_depreciation', None)  ),
                parse_number(  entry.get('is_editable', None)  ),
                parse_number(  entry.get('wifi_mac_address', None)  ),
                parse_number(  entry.get('change_date', None)  ),
                parse_number(  entry.get('display_size', None)  ),
                parse_number(  entry.get('operating_system', None)  ),
                parse_number(  entry.get('serial', None)  ),
                parse_number(  entry.get('end_of_life_date', None)  ),
                parse_number(  entry.get('imei_meid', None)  ),
                parse_number(  entry.get('model', None)  ),
                parse_number(  entry.get('mac_address', None)  ),
                parse_number(  entry.get('entity', None)  ),
                parse_number(  entry.get('PO', None)  )
            ) for entry in list_object_without_field_id],
            select=db.derive_insert_select(LANDING_TABLE_COLUMNS),
            columns=db.derive_insert_columns(LANDING_TABLE_COLUMNS)
        )
        
        log.info(f'Inserted {len(list_object_without_field_id)} rows ({landing_table}).')
        yield len(list_object_without_field_id)

        # increment the offset to get new entries each iteration in the while loop
        params["offset"] += PAGE_SIZE
