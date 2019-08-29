"""
Asset Panda 
Collect Asset Panda assets
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from datetime import datetime

import snowflake
import requests
from urllib.error import HTTPError
# from .utils import yaml_dump



PAGE_SIZE = 50

CONNECTION_OPTIONS = [
    {
        'name': 'username',
        'title': 'AssetPanda Username',
        'prompt': 'Your AssetPanda Username',
        'type': 'str',
        'required': True,
        'secret': False,
    },
    {
        'name': 'secret',
        'title': 'AssetPanda Secret',
        'prompt': 'Your AssetPanda Secret',
        'type': 'str',
        'required': True,
        'secret': True,
    },
    {
        'name': 'OAuth_client_id',
        'title': 'AssetPanda OAuth Client ID',
        'prompt': 'Your AssetPanda OAuth Client ID',
        'type': 'str',
        'required': True,
        'secret': False,
    },
    {
        'name': 'OAuth_client_secret',
        'title': 'AssetPanda OAuth Client Secret',
        'prompt': 'Your AssetPanda OAuth Client Secret',
        'type': 'str',
        'required': True,
        'secret': True,
    },
    {  
        'name': 'asset_entity_id',
        'title': 'AssetPanda Entity ID',
        'prompt': 'Your AssetPanda Asset Entity ID',
        'type': 'int',
        'required': True,
        'secret': False
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


def validate_key(item: dict, validate_dict: dict) -> None:
    pass

def validate_secret(secrets: dict) -> None:
    pass

def get_data(token: str, url: str, params: dict = {}) -> dict:
    """
    Make an API call
    """
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

    # decide whether to clean the data coming in

    return req.json()
    
    
def connect(connection_name, options):
    """
    Create the ingestion tables
    """
    print("connection_name: ", connection_name)
    landing_table = f'data.assetpanda_{connection_name}_connection' # creates table in snowalert
    
    # comment = yaml_dump(module='assetpanda', **options)

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment="AssetPanda Connector") # comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': "AssetPanda ingestion tables created!",
    }

def ingest(table_name, options):
    landing_table = f'data.{table_name}'
    timestamp = datetime.utcnow()
    
    ### Finish this later with Eduardo's help! ###
    # asset_entity_id = options['asset_entity_id']
    # username = options['username']
    # secret = options['secret']
    # OAuth_client_id = options['OAuth_client_id']
    # OAuth_client_secret = options['OAuth_client_secret']


    # general_url = f"https://api.assetpanda.com:443//v2/entities/{asset_entity_id}/objects"
    # fields_url  = f"https://api.assetpanda.com:443//v2/entities/{asset_entity_id}"

    # options['limit'] = PAGE_SIZE

    # assets = get_data(token=OAuth_client_secret, url= general_url, params=options)
    # fields = get_data(token=OAuth_client_secret, url=  fields_url, params=options)

    # delete once actual assets come in
    test_assets = [{
        'id':' ', 
        'is_locked': False, 
        'date_added': '2019-01-22 10:25:18.686 +0000',
        'storage_capacity': ' ',
        'asset_tag_number': ' ',
        'is_deletable': False,
        'has_audit_history': False,
        'purchase_from': ' ',
        'department': ' ',
        'display_with_secondary': ' ',
        'asset_panda_number': 1,
        'object_appreciation': False,
        'status': ' ',
        'purchase_date': '2019-01-22 10:25:18.686 +0000',
        'yubikey_identifier': 1,
        'display_name': ' ',
        'brand': ' ',
        'assigned_to': ' ',
        'share_url': ' ',
        'object_version_ids': 1,
        'creation_date': '2019-01-22 10:25:18.686 +0000',
        'created_by': ' ',
        'purchase_price': 0.01,
        'next_service': '2019-01-22 10:25:18.686 +0000',
        'building': ' ',
        'category': ' ',
        'description': ' ',
        'changed_by': ' ',
        'wireless_status': ' ',
        'created_at': '2019-01-22 10:25:18.686 +0000',
        'gps_coordinates': ' ',
        'updated_at': '2019-01-22 10:25:18.686 +0000',
        'loaner_pool': False,
        'default_attachment': ' ',
        'room': ' ',
        'notes': ' ',
        'object_depreciation': False,
        'is_editable': False,
        'wifi_mac_address': ' ',
        'change_date': '2019-01-22 10:25:18.686 +0000',
        'display_size': ' ',
        'operating_system': ' ',
        'serial': ' ',
        'end_of_life_date': '2019-01-22 10:25:18.686 +0000',
        'imei_meid': ' ',
        'model': ' ',
        'mac_address': ' ',
        'entity': ' ',
        'PO': ' '
    }]

    # tweak to match actual assets
    db.insert(
        landing_table,
        values=[(
            str(asset),
            asset.get('ID'),
            asset.get('is_locked'),
            asset.get('date_added'),
            asset.get('storage_capacity'),
            asset.get('asset_tag_number'),
            asset.get('is_deletable'),
            asset.get('has_audit_history'),
            asset.get('purchase_from'),
            asset.get('department'),
            asset.get('display_with_secondary'),
            asset.get('asset_panda_number'), 
            asset.get('object_appreciation'),
            asset.get('status'),
            asset.get('purchase_date'),
            asset.get('yubikey_identifier'),
            asset.get('display_name'),
            asset.get('brand'),
            asset.get('assigned_to'),
            asset.get('share_url'),
            asset.get('object_version_ids'),
            asset.get('creation_date'),
            asset.get('created_by'),
            asset.get('purchase_price'),
            asset.get('next_service'),
            asset.get('building'),
            asset.get('category'),
            asset.get('description'),
            asset.get('changed_by'),
            asset.get('wireless_status'),
            asset.get('created_at'),
            asset.get('gps_coordinates'),
            asset.get('updated_at'),
            asset.get('loaner_pool'),
            asset.get('default_attachment'),
            asset.get('room'),
            asset.get('notes'),
            asset.get('object_depreciation'),
            asset.get('is_editable'),
            asset.get('wifi_mac_address'),
            asset.get('change_date'),
            asset.get('display_size'),
            asset.get('operating_system'),
            asset.get('serial'),
            asset.get('end_of_life_date'),
            asset.get('imei_meid'),
            asset.get('model'),
            asset.get('mac_address'),
            asset.get('entity'),
            asset.get('PO')

        ) for asset in test_assets],
        select=db.derive_insert_select(LANDING_TABLE_COLUMNS),
        columns=db.derive_insert_columns(LANDING_TABLE_COLUMNS)
    )
    
    log.info(f'Inserted {len(test_assets)} rows ({test_assets}).')
    yield len(test_assets)

