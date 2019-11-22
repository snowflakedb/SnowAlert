"""Airwatch
Collect Airwatch Device information using a API Key, Host, and CMSURL Authentication
"""

from runners.helpers import log
from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE

from datetime import datetime

import requests
from urllib.error import HTTPError
from .utils import yaml_dump

PAGE_SIZE = 500

CONNECTION_OPTIONS = [
    {
        "name": "api_key",
        "title": "Airwatch API Key",
        "prompt": "Your Airwatch API Key",
        "type": "str",
        "secret": True,
        "required": True,
    },
    {
        "name": "host_airwatch",
        "title": "Airwatch Host",
        "prompt": "Your Airwatch Host",
        "type": "str",
        "secret": True,
        "required": True,
    },
    {
        "name": "device_auth",
        "title": "Airwatch CMS Auth for Device URL",
        "prompt": "Your Airwatch CMS Auth for Device URL",
        "type": "str",
        "secret": True,
        "required": True,
    },
    {
        "name": "custom_attributes_auth",
        "title": "Airwatch CMS Auth for Custom Attributes URL",
        "prompt": "Your Airwatch CMS Auth for Custom Attributes URL",
        "type": "str",
        "secret": True,
        "required": True,
    },
]

LANDING_TABLE_COLUMNS_DEVICE = [
    ("INSERT_ID", "NUMBER IDENTITY START 1 INCREMENT 1"),
    ("SNAPSHOT_AT", "TIMESTAMP_LTZ(9)"),
    ("RAW", "VARIANT"),
    ("EAS_IDS", "VARIANT"),
    ("UDID", "VARCHAR(256)"),
    ("SERIAL_NUMBER", "VARCHAR(256)"),
    ("MAC_ADDRESS", "VARCHAR(256)"),
    ("IMEI", "VARCHAR(256)"),
    ("EAS_ID", "VARCHAR(256)"),
    ("ASSET_NUMBER", "VARCHAR(256)"),
    ("DEVICE_FRIENDLY_NAME", "VARCHAR(256)"),
    ("LOCATION_GROUP_ID", "VARIANT"),
    ("LOCATION_GROUP_NAME", "VARCHAR(256)"),
    ("USER_ID", "VARIANT"),
    ("USER_NAME", "VARCHAR(256)"),
    ("DATA_PROTECTION_STATUS", "NUMBER(38,0)"),
    ("USER_EMAIL_ADDRESS", "VARCHAR(256)"),
    ("OWNERSHIP", "VARCHAR(256)"),
    ("PLATFORM_ID", "VARIANT"),
    ("PLATFORM", "VARCHAR(256)"),
    ("MODEL_ID", "VARIANT"),
    ("MODEL", "VARCHAR(256)"),
    ("OPERATING_SYSTEM", "VARCHAR(256)"),
    ("PHONE_NUMBER", "VARCHAR(256)"),
    ("LAST_SEEN", "TIMESTAMP_LTZ(9)"),
    ("ENROLLMENT_STATUS", "VARCHAR(256)"),
    ("COMPLIANCE_STATUS", "VARCHAR(256)"),
    ("COMPROMISED_STATUS", "BOOLEAN"),
    ("LAST_ENROLLED_ON", "TIMESTAMP_LTZ(9)"),
    ("LAST_COMPLIANCE_CHECK_ON", "TIMESTAMP_LTZ(9)"),
    ("LAST_COMPROMISED_CHECK_ON", "TIMESTAMP_LTZ(9)"),
    ("IS_SUPERVISED", "BOOLEAN"),
    ("VIRTUAL_MEMORY", "NUMBER(38,0)"),
    ("DEVICE_CAPACITY", "FLOAT"),
    ("AVAILABLE_DEVICE_CAPACITY", "FLOAT"),
    ("IS_DEVICE_DND_ENABLED", "BOOLEAN"),
    ("IS_DEVICE_LOCATOR_ENABLED", "BOOLEAN"),
    ("IS_CLOUD_BACKUP_ENABLED", "BOOLEAN"),
    ("IS_ACTIVATION_LOCK_ENABLED", "BOOLEAN"),
    ("IS_NETWORKTETHERED", "BOOLEAN"),
    ("BATTERY_LEVEL", "VARCHAR(256)"),
    ("IS_ROAMING", "BOOLEAN"),
    ("SYSTEM_INTEGRITY_PROTECTION_ENABLED", "BOOLEAN"),
    ("PROCESSOR_ARCHITECTURE", "NUMBER(38,0)"),
    ("TOTAL_PHYSICAL_MEMORY", "NUMBER(38,0)"),
    ("AVAILABLE_PHYSICAL_MEMORY", "NUMBER(38,0)"),
    ("DEVICE_CELLULAR_NETWORK_INFO", "VARIANT"),
    ("ENROLLMENT_USER_UUID", "VARCHAR(256)"),
    ("ID", "VARIANT"),
    ("UUID", "VARCHAR(256)"),
]

LANDING_TABLE_COLUMNS_CUSTOM_ATTRIBUTES = [
    ("INSERT_ID", "NUMBER IDENTITY START 1 INCREMENT 1"),
    ("SNAPSHOT_AT", "TIMESTAMP_LTZ(9)"),
    ("RAW", "VARIANT"),
    ("DEVICE_ID", "INT"),
    ("UDID", "VARCHAR(256)"),
    ("SERIAL_NUMBER", "VARCHAR(256)"),
    ("ENROLLMENT_USER_NAME", "VARCHAR(256)"),
    ("ASSET_NUMBER", "VARCHAR(256)"),
    ("CUSTOM_ATTRIBUTES", "VARIANT"),
]


def get_data(url: str, cms_auth: str, api_key: str, params: dict = {}) -> dict:
    headers: dict = {
        "Content-Type": "application/json",
        "aw-tenant-code": api_key,
        "Accept": "application/json",
        "Authorization": cms_auth,
    }
    try:
        log.debug(f"Preparing GET: url={url} with params={params}")
        req = requests.get(url, params=params, headers=headers)
        req.raise_for_status()
    except HTTPError as http_err:
        log.error(f"Error GET: url={url}")
        log.error(f"HTTP error occurred: {http_err}")
        raise
    return req.json()


def connect(connection_name, options):
    landing_table_device = f'data.airwatch_devices_{connection_name}_device_connection'
    landing_table_custom_attributes = (
        f'data.airwatch_devices_{connection_name}_custom_attributes_connection'
    )

    comment = yaml_dump(module='airwatch_devices', **options)

    db.create_table(
        name=landing_table_device, cols=LANDING_TABLE_COLUMNS_DEVICE, comment=comment
    )
    db.execute(f'GRANT INSERT, SELECT ON {landing_table_device} TO ROLE {SA_ROLE}')

    db.create_table(
        name=landing_table_custom_attributes,
        cols=LANDING_TABLE_COLUMNS_CUSTOM_ATTRIBUTES,
        comment=comment,
    )
    db.execute(
        f'GRANT INSERT, SELECT ON {landing_table_custom_attributes} TO ROLE {SA_ROLE}'
    )

    return {'newStage': 'finalized', 'newMessage': "Airwatch ingestion tables created!"}


def ingest(table_name, options):

    host_airwatch = options['host_airwatch']
    api_key = options['api_key']
    device_auth = options['device_auth']
    custom_attributes_auth = options['custom_attributes_auth']

    ingest_type = (
        'device' if table_name.endswith('_DEVICE_CONNECTION') else 'custom_attributes'
    )

    timestamp = datetime.utcnow()
    landing_table = f'data.{table_name}'

    if ingest_type == 'device':

        device_params: dict = {"PageSize": PAGE_SIZE, "Page": 0}
        url = f"https://{host_airwatch}/api/mdm/devices/search"

        while 1:
            result: dict = get_data(url, device_auth, api_key, device_params)

            devices = result["Devices"]

            db.insert(
                landing_table,
                values=[
                    (
                        timestamp,
                        device,
                        device.get('EasIds'),
                        device.get('Udid'),
                        device.get('SerialNumber'),
                        device.get('MacAddress'),
                        device.get('Imei'),
                        device.get('EasId'),
                        device.get('AssetNumber'),
                        device.get('DeviceFriendlyName'),
                        device.get('LocationGroupId'),
                        device.get('LocationGroupName'),
                        device.get('UserId'),
                        device.get('UserName'),
                        device.get('DataProtectionStatus'),
                        device.get('UserEmailAddress'),
                        device.get('Ownership'),
                        device.get('PlatformId'),
                        device.get('Platform'),
                        device.get('ModelId'),
                        device.get('Model'),
                        device.get('OperatingSystem'),
                        device.get('PhoneNumber'),
                        device.get('LastSeen'),
                        device.get('EnrollmentStatus'),
                        device.get('ComplianceStatus'),
                        device.get('CompromisedStatus'),
                        device.get('LastEnrolledOn'),
                        device.get('LastComplianceCheckOn'),
                        device.get('LastCompromisedCheckOn'),
                        device.get('IsSupervised'),
                        device.get('VirtualMemory'),
                        device.get('DeviceCapacity'),
                        device.get('AvailableDeviceCapacity'),
                        device.get('IsDeviceDNDEnabled'),
                        device.get('IsDeviceLocatorEnabled'),
                        device.get('IsCloudBackupEnabled'),
                        device.get('IsActivationLockEnabled'),
                        device.get('IsNetworkTethered'),
                        device.get('BatteryLevel'),
                        device.get('IsRoaming'),
                        device.get('SystemIntegrityProtectionEnabled'),
                        device.get('ProcessorArchitecture'),
                        device.get('TotalPhysicalMemory'),
                        device.get('AvailablePhysicalMemory'),
                        device.get('DeviceCellularNetworkInfo'),
                        device.get('EnrollmentUserUuid'),
                        device.get('Id'),
                        device.get('Uuid'),
                    )
                    for device in devices
                ],
                select=db.derive_insert_select(LANDING_TABLE_COLUMNS_DEVICE),
                columns=db.derive_insert_columns(LANDING_TABLE_COLUMNS_DEVICE),
            )

            log.info(f'Inserted {len(devices)} rows ({landing_table}).')

            yield len(devices)

            processed_total = (result["Page"] + 1) * result["PageSize"]
            if processed_total >= result["Total"]:
                break

            device_params["Page"] += 1

    else:
        custom_device_params: dict = {"PageSize": PAGE_SIZE, "Page": 0}
        url = f"https://{host_airwatch}/api/mdm/devices/customattribute/search"

        while 1:
            result: dict = get_data(
                url, custom_attributes_auth, api_key, custom_device_params
            )

            device_attributes = result["Devices"]

            db.insert(
                landing_table,
                values=[
                    (
                        timestamp,
                        device_attr,
                        device_attr.get('DeviceId'),
                        device_attr.get('Udid'),
                        device_attr.get('SerialNumber'),
                        device_attr.get('EnrollmentUserName'),
                        device_attr.get('AssetNumber'),
                        device_attr.get('CustomAttributes'),
                    )
                    for device_attr in device_attributes
                ],
                select=db.derive_insert_select(LANDING_TABLE_COLUMNS_CUSTOM_ATTRIBUTES),
                columns=db.derive_insert_columns(
                    LANDING_TABLE_COLUMNS_CUSTOM_ATTRIBUTES
                ),
            )

            log.info(f'Inserted {len(device_attributes)} rows ({landing_table}).')

            yield len(device_attributes)

            processed_total = (result["Page"] + 1) * result["PageSize"]
            if processed_total >= result["Total"]:
                break

            custom_device_params["Page"] += 1
