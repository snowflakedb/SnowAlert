"""
This is the Azure AD Audit connector; it connects blobs holding AD Audit logs in an Azure Storage Account to Snowflake.
"""
from os import environ

from runners.helpers.auth import load_pkb_rsa
from runners.helpers.dbconfig import PRIVATE_KEY, PRIVATE_KEY_PASSWORD, DATABASE
from runners.helpers import db, log

from azure.storage.blob import BlockBlobService
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'account_name',
        'title': 'Storage Account',
        'prompt': 'The storage account holding your AD Audit log blobs',
    },
    {
        'type': 'str',
        'name': 'blob_name',
        'title': 'Blob Name',
        'prompt': 'Blob in the Storage Account containing the AD Audit logs',
        'default': 'insights-logs-auditlogs'
    },
    {
        'type': 'str',
        'name': 'sas_token',
        'title': 'SAS Token',
        'prompt': "A SAS Token which can list and read the files in the blob.",
        'secret': True
    },
    {
        'type': 'str',
        'name': 'suffix',
        'title': 'URL Suffix',
        'prompt': 'The Azure URL Suffix for the storage account',
        'default': 'core.windows.net'
    }
]

FILE_FORMAT = """
    TYPE = "JSON",
    COMPRESSION = "AUTO",
    ENABLE_OCTAL = FALSE,
    ALLOW_DUPLICATE = FALSE,
    STRIP_OUTER_ARRAY = FALSE,
    STRIP_NULL_VALUES = FALSE,
    IGNORE_UTF8_ERRORS = FALSE,
    SKIP_BYTE_ORDER_MARK = TRUE
"""

AZURE_AD_AUDIT_CONNECTION_TABLE_COLUMNS = [
    ('RAW', 'VARIANT'),
    ('HASH_RAW', 'NUMBER(38,0)'),
    ('CALLER_IP_ADDRESS', 'VARCHAR'),
    ('CATEGORY', 'VARCHAR'),
    ('CORRELATION_ID', 'VARCHAR'),
    ('DURATION_MS', 'NUMBER'),
    ('LEVEL', 'VARCHAR'),
    ('OPERATION_NAME', 'VARCHAR'),
    ('OPERATION_VERSION', 'VARCHAR'),
    ('PROPERTIES', 'VARIANT'),
    ('PROPERTIES_ACTIVITY_DATE_TIME', 'TIMESTAMP_LTZ(9)'),
    ('PROPERTIES_ACTIVITY_DISPLAY_NAME', 'VARCHAR'),
    ('PROPERTIES_ADDITIONAL_DETAILS', 'VARIANT'),
    ('PROPERTIES_CATEGORY', 'VARCHAR'),
    ('PROPERTIES_ID', 'VARCHAR'),
    ('PROPERTIES_INITIATED_BY', 'VARIANT'),
    ('PROPERTIES_LOGGED_BY_SERVICE', 'VARCHAR'),
    ('PROPERTIES_OPERATION_TYPE', 'VARCHAR'),
    ('PROPERTIES_RESULT', 'VARCHAR'),
    ('PROPERTIES_RESULT_REASON', 'VARCHAR'),
    ('PROPERTIES_TARGET_RESOURCES', 'VARIANT'),
    ('RESOURCE_ID', 'VARCHAR'),
    ('RESULT_SIGNATURE', 'VARCHAR'),
    ('TENANT_ID', 'VARCHAR'),
    ('EVENT_TIME', 'TIMESTAMP_LTZ(9)'),
    ('LOADED_ON', 'TIMESTAMP_LTZ(9)')
]


def connect(name, options):

    name = f"AZURE_AD_AUDIT_{name}"

    comment = f"""
---
module: azure_ad_audit
name: {name}
storage_account: {options['account_name']}
blob_name: {options['blob_name']}
sas_token: {options['sas_token']}
suffix: {options['suffix']}
"""

    db.create_stage(
        name=f'{name}_STAGE',
        url=f"azure://{options['account_name']}.blob.{options['suffix']}/{options['blob_name']}",
        cloud='azure',
        prefix='',
        credentials=options['sas_token'],
        file_format=FILE_FORMAT
    )

    db.create_table(
        name=f'{name}_CONNECTION',
        cols=AZURE_AD_AUDIT_CONNECTION_TABLE_COLUMNS,
        comment=comment
    )

    pipe_sql = f"""
COPY INTO data.{name}_CONNECTION (RAW, HASH_RAW, CALLER_IP_ADDRESS, CATEGORY, CORRELATION_ID,
                                         DURATION_MS, LEVEL, OPERATION_NAME, OPERATION_VERSION, PROPERTIES,
                                         PROPERTIES_ACTIVITY_DATE_TIME, PROPERTIES_ACTIVITY_DISPLAY_NAME,
                                         PROPERTIES_ADDITIONAL_DETAILS, PROPERTIES_CATEGORY, PROPERTIES_ID, PROPERTIES_INITIATED_BY,
                                         PROPERTIES_LOGGED_BY_SERVICE, PROPERTIES_OPERATION_TYPE, PROPERTIES_RESULT,
                                         PROPERTIES_RESULT_REASON, PROPERTIES_TARGET_RESOURCES,
                                         RESOURCE_ID, RESULT_SIGNATURE, TENANT_ID, EVENT_TIME, LOADED_ON)
FROM (
  SELECT $1, HASH($1), $1:callerIpAddress::STRING, $1:category::STRING, $1:correlationId::STRING, $1:durationMs::NUMBER,
  $1:level::STRING, $1:operationName::STRING, $1:operationVersion::STRING, $1:properties::VARIANT, $1:properties.activityDateTime::TIMESTAMP_LTZ,
  $1:properties.activityDisplayName::STRING, $1:properties.additionalDetails::VARIANT, $1:properties.category::STRING, $1:properties.id::STRING,
  $1:properties.initiatedBy::VARIANT, $1:properties.loggedByService::STRING, $1:properties.operationType::STRING, $1:properties.result::STRING,
  $1:resultReason::STRING, $1:properties.targetResources::VARIANT, $1:resourceId::STRING, $1:resultSignature::STRING, $1:tenantId::STRING,
  $1:time::TIMESTAMP_LTZ, CURRENT_TIMESTAMP()
  FROM @data.{name}_stage
)
"""
    db.create_pipe(
        name=f"{name}_PIPE",
        sql=pipe_sql,
        replace=True)


def test(name):
    yield db.fetch(f"ls @data.{name}_STAGE")
    yield db.fetch(f"SHOW TABLES LIKE '{name}_CONNECTION' IN DATA")


def ingest(name, options):
    name = f"AZURE_AD_AUDIT_{name}"
    block_blob_service = BlockBlobService(
        account_name=options['account_name'],
        sas_token=options['sas_token'],
        endpoint_suffix=options['suffix']
    )

    files = block_blob_service.list_blobs(options['blob_name'])

    timestamp_query = f"""
    SELECT loaded_on
    FROM data.{name}_CONNECTION
    ORDER BY loaded_on DESC
    LIMIT 1
    """
    ts = next(db.fetch(timestamp_query), None)
    last_loaded = ts['LOADED_ON'] if ts else None

    log.info(f"Last loaded time is {last_loaded}")

    new_files = []
    if last_loaded:
        for file in files:
            if file.properties.creation_time > last_loaded:
                new_files.append(StagedFile(file.name, None))
    else:
        for file in files:
            new_files.append(StagedFile(file.name, None))

    log.info(f"Found {len(new_files)} files to ingest")

    # Proxy object that abstracts the Snowpipe REST API
    ingest_manager = SimpleIngestManager(account=environ.get('SNOWFLAKE_ACCOUNT'),
                                         host=f'{environ.get("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com',
                                         user=environ.get('SA_USER'),
                                         pipe=f'{DATABASE}.DATA.{name}_PIPE',
                                         private_key=load_pkb_rsa(PRIVATE_KEY, PRIVATE_KEY_PASSWORD))

    if len(new_files) > 0:
        try:
            response = ingest_manager.ingest_files(new_files)
            log.info(response)
        except Exception as e:
            log.error(e)
            return
