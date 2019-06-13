"""
This is the Azure Operation connector; it connects blobs holding Operation logs in an Azure Storage Account to Snowflake.
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
        'prompt': 'The storage account holding your Operation log blobs',
        'placeholder': 'azstorageaccount',
        'required': True
    },
    {
        'type': 'str',
        'name': 'blob_name',
        'title': 'Blob Name',
        'prompt': 'Blob in the Storage Account containing the Operation logs',
        'default': 'insights-operational-logs',
        'required': True
    },
    {
        'type': 'str',
        'name': 'sas_token',
        'title': 'SAS Token',
        'prompt': "A SAS Token which can list and read the files in the blob.",
        'secret': True,
        'placeholder': '?sv=2010-01-01&ss=abcd&srt=def&sp=gh&se=2011-01-01T00:12:34Z&st=2011-01-23T45:67:89Z&spr=https&sig=abcdefghijklmnopqrstuvwxyz%3D',
        'required': True
    },
    {
        'type': 'str',
        'name': 'suffix',
        'title': 'URL Suffix',
        'prompt': 'The Azure URL Suffix for the storage account',
        'default': 'core.windows.net',
        'required': True
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

AZURE_OPERATION_CONNECTION_TABLE_COLUMNS = [
    ('RAW', 'VARIANT'),
    ('HASH_RAW', 'NUMBER'),
    ('CALLER_IP_ADDRESS', 'VARCHAR'),
    ('CATEGORY', 'VARCHAR'),
    ('CORRELATION_ID', 'VARCHAR'),
    ('DURATION_MS', 'NUMBER'),
    ('IDENTITY', 'VARIANT'),
    ('IDENTITY_AUTHORIZATION', 'VARIANT'),
    ('IDENTITY_CLAIMS', 'VARIANT'),
    ('LEVEL', 'VARCHAR'),
    ('LOCATION', 'VARCHAR'),
    ('OPERATION_NAME', 'VARCHAR'),
    ('PROPERTIES', 'VARIANT'),
    ('PROPERTIES_ANCESTORS', 'VARCHAR'),
    ('PROPERTIES_IS_COMPLIANCE_CHECK', 'VARCHAR'),
    ('PROPERTIES_POLICIES', 'VARIANT'),
    ('PROPERTIES_RESOURCE_LOCAATION', 'VARCHAR'),
    ('RESOURCE_ID', 'VARCHAR'),
    ('RESULT_SIGNATURE', 'VARCHAR'),
    ('RESULT_TYPE', 'VARCHAR'),
    ('EVENT_TIME', 'TIMESTAMP_LTZ'),
    ('LOADED_AT', 'TIMESTAMP_LTZ')
]


def connect(name, options):
    name = f"AZURE_OPERATION_{name}"
    comment = f"""
---
module: azure_operation
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
        cols=AZURE_OPERATION_CONNECTION_TABLE_COLUMNS,
        comment=comment
    )

    pipe_sql = f"""
COPY INTO DATA.{name}_CONNECTION(RAW, HASH_RAW, CALLER_IP_ADDRESS, CATEGORY, CORRELATION_ID, DURATION_MS,
                                 IDENTITY, IDENTITY_AUTHORIZATION, IDENTITY_CLAIMS, LEVEL, LOCATION,
                                 OPERATION_NAME, PROPERTIES, PROPERTIES_ANCESTORS, PROPERTIES_IS_COMPLIANCE_CHECK,
                                 PROPERTIES_POLICIES, PROPERTIES_RESOURCE_LOCAATION, RESOURCE_ID, RESULT_SIGNATURE,
                                 RESULT_TYPE, EVENT_TIME, LOADED_AT)
FROM (
SELECT $1, HASH($1), $1:callerIpAddress::STRING, $1:category::STRING, $1:correlationId::STRING, $1:durationMs::NUMBER,
       $1:identity::VARIANT, $1:identity.authorization::VARIANT, $1:identity.claims::VARIANT, $1:level::STRING,
       $1:location::STRING, $1:operationName::STRING, $1:properties::VARIANT, $1:properties.ancestors::STRING,
       $1:properties.isComplianceCheck::STRING, PARSE_JSON($1:properties.policies), $1:properties.resourceLocation::STRING,
       $1:resourceId::STRING, $1:resultSignature::STRING, $1:resultType::STRING, $1:time::TIMESTAMP_LTZ, CURRENT_TIMESTAMP()
FROM @DATA.{name}_STAGE)
"""
    db.create_pipe(
        name=f'{name}_PIPE',
        sql=pipe_sql,
        replace=True)


def ingest(name, options):
    name = f"AZURE_OPERATION_{name}"
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


def test(name):
    yield db.fetch(f"ls @data.{name}_STAGE")
    yield db.fetch(f"SHOW TABLES LIKE '{name}_CONNECTION' IN DATA")
