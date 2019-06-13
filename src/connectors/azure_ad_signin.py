"""
This is the Azure AD Signin connector; it connects blobs holding AD Signin logs in an Azure Storage Account to Snowflake.
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
        'prompt': 'The storage account holding your AD Signin log blobs',
    },
    {
        'type': 'str',
        'name': 'blob_name',
        'title': 'Blob Name',
        'prompt': 'Blob in the Storage Account containing the AD Signin logs',
        'default': 'insights-logs-signinlogs'
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

AZURE_AD_SIGNIN_CONNECTION_TABLE_COLUMNS = [
    ('RAW', 'VARIANT'),
    ('HASH_RAW', 'NUMBER'),
    ('LEVEL', 'NUMBER'),
    ('CALLER_IP_ADDRESS', 'VARCHAR'),
    ('CATEGORY', 'VARCHAR'),
    ('CORRELATION_ID', 'VARCHAR'),
    ('DURATION_MS', 'NUMBER'),
    ('IDENTITY', 'VARCHAR'),
    ('LOCATION', 'VARCHAR'),
    ('OPERATION_NAME', 'VARCHAR'),
    ('OPERATION_VERSION', 'VARCHAR'),
    ('PROPERTIES', 'VARIANT'),
    ('PROPERTIES_APP_DISPLAY_NAME', 'VARCHAR'),
    ('PROPERTIES_APP_ID', 'VARCHAR'),
    ('PROPERTIES_APPLIED_CONDITIONAL_ACESS_POLICIES', 'VARIANT'),
    ('PROPERTIES_AUTHENTICATION_METHODS_USED', 'VARIANT'),
    ('PROPERTIES_AUTHENTICATION_PROCESSING_DETAILS', 'VARIANT'),
    ('PROPERTIES_CLIENT_APP_USED', 'VARCHAR'),
    ('PROPERTIES_CONDITIONAL_ACCESS_STATUS', 'VARCHAR'),
    ('PROPERTIES_CREATED_DATE_TIME', 'TIMESTAMP_LTZ'),
    ('PROPERTIES_DEVICE_DETAIL', 'VARIANT'),
    ('PROPERTIES_ID', 'VARCHAR'),
    ('PROPERTIES_IP_ADDRESS', 'VARCHAR'),
    ('PROPERTIES_IS_INTERACTIVE', 'BOOLEAN'),
    ('PROPERTIES_LOCATION', 'VARIANT'),
    ('PROPERTIES_MFA_DETAIL', 'VARIANT'),
    ('PROPERTIES_NETWORK_LOCATION', 'VARIANT'),
    ('PROPERTIES_PROCESSING_TIME_IN_MILLISECONDS', 'NUMBER'),
    ('PROPERTIES_RESOURCE_DISPLAY_NAME', 'VARCHAR'),
    ('PROPERTIES_RESOURCE_ID', 'VARCHAR'),
    ('PROPERTIES_RISK_DETAIL', 'VARCHAR'),
    ('PROPERTIES_RISK_EVENT_TYPES', 'VARIANT'),
    ('PROPERTIES_RISK_LEVEL_AGGREGATED', 'VARCHAR'),
    ('PROPERTIES_RISK_LEVEL_DURING_SIGNIN', 'VARCHAR'),
    ('PROPERTIES_RISK_STATE', 'VARCHAR'),
    ('PROPERTIES_STATUS', 'VARIANT'),
    ('PROPERTIES_TOKEN_ISSUER_TYPE', 'VARCHAR'),
    ('PROPERTIES_USER_DISPLAY_NAME', 'VARCHAR'),
    ('PROPERTIES_USER_ID', 'VARCHAR'),
    ('PROPERTIES_USER_PRINCIPAL_NAME', 'VARCHAR'),
    ('RESOURCE_ID', 'VARCHAR'),
    ('RESULT_DESCRIPTION', 'VARCHAR'),
    ('RESULT_SIGNATURE', 'VARCHAR'),
    ('RESULT_TYPE', 'VARCHAR'),
    ('TENANT_ID', 'VARCHAR'),
    ('EVENT_TIME', 'TIMESTAMP_LTZ'),
    ('LOADED_ON', 'TIMESTAMP_LTZ')
]


def connect(name, options):
    name = f"AZURE_AD_SIGNIN_{name}"
    comment = f"""
---
module: azure_ad_signin
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
        cols=AZURE_AD_SIGNIN_CONNECTION_TABLE_COLUMNS,
        comment=comment
    )

    pipe_sql = f"""
COPY INTO DATA.{name}_CONNECTION (RAW, HASH_RAW, LEVEL, CALLER_IP_ADDRESS, CATEGORY, CORRELATION_ID, DURATION_MS, IDENTITY, LOCATION,
                                  OPERATION_NAME, OPERATION_VERSION, PROPERTIES, PROPERTIES_APP_DISPLAY_NAME, PROPERTIES_APP_ID,
                                  PROPERTIES_APPLIED_CONDITIONAL_ACESS_POLICIES, PROPERTIES_AUTHENTICATION_METHODS_USED,
                                  PROPERTIES_AUTHENTICATION_PROCESSING_DETAILS, PROPERTIES_CLIENT_APP_USED,
                                  PROPERTIES_CONDITIONAL_ACCESS_STATUS, PROPERTIES_CREATED_DATE_TIME, PROPERTIES_DEVICE_DETAIL,
                                  PROPERTIES_ID, PROPERTIES_IP_ADDRESS, PROPERTIES_IS_INTERACTIVE, PROPERTIES_LOCATION,
                                  PROPERTIES_MFA_DETAIL, PROPERTIES_NETWORK_LOCATION, PROPERTIES_PROCESSING_TIME_IN_MILLISECONDS,
                                  PROPERTIES_RESOURCE_DISPLAY_NAME, PROPERTIES_RESOURCE_ID, PROPERTIES_RISK_DETAIL,
                                  PROPERTIES_RISK_EVENT_TYPES, PROPERTIES_RISK_LEVEL_AGGREGATED, PROPERTIES_RISK_LEVEL_DURING_SIGNIN,
                                  PROPERTIES_RISK_STATE, PROPERTIES_STATUS, PROPERTIES_TOKEN_ISSUER_TYPE, PROPERTIES_USER_DISPLAY_NAME,
                                  PROPERTIES_USER_ID, PROPERTIES_USER_PRINCIPAL_NAME, RESOURCE_ID, RESULT_DESCRIPTION, RESULT_SIGNATURE,
                                  RESULT_TYPE, TENANT_ID, EVENT_TIME, LOADED_ON )
FROM (
     SELECT $1, HASH($1), $1:Level::NUMBER, $1:callerIpAddress::STRING, $1:category::STRING, $1:correlationId::STRING, $1:durationMs,
            $1:identity::STRING, $1:location::STRING, $1:operationName::STRING, $1:operationVersion::STRING, $1:properties::VARIANT,
            $1:properties.appDisplayName::STRING, $1:properties.appId::STRING, $1:properties.appliedConditionalAccessPolicies::VARIANT,
            $1:properties.authenticationMethodsUsed::VARIANT, $1:properties.authenticationProcessingDetails::VARIANT,
            $1:properties.clientAppUsed::STRING, $1:properties.conditionalAccessStatus::STRING, $1:properties.createdDateTime::TIMESTAMP_LTZ,
            $1:properties.deviceDetail::VARIANT, $1:properties.id::STRING, $1:properties.ipAddress::STRING, $1:properties.isInteractive::BOOLEAN,
            $1:properties.location::VARIANT, $1:properties.mfaDetail::VARIANT, $1:properties.networkLocationDetails::VARIANT,
            $1:properties.processingTimeInMilliseconds::NUMBER, $1:properties.resourceDisplayName::STRING, $1:properties.resourceId::STRING,
            $1:properties.riskDetail::STRING, $1:properties.riskEventTypes::VARIANT, $1:properties.riskLevelAggregated::STRING,
            $1:properties.riskLevelDuringSignIn::STRING, $1:properties.riskState::VARIANT, $1:properties.status::VARIANT,
            $1:properties.tokenIssuerType::STRING, $1:properties.userDisplayName::STRING, $1:properties.userId::STRING,
            $1:properties.userPrincipalName::STRING, $1:resourceId::STRING, $1:resultDescription::STRING, $1:resultSignature::STRING,
            $1:resultType::STRING, $1:tenantId::STRING, $1:time::TIMESTAMP_LTZ, CURRENT_TIMESTAMP()
    FROM @DATA.{name}_STAGE
)
"""
    db.create_pipe(
        name=f'{name}_PIPE',
        sql=pipe_sql,
        replace=True)


def ingest(name, options):
    name = f"AZURE_AD_SIGNIN_{name}"
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
