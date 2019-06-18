"""Azure Log
Collects Azure AD Signin, AD Activity, or Operation logs into a columnar table
"""

from os import environ
import re

from runners.helpers.auth import load_pkb_rsa
from runners.helpers.dbconfig import PRIVATE_KEY, PRIVATE_KEY_PASSWORD, DATABASE
from runners.helpers.dbconfig import ROLE as SA_ROLE
from runners.helpers import db, log

from azure.storage.blob import BlockBlobService
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile


CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'account_name',
        'title': 'Storage Account',
        'prompt': 'The storage account holding your log blobs',
        'placeholder': 'azstorageaccount',
        'required': True
    },
    {
        'type': 'str',
        'name': 'blob_name',
        'title': 'Blob Name',
        'prompt': 'Blob in the Storage Account containing the logs',
        'placeholder': 'insights-logs',
        'required': True
    },
    {
        'type': 'str',
        'name': 'sas_token',
        'title': 'SAS Token',
        'prompt': "A SAS Token which can list and read the files in the blob.",
        'secret': True,
        'placeholder': (
            '?sv=2010-01-01&ss=abcd&srt=def&sp=gh&se=2011-01-01T00:12:34Z'
            '&st=2011-01-23T45:67:89Z&spr=https&sig=abcdefghijklmnopqrstuvwxyz%3D'
        ),
        'required': True
    },
    {
        'type': 'str',
        'name': 'suffix',
        'title': 'URL Suffix',
        'prompt': 'The Azure URL Suffix for the storage account',
        'default': 'core.windows.net',
        'required': True
    },
    {
        'type': 'select',
        'options': ['audit', 'signin', 'operation'],
        'name': 'log_type',
        'title': 'Log Type',
        'prompt': 'The type of logs you are ingesting to Snowflake.',
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

AZURE_TABLE_COLUMNS = {
    'operation': [
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
        ('LOADED_ON', 'TIMESTAMP_LTZ')
    ],
    'audit': [
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
    ],
    'signin': [
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
}


def connect(name, options):
    name = f"AZURE_{options['log_type']}_{name}"
    comment = f"""
---
module: azure_log
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

    db.execute(f'GRANT USAGE ON STAGE DATA.{name}_STAGE TO ROLE {SA_ROLE}')

    db.create_table(
        name=f'{name}_CONNECTION',
        cols=AZURE_TABLE_COLUMNS[options['log_type']],
        comment=comment
    )

    db.execute(f'GRANT INSERT, SELECT ON DATA.{name}_CONNECTION TO ROLE {SA_ROLE}')

    pipe_sql = {
        'operation': f"""
COPY INTO DATA.{name}_CONNECTION(RAW, HASH_RAW, CALLER_IP_ADDRESS, CATEGORY, CORRELATION_ID, DURATION_MS,
                                 IDENTITY, IDENTITY_AUTHORIZATION, IDENTITY_CLAIMS, LEVEL, LOCATION,
                                 OPERATION_NAME, PROPERTIES, PROPERTIES_ANCESTORS, PROPERTIES_IS_COMPLIANCE_CHECK,
                                 PROPERTIES_POLICIES, PROPERTIES_RESOURCE_LOCAATION, RESOURCE_ID, RESULT_SIGNATURE,
                                 RESULT_TYPE, EVENT_TIME, LOADED_ON)
FROM (
    SELECT $1, HASH($1), $1:callerIpAddress::STRING, $1:category::STRING, $1:correlationId::STRING,
        $1:durationMs::NUMBER, $1:identity::VARIANT, $1:identity.authorization::VARIANT, $1:identity.claims::VARIANT,
        $1:level::STRING, $1:location::STRING, $1:operationName::STRING, $1:properties::VARIANT,
        $1:properties.ancestors::STRING, $1:properties.isComplianceCheck::STRING, PARSE_JSON($1:properties.policies),
        $1:properties.resourceLocation::STRING, $1:resourceId::STRING, $1:resultSignature::STRING,
        $1:resultType::STRING, $1:time::TIMESTAMP_LTZ, CURRENT_TIMESTAMP()
    FROM @DATA.{name}_STAGE)
""",
        'audit': f"""
COPY INTO data.{name}_CONNECTION (RAW, HASH_RAW, CALLER_IP_ADDRESS, CATEGORY, CORRELATION_ID,
                                  DURATION_MS, LEVEL, OPERATION_NAME, OPERATION_VERSION, PROPERTIES,
                                  PROPERTIES_ACTIVITY_DATE_TIME, PROPERTIES_ACTIVITY_DISPLAY_NAME,
                                  PROPERTIES_ADDITIONAL_DETAILS, PROPERTIES_CATEGORY, PROPERTIES_ID,
                                  PROPERTIES_INITIATED_BY, PROPERTIES_LOGGED_BY_SERVICE, PROPERTIES_OPERATION_TYPE,
                                  PROPERTIES_RESULT, PROPERTIES_RESULT_REASON, PROPERTIES_TARGET_RESOURCES,
                                  RESOURCE_ID, RESULT_SIGNATURE, TENANT_ID, EVENT_TIME, LOADED_ON)
FROM (
    SELECT $1, HASH($1), $1:callerIpAddress::STRING, $1:category::STRING, $1:correlationId::STRING,
        $1:durationMs::NUMBER, $1:level::STRING, $1:operationName::STRING, $1:operationVersion::STRING,
        $1:properties::VARIANT, $1:properties.activityDateTime::TIMESTAMP_LTZ,
        $1:properties.activityDisplayName::STRING, $1:properties.additionalDetails::VARIANT,
        $1:properties.category::STRING, $1:properties.id::STRING, $1:properties.initiatedBy::VARIANT,
        $1:properties.loggedByService::STRING, $1:properties.operationType::STRING, $1:properties.result::STRING,
        $1:resultReason::STRING, $1:properties.targetResources::VARIANT, $1:resourceId::STRING,
        $1:resultSignature::STRING, $1:tenantId::STRING, $1:time::TIMESTAMP_LTZ, CURRENT_TIMESTAMP()
  FROM @data.{name}_stage
)
""",
        'signin': f"""
COPY INTO DATA.{name}_CONNECTION (
    RAW, HASH_RAW, LEVEL, CALLER_IP_ADDRESS, CATEGORY, CORRELATION_ID, DURATION_MS,
    IDENTITY, LOCATION, OPERATION_NAME, OPERATION_VERSION, PROPERTIES,
    PROPERTIES_APP_DISPLAY_NAME, PROPERTIES_APP_ID,
    PROPERTIES_APPLIED_CONDITIONAL_ACESS_POLICIES, PROPERTIES_AUTHENTICATION_METHODS_USED,
    PROPERTIES_AUTHENTICATION_PROCESSING_DETAILS, PROPERTIES_CLIENT_APP_USED,
    PROPERTIES_CONDITIONAL_ACCESS_STATUS, PROPERTIES_CREATED_DATE_TIME,
    PROPERTIES_DEVICE_DETAIL, PROPERTIES_ID, PROPERTIES_IP_ADDRESS, PROPERTIES_IS_INTERACTIVE, PROPERTIES_LOCATION,
    PROPERTIES_MFA_DETAIL, PROPERTIES_NETWORK_LOCATION, PROPERTIES_PROCESSING_TIME_IN_MILLISECONDS,
    PROPERTIES_RESOURCE_DISPLAY_NAME, PROPERTIES_RESOURCE_ID, PROPERTIES_RISK_DETAIL,
    PROPERTIES_RISK_EVENT_TYPES, PROPERTIES_RISK_LEVEL_AGGREGATED, PROPERTIES_RISK_LEVEL_DURING_SIGNIN,
    PROPERTIES_RISK_STATE, PROPERTIES_STATUS, PROPERTIES_TOKEN_ISSUER_TYPE, PROPERTIES_USER_DISPLAY_NAME,
    PROPERTIES_USER_ID, PROPERTIES_USER_PRINCIPAL_NAME, RESOURCE_ID, RESULT_DESCRIPTION, RESULT_SIGNATURE,
    RESULT_TYPE, TENANT_ID, EVENT_TIME, LOADED_ON
)
FROM (
    SELECT $1, HASH($1), $1:Level::NUMBER, $1:callerIpAddress::STRING, $1:category::STRING, $1:correlationId::STRING,
        $1:durationMs, $1:identity::STRING, $1:location::STRING, $1:operationName::STRING, $1:operationVersion::STRING,
        $1:properties::VARIANT, $1:properties.appDisplayName::STRING, $1:properties.appId::STRING,
        $1:properties.appliedConditionalAccessPolicies::VARIANT, $1:properties.authenticationMethodsUsed::VARIANT,
        $1:properties.authenticationProcessingDetails::VARIANT, $1:properties.clientAppUsed::STRING,
        $1:properties.conditionalAccessStatus::STRING, $1:properties.createdDateTime::TIMESTAMP_LTZ,
        $1:properties.deviceDetail::VARIANT, $1:properties.id::STRING, $1:properties.ipAddress::STRING,
        $1:properties.isInteractive::BOOLEAN, $1:properties.location::VARIANT, $1:properties.mfaDetail::VARIANT,
        $1:properties.networkLocationDetails::VARIANT, $1:properties.processingTimeInMilliseconds::NUMBER,
        $1:properties.resourceDisplayName::STRING, $1:properties.resourceId::STRING, $1:properties.riskDetail::STRING,
        $1:properties.riskEventTypes::VARIANT, $1:properties.riskLevelAggregated::STRING,
        $1:properties.riskLevelDuringSignIn::STRING, $1:properties.riskState::VARIANT, $1:properties.status::VARIANT,
        $1:properties.tokenIssuerType::STRING, $1:properties.userDisplayName::STRING, $1:properties.userId::STRING,
        $1:properties.userPrincipalName::STRING, $1:resourceId::STRING, $1:resultDescription::STRING,
        $1:resultSignature::STRING, $1:resultType::STRING, $1:tenantId::STRING, $1:time::TIMESTAMP_LTZ,
        CURRENT_TIMESTAMP()
    FROM @DATA.{name}_STAGE
)
"""
    }

    db.create_pipe(
        name=f"{name}_PIPE",
        sql=pipe_sql[options['log_type']],
        replace=True)

    db.execute(f'ALTER PIPE DATA.{name}_PIPE SET PIPE_EXECUTION_PAUSED=true')
    db.execute(f'GRANT OWNERSHIP ON PIPE DATA.{name}_PIPE TO ROLE {SA_ROLE}')

    return {'newStage': 'finalized', 'newMessage': 'Table, Stage, and Pipe created'}


def ingest(name, options):
    required_envars = {'SA_USER', 'SNOWFLAKE_ACCOUNT'}
    missing_envars = required_envars - set(environ)
    if missing_envars:
        raise RuntimeError(f'Missing required env variable(s): {missing_envars}')

    block_blob_service = BlockBlobService(
        account_name=options['storage_account'],
        sas_token=options['sas_token'],
        endpoint_suffix=options['suffix']
    )
    base_name = re.sub(r'_CONNECTION$', '', name)

    db.execute(f"select SYSTEM$PIPE_FORCE_RESUME('DATA.{base_name}_PIPE');")

    files = block_blob_service.list_blobs(options['blob_name'])

    timestamp_query = f"""
    SELECT loaded_on
    FROM data.{name}
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
    ingest_manager = SimpleIngestManager(
        account=environ.get('SNOWFLAKE_ACCOUNT'),
        host=f'{environ.get("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com',
        user=environ.get('SA_USER'),
        pipe=f'{DATABASE}.DATA.{base_name}_PIPE',
        private_key=load_pkb_rsa(PRIVATE_KEY, PRIVATE_KEY_PASSWORD)
    )

    if len(new_files) > 0:
        try:
            response = ingest_manager.ingest_files(new_files)
            log.info(response)
        except Exception as e:
            log.error(e)
            return


def test(name):
    yield True
