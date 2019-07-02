"""Azure Activity Logs
Collect Active Directory (AD) Sign-ins, Audit, or Operation Logs
"""

import re

from runners.helpers.auth import load_pkb_rsa
from runners.helpers.dbconfig import PRIVATE_KEY, PRIVATE_KEY_PASSWORD, DATABASE, USER, ACCOUNT
from runners.helpers.dbconfig import ROLE as SA_ROLE
from runners.helpers import db, log, vault
from runners.utils import groups_of

from azure.storage.blob import BlockBlobService
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile


CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'account_name',
        'title': "Storage Account",
        'prompt': "Your storage account with the container where Azure sends logs",
        'placeholder': "azstorageaccount",
        'required': True
    },
    {
        'type': 'str',
        'name': 'container_name',
        'title': "Container Name",
        'prompt': "Your storage container where Azure sends logs",
        'placeholder': "insights-logs",
        'required': True
    },
    {
        'type': 'str',
        'name': 'sas_token',
        'title': "SAS Token",
        'prompt': "Access Token which can list and read the log files in your storage account",
        'mask_on_screen': True,
        'placeholder': (
            "?sv=2010-01-01&ss=abcd&srt=def&sp=gh&se=2011-01-01T00:12:34Z"
            "&st=2011-01-23T45:67:89Z&spr=https&sig=abcdefghijklmnopqrstuvwxyz%3D"
        ),
        'required': True
    },
    {
        'type': 'str',
        'name': 'connection_type',
        'options': [
            {'value': 'signin', 'label': "Active Directory (AD) Sign-in Logs"},
            {'value': 'audit', 'label': "Active Directory (AD) Audit Logs"},
            {'value': 'operation', 'label': "Operation Logs"},
        ],
        'title': "Log Type",
        'placeholder': "Choose Log Type",
        'prompt': "Azure provides several activity log streams, choose one for this connector",
        'required': True
    },
    {
        'type': 'str',
        'name': 'suffix',
        'title': "Endpoint Suffix (optional)",
        'prompt': "If using Azure Storage in an independent cloud, modify the endpoint suffix below",
        'default': 'core.windows.net',
        'required': True
    },
]

FILE_FORMAT = '''
    TYPE = "JSON",
    COMPRESSION = "AUTO",
    ENABLE_OCTAL = FALSE,
    ALLOW_DUPLICATE = FALSE,
    STRIP_OUTER_ARRAY = FALSE,
    STRIP_NULL_VALUES = FALSE,
    IGNORE_UTF8_ERRORS = FALSE,
    SKIP_BYTE_ORDER_MARK = TRUE
'''

LANDING_TABLES_COLUMNS = {
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


def connect(connection_name, options):
    connection_type = options['connection_type']

    base_name = f"azure_{connection_name}_{connection_type}"
    account_name = options['account_name']
    container_name = options['container_name']
    suffix = options['suffix']
    sas_token = options['sas_token']
    sas_token_ct = vault.encrypt(sas_token)

    comment = f'''
---
module: azure
storage_account: {account_name}
container_name: {container_name}
suffix: {suffix}
sas_token: {sas_token_ct}
sa_user: {USER}
snowflake_account: {ACCOUNT}
database: {DATABASE}
'''

    db.create_stage(
        name=f'data.{base_name}_STAGE',
        url=f"azure://{account_name}.blob.{suffix}/{container_name}",
        cloud='azure',
        prefix='',
        credentials=sas_token,
        file_format=FILE_FORMAT
    )

    db.execute(f'GRANT USAGE ON STAGE data.{base_name}_STAGE TO ROLE {SA_ROLE}')

    db.create_table(
        name=f'data.{base_name}_CONNECTION',
        cols=LANDING_TABLES_COLUMNS[connection_type],
        comment=comment
    )

    db.execute(f'GRANT INSERT, SELECT ON data.{base_name}_CONNECTION TO ROLE {SA_ROLE}')

    pipe_sql = {
        'operation': f'''
COPY INTO DATA.{base_name}_CONNECTION(RAW, HASH_RAW, CALLER_IP_ADDRESS, CATEGORY, CORRELATION_ID, DURATION_MS,
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
    FROM @DATA.{base_name}_STAGE)
''',
        'audit': f'''
COPY INTO data.{base_name}_CONNECTION (RAW, HASH_RAW, CALLER_IP_ADDRESS, CATEGORY, CORRELATION_ID,
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
  FROM @data.{base_name}_STAGE
)
''',
        'signin': f'''
COPY INTO DATA.{base_name}_CONNECTION (
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
        $1:durationMs, $1:identity::STRING, $1:location::STRING, $1:operationName::STRING,
        $1:operationVersion::STRING, $1:properties::VARIANT, $1:properties.appDisplayName::STRING,
        $1:properties.appId::STRING, $1:properties.appliedConditionalAccessPolicies::VARIANT,
        $1:properties.authenticationMethodsUsed::VARIANT, $1:properties.authenticationProcessingDetails::VARIANT,
        $1:properties.clientAppUsed::STRING, $1:properties.conditionalAccessStatus::STRING,
        $1:properties.createdDateTime::TIMESTAMP_LTZ, $1:properties.deviceDetail::VARIANT, $1:properties.id::STRING,
        $1:properties.ipAddress::STRING, $1:properties.isInteractive::BOOLEAN, $1:properties.location::VARIANT,
        $1:properties.mfaDetail::VARIANT, $1:properties.networkLocationDetails::VARIANT,
        $1:properties.processingTimeInMilliseconds::NUMBER, $1:properties.resourceDisplayName::STRING,
        $1:properties.resourceId::STRING, $1:properties.riskDetail::STRING, $1:properties.riskEventTypes::VARIANT,
        $1:properties.riskLevelAggregated::STRING, $1:properties.riskLevelDuringSignIn::STRING,
        $1:properties.riskState::VARIANT, $1:properties.status::VARIANT, $1:properties.tokenIssuerType::STRING,
        $1:properties.userDisplayName::STRING, $1:properties.userId::STRING, $1:properties.userPrincipalName::STRING,
        $1:resourceId::STRING, $1:resultDescription::STRING, $1:resultSignature::STRING, $1:resultType::STRING,
        $1:tenantId::STRING, $1:time::TIMESTAMP_LTZ,
        CURRENT_TIMESTAMP()
    FROM @DATA.{base_name}_STAGE
)
'''
    }

    db.create_pipe(
        name=f"data.{base_name}_PIPE",
        sql=pipe_sql[options['connection_type']],
        replace=True
    )

    db.execute(f'ALTER PIPE data.{base_name}_PIPE SET PIPE_EXECUTION_PAUSED=true')
    db.execute(f'GRANT OWNERSHIP ON PIPE data.{base_name}_PIPE TO ROLE {SA_ROLE}')

    return {'newStage': 'finalized', 'newMessage': 'Table, Stage, and Pipe created'}


def ingest(table_name, options):
    base_name = re.sub(r'_CONNECTION$', '', table_name)
    storage_account = options['storage_account']
    sas_token = vault.decrypt_if_encrypted(options['sas_token'])
    suffix = options['suffix']
    container_name = options['container_name']
    snowflake_account = options['snowflake_account']
    sa_user = options['sa_user']
    database = options['database']

    block_blob_service = BlockBlobService(
        account_name=storage_account,
        sas_token=sas_token,
        endpoint_suffix=suffix
    )

    db.execute(f"select SYSTEM$PIPE_FORCE_RESUME('DATA.{base_name}_PIPE');")

    last_loaded = db.fetch_latest(f'data.{table_name}', 'loaded_on')

    log.info(f"Last loaded time is {last_loaded}")

    blobs = block_blob_service.list_blobs(container_name)
    new_files = [
        StagedFile(b.name, None) for b in blobs if (
            last_loaded is None or b.properties.creation_time > last_loaded
        )
    ]

    log.info(f"Found {len(new_files)} files to ingest")

    # Proxy object that abstracts the Snowpipe REST API
    ingest_manager = SimpleIngestManager(
        account=snowflake_account,
        host=f'{snowflake_account}.snowflakecomputing.com',
        user=sa_user,
        pipe=f'{database}.data.{base_name}_PIPE',
        private_key=load_pkb_rsa(PRIVATE_KEY, PRIVATE_KEY_PASSWORD)
    )

    if len(new_files) > 0:
        for file_group in groups_of(4999, new_files):
            response = ingest_manager.ingest_files(file_group)
            log.info(response)
            yield len(file_group)
