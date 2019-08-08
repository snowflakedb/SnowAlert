"""Azure Active Directory Logs
Collect AD Signin, Audit, or Operation Logs using an SAS Token
"""

from runners.helpers.dbconfig import ROLE as SA_ROLE, WAREHOUSE
from runners.helpers import db


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
        'name': 'cloud_type',
        'options': [
            {'value': 'reg', 'label': "Azure Cloud"},
            {'value': 'gov', 'label': "Azure Gov Cloud"}
        ],
        'title': "Cloud Type",
        'placeholder': "Choose Cloud Type",
        'prompt': "Azure provides two types of clouds: regular and government",
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
        ('PROPERTIES_RESOURCE_LOCATION', 'VARCHAR'),
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

EXTERNAL_TABLE_COLUMNS = [
    ('timestamp_part',
     'timestamp_ltz',
     '''to_timestamp_ltz(regexp_replace(
  metadata$filename,
  '.*y=(\\d*).m=(\\d*).d=(\\d*).h=(\\d*).m=(\\d*).*json$', '\\1-\\2-\\3T\\4:\\5', 1,1,'e'))''')
]


def connect(connection_name, options):
    connection_type = options['connection_type']

    base_name = f"azure_log_{connection_name}_{connection_type}"
    account_name = options['account_name']
    container_name = options['container_name']
    suffix = options['suffix']
    cloud_type = options['cloud_type']
    sas_token = options['sas_token']

    comment = f'''
---
module: azure_log
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
        comment=comment,
        ifnotexists=True
    )

    db.execute(f'GRANT INSERT, SELECT ON data.{base_name}_CONNECTION TO ROLE {SA_ROLE}')

    db.create_external_table(name=f'data.{base_name}_EXTERNAL',
                             location=f'@data.{base_name}_STAGE',
                             cols=EXTERNAL_TABLE_COLUMNS,
                             partition='timestamp_part',
                             file_format='TYPE=JSON')

    db.execute(f'GRANT SELECT ON data.{base_name}_EXTERNAL TO ROLE {SA_ROLE}')

    stored_proc_def = f"""
var sql_command =
 "alter external table data.{base_name}_EXTERNAL REFRESH";
try {{
    snowflake.execute (
        {{sqlText: sql_command}}
        );
    return "Succeeded.";   // Return a success/error indicator.
    }}
catch (err)  {{
    return "Failed: " + err;   // Return a success/error indicator.
    }}
"""

    db.create_stored_procedure(name=f'data.{base_name}_PROCEDURE', args=[], return_type='string', executor='OWNER', definition=stored_proc_def)

    refresh_task_sql = f'CALL data.{base_name}_PROCEDURE()'
    db.create_task(name=f'data.{base_name}_REFRESH_TASK',
                   warehouse=WAREHOUSE,
                   schedule='5 minutes',
                   sql=refresh_task_sql)

    select_statement_sql = {'reg': f"SELECT VALUE FROM DATA.{base_name}_EXTERNAL WHERE TIMESTAMP_PART >= DATEADD(HOUR, -2, CURRENT_TIMESTAMP())",
                            'gov': f"SELECT VALUE FROM (SELECT VALUE AS A FROM DATA.{base_name}_EXTERNAL WHERE TIMESTAMP_PART >= DATEADD(HOUR, -2, CURRENT_TIMESTAMP())), LATERAL FLATTEN (INPUT => A:records)"
                            }

    insert_task_sql = {
        'operation': f"""
INSERT (
    RAW, HASH_RAW, CALLER_IP_ADDRESS, CATEGORY, CORRELATION_ID, DURATION_MS,
    IDENTITY, IDENTITY_AUTHORIZATION, IDENTITY_CLAIMS, LEVEL, LOCATION,
    OPERATION_NAME, PROPERTIES, PROPERTIES_ANCESTORS, PROPERTIES_IS_COMPLIANCE_CHECK,
    PROPERTIES_POLICIES, PROPERTIES_RESOURCE_LOCATION, RESOURCE_ID, RESULT_SIGNATURE,
    RESULT_TYPE, EVENT_TIME, LOADED_ON
) VALUES (
    VALUE, HASH(VALUE), VALUE:callerIpAddress::STRING, VALUE:category::STRING, VALUE:correlationId::STRING,
    VALUE:durationMs::NUMBER, VALUE:identity::VARIANT, VALUE:identity.authorization::VARIANT,
    VALUE:identity.claims::VARIANT, VALUE:level::STRING, VALUE:location::STRING, VALUE:operationName::STRING,
    VALUE:properties::VARIANT, VALUE:properties.ancestors::STRING, VALUE:properties.isComplianceCheck::STRING,
    PARSE_JSON(VALUE:properties.policies),VALUE:properties.resourceLocation::STRING, VALUE:resourceId::STRING,
    VALUE:resultSignature::STRING,VALUE:resultType::STRING, value:time::TIMESTAMP_LTZ, CURRENT_TIMESTAMP()
)
""",
        'audit': f"""
INSERT (
    RAW, HASH_RAW, CALLER_IP_ADDRESS, CATEGORY, CORRELATION_ID,
    DURATION_MS, LEVEL, OPERATION_NAME, OPERATION_VERSION, PROPERTIES,
    PROPERTIES_ACTIVITY_DATE_TIME, PROPERTIES_ACTIVITY_DISPLAY_NAME,
    PROPERTIES_ADDITIONAL_DETAILS, PROPERTIES_CATEGORY, PROPERTIES_ID,
    PROPERTIES_INITIATED_BY, PROPERTIES_LOGGED_BY_SERVICE, PROPERTIES_OPERATION_TYPE,
    PROPERTIES_RESULT, PROPERTIES_RESULT_REASON, PROPERTIES_TARGET_RESOURCES,
    RESOURCE_ID, RESULT_SIGNATURE, TENANT_ID, EVENT_TIME, LOADED_ON
) VALUES (
    VALUE, HASH(VALUE), VALUE:callerIpAddress::STRING, VALUE:category::STRING, VALUE:correlationId::STRING,
    VALUE:durationMs::NUMBER, VALUE:level::STRING, VALUE:operationName::STRING, VALUE:operationVersion::STRING,
    VALUE:properties::VARIANT, VALUE:properties.activityDateTime::TIMESTAMP_LTZ,
    VALUE:properties.activityDisplayName::STRING, VALUE:properties.additionalDetails::VARIANT,
    VALUE:properties.category::STRING, VALUE:properties.id::STRING, VALUE:properties.initiatedBy::VARIANT,
    VALUE:properties.loggedByService::STRING, VALUE:properties.operationType::STRING, VALUE:properties.result::STRING,
    VALUE:resultReason::STRING, VALUE:properties.targetResources::VARIANT, VALUE:resourceId::STRING,
    VALUE:resultSignature::STRING, VALUE:tenantId::STRING, VALUE:time::TIMESTAMP_LTZ, CURRENT_TIMESTAMP()
)
""",
        'signin': f"""
INSERT (
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
) VALUES (
    VALUE, HASH(VALUE), VALUE:Level::NUMBER, VALUE:callerIpAddress::STRING, VALUE:category::STRING,
    VALUE:correlationId::STRING, VALUE:durationMs, VALUE:identity::STRING, VALUE:location::STRING,
    VALUE:operationName::STRING, VALUE:operationVersion::STRING, VALUE:properties::VARIANT,
    VALUE:properties.appDisplayName::STRING, VALUE:properties.appId::STRING,
    VALUE:properties.appliedConditionalAccessPolicies::VARIANT, VALUE:properties.authenticationMethodsUsed::VARIANT,
    VALUE:properties.authenticationProcessingDetails::VARIANT, VALUE:properties.clientAppUsed::STRING,
    VALUE:properties.conditionalAccessStatus::STRING, VALUE:properties.createdDateTime::TIMESTAMP_LTZ,
    VALUE:properties.deviceDetail::VARIANT, VALUE:properties.id::STRING, VALUE:properties.ipAddress::STRING,
    VALUE:properties.isInteractive::BOOLEAN, VALUE:properties.location::VARIANT,
    VALUE:properties.mfaDetail::VARIANT, VALUE:properties.networkLocationDetails::VARIANT,
    VALUE:properties.processingTimeInMilliseconds::NUMBER, VALUE:properties.resourceDisplayName::STRING,
    VALUE:properties.resourceId::STRING, VALUE:properties.riskDetail::STRING,
    VALUE:properties.riskEventTypes::VARIANT, VALUE:properties.riskLevelAggregated::STRING,
    VALUE:properties.riskLevelDuringSignIn::STRING, VALUE:properties.riskState::VARIANT,
    VALUE:properties.status::VARIANT, VALUE:properties.tokenIssuerType::STRING,
    VALUE:properties.userDisplayName::STRING, VALUE:properties.userId::STRING,
    VALUE:properties.userPrincipalName::STRING, VALUE:resourceId::STRING, VALUE:resultDescription::STRING,
    VALUE:resultSignature::STRING, VALUE:resultType::STRING, VALUE:tenantId::STRING, VALUE:time::TIMESTAMP_LTZ,
    CURRENT_TIMESTAMP()
)
""",
    }

    ingest_task_sql = f"""
MERGE INTO data.{base_name}_CONNECTION A
USING (
  {select_statement_sql[cloud_type]}
) B
on A.RAW = B.VALUE
WHEN NOT MATCHED THEN
{insert_task_sql[connection_type]}
"""

    db.create_task(name=f'data.{base_name}_INGEST_TASK',
                   warehouse=WAREHOUSE,
                   schedule=f'AFTER DATA.{base_name}_REFRESH_TASK',
                   sql=ingest_task_sql)

    return {'newStage': 'finalized', 'newMessage': 'Table, Stage, External Table, Stored Procedure, and Tasks created.'}
