"""Azure Active Directory Logs
Collect AD Signin, Audit, or Operation Logs using an SAS Token
"""

from runners.helpers.dbconfig import ROLE as SA_ROLE, WAREHOUSE
from runners.helpers import db

from .utils import yaml_dump

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'account_name',
        'title': "Storage Account",
        'prompt': "Your storage account with the container where Azure sends logs",
        'placeholder': "azstorageaccount",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'container_name',
        'title': "Container Name",
        'prompt': "Your storage container where Azure sends logs",
        'placeholder': "insights-logs",
        'required': True,
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
        'required': True,
    },
    {
        'type': 'str',
        'name': 'cloud_type',
        'options': [
            {'value': 'reg', 'label': "Azure Cloud"},
            {'value': 'gov', 'label': "Azure Gov Cloud"},
        ],
        'title': "Cloud Type",
        'placeholder': "Choose Cloud Type",
        'prompt': "Azure provides two types of clouds: regular and government",
        'required': True,
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
        'required': True,
    },
    {
        'type': 'str',
        'name': 'suffix',
        'title': "Endpoint Suffix (optional)",
        'prompt': "If using Azure Storage in an independent cloud, modify the endpoint suffix below",
        'default': 'core.windows.net',
        'required': True,
    },
]

FILE_FORMAT = db.TypeOptions(
    type='JSON',
    compression='AUTO',
    enable_octal=False,
    allow_duplicate=False,
    strip_outer_array=False,
    strip_null_values=False,
    ignore_utf8_errors=False,
    skip_byte_order_mark=True,
)

LANDING_TABLES_COLUMNS = {
    'operation': [
        ('raw', 'VARIANT'),
        ('hash_raw', 'NUMBER'),
        ('caller_ip_address', 'VARCHAR'),
        ('category', 'VARCHAR'),
        ('correlation_id', 'VARCHAR'),
        ('duration_ms', 'NUMBER'),
        ('identity', 'VARIANT'),
        ('identity_authorization', 'VARIANT'),
        ('identity_claims', 'VARIANT'),
        ('level', 'VARCHAR'),
        ('location', 'VARCHAR'),
        ('operation_name', 'VARCHAR'),
        ('properties', 'VARIANT'),
        ('properties_ancestors', 'VARCHAR'),
        ('properties_is_compliance_check', 'VARCHAR'),
        ('properties_policies', 'VARIANT'),
        ('properties_resource_location', 'VARCHAR'),
        ('resource_id', 'VARCHAR'),
        ('result_signature', 'VARCHAR'),
        ('result_type', 'VARCHAR'),
        ('event_time', 'TIMESTAMP_LTZ'),
        ('loaded_on', 'TIMESTAMP_LTZ'),
    ],
    'audit': [
        ('raw', 'VARIANT'),
        ('hash_raw', 'NUMBER(38,0)'),
        ('caller_ip_address', 'VARCHAR'),
        ('category', 'VARCHAR'),
        ('correlation_id', 'VARCHAR'),
        ('duration_ms', 'NUMBER'),
        ('level', 'VARCHAR'),
        ('operation_name', 'VARCHAR'),
        ('operation_version', 'VARCHAR'),
        ('properties', 'VARIANT'),
        ('properties_activity_date_time', 'TIMESTAMP_LTZ(9)'),
        ('properties_activity_display_name', 'VARCHAR'),
        ('properties_additional_details', 'VARIANT'),
        ('properties_category', 'VARCHAR'),
        ('properties_id', 'VARCHAR'),
        ('properties_initiated_by', 'VARIANT'),
        ('properties_logged_by_service', 'VARCHAR'),
        ('properties_operation_type', 'VARCHAR'),
        ('properties_result', 'VARCHAR'),
        ('properties_result_reason', 'VARCHAR'),
        ('properties_target_resources', 'VARIANT'),
        ('resource_id', 'VARCHAR'),
        ('result_signature', 'VARCHAR'),
        ('tenant_id', 'VARCHAR'),
        ('event_time', 'TIMESTAMP_LTZ(9)'),
        ('loaded_on', 'TIMESTAMP_LTZ(9)'),
    ],
    'signin': [
        ('raw', 'VARIANT'),
        ('hash_raw', 'NUMBER'),
        ('level', 'NUMBER'),
        ('caller_ip_address', 'VARCHAR'),
        ('category', 'VARCHAR'),
        ('correlation_id', 'VARCHAR'),
        ('duration_ms', 'NUMBER'),
        ('identity', 'VARCHAR'),
        ('location', 'VARCHAR'),
        ('operation_name', 'VARCHAR'),
        ('operation_version', 'VARCHAR'),
        ('properties', 'VARIANT'),
        ('properties_app_display_name', 'VARCHAR'),
        ('properties_app_id', 'VARCHAR'),
        ('properties_applied_conditional_acess_policies', 'VARIANT'),
        ('properties_authentication_methods_used', 'VARIANT'),
        ('properties_authentication_processing_details', 'VARIANT'),
        ('properties_client_app_used', 'VARCHAR'),
        ('properties_conditional_access_status', 'VARCHAR'),
        ('properties_created_date_time', 'TIMESTAMP_LTZ'),
        ('properties_device_detail', 'VARIANT'),
        ('properties_id', 'VARCHAR'),
        ('properties_ip_address', 'VARCHAR'),
        ('properties_is_interactive', 'BOOLEAN'),
        ('properties_location', 'VARIANT'),
        ('properties_mfa_detail', 'VARIANT'),
        ('properties_network_location', 'VARIANT'),
        ('properties_processing_time_in_milliseconds', 'NUMBER'),
        ('properties_resource_display_name', 'VARCHAR'),
        ('properties_resource_id', 'VARCHAR'),
        ('properties_risk_detail', 'VARCHAR'),
        ('properties_risk_event_types', 'VARIANT'),
        ('properties_risk_level_aggregated', 'VARCHAR'),
        ('properties_risk_level_during_signin', 'VARCHAR'),
        ('properties_risk_state', 'VARCHAR'),
        ('properties_status', 'VARIANT'),
        ('properties_token_issuer_type', 'VARCHAR'),
        ('properties_user_display_name', 'VARCHAR'),
        ('properties_user_id', 'VARCHAR'),
        ('properties_user_principal_name', 'VARCHAR'),
        ('resource_id', 'VARCHAR'),
        ('result_description', 'VARCHAR'),
        ('result_signature', 'VARCHAR'),
        ('result_type', 'VARCHAR'),
        ('tenant_id', 'VARCHAR'),
        ('event_time', 'TIMESTAMP_LTZ'),
        ('loaded_on', 'TIMESTAMP_LTZ'),
    ],
}

GET_TIMESTAMP_FROM_FILENAME_SQL = {
    'operation': r'''to_timestamp_ltz(
substr(metadata$filename, 79, 4)
|| '-' ||
substr(metadata$filename, 86, 2)
|| '-' ||
substr(metadata$filename, 91, 2)
|| 'T' ||
substr(metadata$filename, 96, 2)
|| ':' ||
substr(metadata$filename, 101, 2))
''',
    'audit': r'''to_timestamp_ltz(
substr(metadata$filename, 49, 4)
|| '-' ||
substr(metadata$filename, 56, 2)
|| '-' ||
substr(metadata$filename, 61, 2)
|| 'T' ||
substr(metadata$filename, 66, 2)
|| ':' ||
substr(metadata$filename, 71, 2))
    ''',
    'signin': r'''to_timestamp_ltz(
substr(metadata$filename, 49, 4)
|| '-' ||
substr(metadata$filename, 56, 2)
|| '-' ||
substr(metadata$filename, 61, 2)
|| 'T' ||
substr(metadata$filename, 66, 2)
|| ':' ||
substr(metadata$filename, 71, 2))
''',
}

# This requires External Tables to support REGEXP_REPLACE, which they currently do not.
# r'''TO_TIMESTAMP_LTZ(REGEXP_REPLACE(
#  METADATA$FILENAME,
#  '.*y=(\d*).m=(\d*).d=(\d*).h=(\d*).m=(\d*).*json$',
#  '\1-\2-\3T\4:\5',
#  1, 1, 'e'
# ))'''


def connect(connection_name, options):
    connection_type = options['connection_type']

    base_name = f"azure_log_{connection_name}_{connection_type}"
    account_name = options['account_name']
    container_name = options['container_name']
    suffix = options['suffix']
    cloud_type = options['cloud_type']
    sas_token = options['sas_token']

    comment = yaml_dump(module='azure_log')

    db.create_stage(
        name=f'data.{base_name}_stage',
        url=f"azure://{account_name}.blob.{suffix}/{container_name}",
        cloud='azure',
        prefix='',
        credentials=sas_token,
        file_format=FILE_FORMAT,
    )

    db.execute(f'GRANT USAGE ON STAGE data.{base_name}_stage TO ROLE {SA_ROLE}')

    db.create_table(
        name=f'data.{base_name}_connection',
        cols=LANDING_TABLES_COLUMNS[connection_type],
        comment=comment,
        ifnotexists=True,
    )

    db.execute(f'GRANT INSERT, SELECT ON data.{base_name}_connection TO ROLE {SA_ROLE}')

    external_table_columns = [
        (
            'timestamp_part',
            'TIMESTAMP_LTZ',
            GET_TIMESTAMP_FROM_FILENAME_SQL[connection_type],
        )
    ]

    db.create_external_table(
        name=f'data.{base_name}_external',
        location=f'@data.{base_name}_stage',
        cols=external_table_columns,
        partition='timestamp_part',
        file_format=db.TypeOptions(type='JSON'),
    )

    db.execute(f'GRANT SELECT ON data.{base_name}_external TO ROLE {SA_ROLE}')

    stored_proc_def = f"""
var sql_command = "ALTER EXTERNAL TABLE data.{base_name}_external REFRESH";
try {{
    snowflake.execute ({{sqlText: sql_command}});
    return "Succeeded.";
}} catch (err)  {{
    return "Failed: " + err;
}}
"""

    db.create_stored_procedure(
        name=f'data.{base_name}_procedure',
        args=[],
        return_type='string',
        executor='OWNER',
        definition=stored_proc_def,
    )

    refresh_task_sql = f'CALL data.{base_name}_procedure()'
    db.create_task(
        name=f'data.{base_name}_refresh_task',
        warehouse=WAREHOUSE,
        schedule='5 minutes',
        sql=refresh_task_sql,
    )

    select_statement_sql = {
        'reg': (
            f"SELECT value "
            f"FROM data.{base_name}_external "
            f"WHERE timestamp_part >= DATEADD(HOUR, -2, CURRENT_TIMESTAMP())"
        ),
        'gov': (
            f"SELECT value FROM ("
            f"  SELECT value AS a "
            f"  FROM data.{base_name}_external"
            f"  WHERE timestamp_part >= DATEADD(HOUR, -2, CURRENT_TIMESTAMP())"
            f"), LATERAL FLATTEN (INPUT => a:records)"
        ),
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
MERGE INTO data.{base_name}_connection a
USING (
  {select_statement_sql[cloud_type]}
) b
ON a.raw = b.value
WHEN NOT MATCHED THEN
{insert_task_sql[connection_type]}
"""

    db.create_task(
        name=f'data.{base_name}_ingest_task',
        warehouse=WAREHOUSE,
        schedule=f'AFTER data.{base_name}_refresh_task',
        sql=ingest_task_sql,
    )

    return {
        'newStage': 'finalized',
        'newMessage': 'Created Stage, Tables, Stored Procedure, and Tasks.',
    }
