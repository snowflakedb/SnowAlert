"""AWS CloudTrail
Collect AWS CloudTrail logs from S3 using a privileged Role
"""

from json import dumps

from runners.helpers import db
from runners.helpers.dbconfig import WAREHOUSE
from time import sleep

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'bucket_name',
        'title': "Source S3 Bucket",
        'prompt': "Your S3 bucket where AWS sends CloudTrail",
        'prefix': "s3://",
        'placeholder': "my-cloudtrail-s3-bucket",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'aws_role',
        'title': "CloudTrail Bucket Reader Role",
        'prompt': "Role to be assumed for access to CloudTrail files in S3",
        'placeholder': "arn:aws:iam::012345678987:role/my-cloudtrail-read-role",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'filter',
        'title': "Prefix Filter (optional)",
        'prompt': "Folder in S3 bucket where CloudTrail puts logs",
        'default': "AWSLogs/",
        'required': True,
    },
]

FILE_FORMAT = '''
    TYPE = "JSON",
    COMPRESSION = "AUTO",
    ENABLE_OCTAL = FALSE,
    ALLOW_DUPLICATE = FALSE,
    STRIP_OUTER_ARRAY = TRUE,
    STRIP_NULL_VALUES = FALSE,
    IGNORE_UTF8_ERRORS = FALSE,
    SKIP_BYTE_ORDER_MARK = TRUE
'''

LANDING_TABLE_COLUMNS = [
    ('insert_id', 'NUMBER IDENTITY START 1 INCREMENT 1'),
    ('insert_time', 'TIMESTAMP_LTZ(9)'),
    ('raw', 'VARIANT'),
    ('hash_raw', 'NUMBER'),
    ('event_time', 'TIMESTAMP_LTZ(9)'),
    ('aws_region', 'STRING'),
    ('event_id', 'STRING'),
    ('event_name', 'STRING'),
    ('event_source', 'STRING'),
    ('event_type', 'STRING'),
    ('event_version', 'STRING'),
    ('recipient_account_id', 'STRING'),
    ('request_id', 'STRING'),
    ('request_parameters', 'VARIANT'),
    ('response_elements', 'VARIANT'),
    ('source_ip_address', 'STRING'),
    ('user_agent', 'STRING'),
    ('user_identity', 'VARIANT'),
    ('user_identity_type', 'STRING'),
    ('user_identity_principal_id', 'STRING'),
    ('user_identity_arn', 'STRING'),
    ('user_identity_accountid', 'STRING'),
    ('user_identity_invokedby', 'STRING'),
    ('user_identity_access_key_id', 'STRING'),
    ('user_identity_username', 'STRING'),
    ('user_identity_session_context_attributes_mfa_authenticated', 'BOOLEAN'),
    ('user_identity_session_context_attributes_creation_date', 'STRING'),
    ('user_identity_session_context_session_issuer_type', 'STRING'),
    ('user_identity_session_context_session_issuer_principal_id', 'STRING'),
    ('user_identity_session_context_session_issuer_arn', 'STRING'),
    ('user_identity_session_context_session_issuer_account_id', 'STRING'),
    ('user_identity_session_context_session_issuer_user_name', 'STRING'),
    ('error_code', 'STRING'),
    ('error_message', 'STRING'),
    ('additional_event_data', 'VARIANT'),
    ('api_version', 'STRING'),
    ('read_only', 'BOOLEAN'),
    ('resources', 'VARIANT'),
    ('service_event_details', 'STRING'),
    ('shared_event_id', 'STRING'),
    ('vpc_endpoint_id', 'STRING'),
]


CONNECT_RESPONSE_MESSAGE = '''
STEP 1: Modify the Role "{role}" to include the following trust relationship:

{role_trust_relationship}


STEP 2: For Role "{role}", add the following inline policy:

{role_policy}
'''


def connect(connection_name, options):
    base_name = f'AWS_CLOUDTRAIL_{connection_name}_EVENTS'.upper()
    stage = f'data.{base_name}_STAGE'
    staging_table = f'data.{base_name}_STAGING'
    landing_table = f'data.{base_name}_CONNECTION'

    bucket = options['bucket_name']
    prefix = options.get('filter', 'AWSLogs/')
    role = options['aws_role']

    comment = f'''
---
module: aws_cloudtrail
'''

    db.create_stage(
        name=stage,
        url=f's3://{bucket}',
        prefix=prefix,
        cloud='aws',
        credentials=role,
        file_format=FILE_FORMAT,
    )

    db.create_table(name=staging_table, cols=[('v', 'variant')])

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment=comment)

    stage_props = db.fetch_props(
        f'DESC STAGE {stage}', filter=('AWS_EXTERNAL_ID', 'SNOWFLAKE_IAM_USER')
    )

    prefix = prefix.rstrip('/')

    return {
        'newStage': 'created',
        'newMessage': CONNECT_RESPONSE_MESSAGE.format(
            role=role,
            role_trust_relationship=dumps(
                {
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': {'AWS': stage_props['SNOWFLAKE_IAM_USER']},
                            'Action': 'sts:AssumeRole',
                            'Condition': {
                                'StringEquals': {
                                    'sts:ExternalId': stage_props['AWS_EXTERNAL_ID']
                                }
                            },
                        }
                    ],
                },
                indent=4,
            ),
            role_policy=dumps(
                {
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': ['s3:GetObject', 's3:GetObjectVersion'],
                            'Resource': f'arn:aws:s3:::{bucket}/{prefix}/*',
                        },
                        {
                            'Effect': 'Allow',
                            'Action': 's3:ListBucket',
                            'Resource': f'arn:aws:s3:::{bucket}',
                            'Condition': {'StringLike': {'s3:prefix': [f'{prefix}/*']}},
                        },
                    ],
                },
                indent=4,
            ),
        ),
    }


def finalize(connection_name):
    base_name = f'AWS_CLOUDTRAIL_{connection_name}_EVENTS'.upper()
    pipe = f'data.{base_name}_PIPE'
    landing_table = f'data.{base_name}_CONNECTION'

    # Step two: Configure the remainder once the role is properly configured.
    cloudtrail_ingest_task = f'''
INSERT INTO {landing_table} (
  insert_time, raw, hash_raw, event_time, aws_region, event_id, event_name, event_source, event_type,
  event_version, recipient_account_id, request_id, request_parameters, response_elements, source_ip_address,
  user_agent, user_identity, user_identity_type, user_identity_principal_id, user_identity_arn,
  user_identity_accountid, user_identity_invokedby, user_identity_access_key_id, user_identity_username,
  user_identity_session_context_attributes_mfa_authenticated, user_identity_session_context_attributes_creation_date,
  user_identity_session_context_session_issuer_type, user_identity_session_context_session_issuer_principal_id,
  user_identity_session_context_session_issuer_arn, user_identity_session_context_session_issuer_account_id,
  user_identity_session_context_session_issuer_user_name, error_code, error_message, additional_event_data,
  api_version, read_only, resources, service_event_details, shared_event_id, vpc_endpoint_id
)
SELECT CURRENT_TIMESTAMP() insert_time
    , value raw
    , HASH(value) hash_raw
    --- In the rare event of an unparsable timestamp, the following COALESCE keeps the pipeline from failing.
    --- Compare event_time to TRY_TO_TIMESTAMP(raw:eventTime::STRING) to establish if the timestamp was parsed.
    , COALESCE(
        TRY_TO_TIMESTAMP(value:eventTime::STRING)::TIMESTAMP_LTZ(9),
        CURRENT_TIMESTAMP()
      ) event_time
    , value:awsRegion::STRING aws_region
    , value:eventID::STRING event_id
    , value:eventName::STRING event_name
    , value:eventSource::STRING event_source
    , value:eventType::STRING event_type
    , value:eventVersion::STRING event_version
    , value:recipientAccountId::STRING recipient_account_id
    , value:requestID::STRING request_id
    , value:requestParameters::VARIANT request_parameters
    , value:responseElements::VARIANT response_elements
    , value:sourceIPAddress::STRING source_ip_address
    , value:userAgent::STRING user_agent
    , value:userIdentity::VARIANT user_identity
    , value:userIdentity.type::STRING user_identity_type
    , value:userIdentity.principalId::STRING user_identity_principal_id
    , value:userIdentity.arn::STRING user_identity_arn
    , value:userIdentity.accountId::STRING user_identity_accountid
    , value:userIdentity.invokedBy::STRING user_identity_invokedby
    , value:userIdentity.accessKeyId::STRING user_identity_access_key_id
    , value:userIdentity.userName::STRING user_identity_username
    , TRY_CAST(value:userIdentity.sessionContext.attributes.mfaAuthenticated::STRING AS BOOLEAN) user_identity_session_context_attributes_mfa_authenticated
    , value:userIdentity.sessionContext.attributes.creationDate::STRING user_identity_session_context_attributes_creation_date
    , value:userIdentity.sessionContext.sessionIssuer.type::STRING user_identity_session_context_session_issuer_type
    , value:userIdentity.sessionContext.sessionIssuer.principalId::STRING user_identity_session_context_session_issuer_principal_id
    , value:userIdentity.sessionContext.sessionIssuer.arn::STRING user_identity_session_context_session_issuer_arn
    , value:userIdentity.sessionContext.sessionIssuer.accountId::STRING user_identity_session_context_session_issuer_account_id
    , value:userIdentity.sessionContext.sessionIssuer.userName::STRING user_identity_session_context_session_issuer_user_name
    , value:errorCode::STRING error_code
    , value:errorMessage::STRING error_message
    , value:additionalEventData::VARIANT additional_event_data
    , value:apiVersion::STRING api_version
    , value:readOnly::BOOLEAN read_only
    , value:resources::VARIANT resources
    , value:serviceEventDetails::STRING service_event_details
    , value:sharedEventId::STRING shared_event_id
    , value:vpcEndpointId::STRING vpc_endpoint_id
FROM data.{base_name}_STREAM, table(flatten(input => v:Records))
WHERE ARRAY_SIZE(v:Records) > 0
'''

    db.create_stream(
        name=f'data.{base_name}_STREAM', target=f'data.{base_name}_STAGING'
    )

    # IAM change takes 5-15 seconds to take effect
    sleep(5)
    db.retry(
        lambda: db.create_pipe(
            name=pipe,
            sql=f"COPY INTO data.{base_name}_STAGING(v) FROM @data.{base_name}_STAGE/",
            replace=True,
            autoingest=True,
        ),
        n=10,
        sleep_seconds_btw_retry=1,
    )

    db.create_task(
        name=f'data.{base_name}_TASK',
        schedule='1 minute',
        warehouse=WAREHOUSE,
        sql=cloudtrail_ingest_task,
    )

    pipe_description = next(db.fetch(f'DESC PIPE {pipe}'), None)
    if pipe_description is None:
        return {
            'newStage': 'error',
            'newMessage': f"{pipe} doesn't exist; please reach out to Snowflake Security for assistance.",
        }
    else:
        sqs_arn = pipe_description['notification_channel']

    return {
        'newStage': 'finalized',
        'newMessage': (
            f"Please add this SQS Queue ARN to the bucket event notification"
            f"channel for all object create events:\n\n  {sqs_arn}\n\n"
            f"To backfill the landing table with existing data, please run:\n\n  ALTER PIPE {pipe} REFRESH;\n\n"
        ),
    }
