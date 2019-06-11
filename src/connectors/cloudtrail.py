"""This is a CloudTrail connector

it pipes in CloudTrail stuff...
"""

from runners.helpers import db
from runners.helpers.dbconfig import WAREHOUSE
from time import sleep

import json

CONNECTION_OPTIONS = [
    {'name': 'bucket_name', 'type': 'str', 'prefix': 's3://'},
    {'name': 'filter', 'type': 'str', 'default': 'AWSLogs/'},
    {'name': 'aws_role', 'type': 'str'}
]

FILE_FORMAT = """
    TYPE = "JSON",
    COMPRESSION = "AUTO",
    ENABLE_OCTAL = FALSE,
    ALLOW_DUPLICATE = FALSE,
    STRIP_OUTER_ARRAY = TRUE,
    STRIP_NULL_VALUES = FALSE,
    IGNORE_UTF8_ERRORS = FALSE,
    SKIP_BYTE_ORDER_MARK = TRUE
"""

CLOUDTRAIL_LANDING_TABLE_COLUMNS = [
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
    ('vpc_endpoint_id', 'STRING')
]


def connect(name, options):
    # Step one: create everything we can with the knowledge we have

    name = f'CLOUDTRAIL_{name}'.upper()

    bucket = options['bucket_name']
    prefix = options.get('filter', 'AWSLogs/')
    role = options['aws_role']

    comment = f"""
---
name: {name}
"""

    db.create_stage(name=name + '_STAGE', url=f's3://{bucket}', prefix=prefix, cloud='aws',
                    credentials=role, file_format=FILE_FORMAT)

    db.create_table(name=name + '_STAGING', cols=[('v', 'variant')])

    db.create_table(name=name + "_EVENTS_CONNECTION", cols=CLOUDTRAIL_LANDING_TABLE_COLUMNS, comment=comment)

    results = {}
    stage_data = db.fetch(f'DESC STAGE DATA.{name}_STAGE')
    for row in stage_data:
        if row['property'] == 'AWS_EXTERNAL_ID' or row['property'] == 'SNOWFLAKE_IAM_USER':
            results[row['property']] = row['property_value']

    aws_trust_relationship = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"{results['SNOWFLAKE_IAM_USER']}"
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": f"{results['AWS_EXTERNAL_ID']}"
                    }
                }
            }
        ]
    }

    output = {'title': 'Next Steps', 'body': f"""
Please apply this policy as a trust relationship for the role you specified, to allow Snowflake the ability to assume the role and read files from the designated S3 Bucket.\n\n
{json.dumps(aws_trust_relationship)}
"""}

    return {'newStage': 'created', 'newMessage': output['body']}


def finalize(name):
    name = f'CLOUDTRAIL_{name}'.upper()

    # Step two: Configure the remainder once the role is properly configured.
    cloudtrail_ingest_task = f"""
INSERT INTO DATA.{name}_EVENTS_CONNECTION (
  raw, hash_raw, event_time, aws_region, event_id, event_name, event_source, event_type, event_version,
  recipient_account_id, request_id, request_parameters, response_elements, source_ip_address,
  user_agent, user_identity, user_identity_type, user_identity_principal_id, user_identity_arn,
  user_identity_accountid, user_identity_invokedby, user_identity_access_key_id, user_identity_username,
  user_identity_session_context_attributes_mfa_authenticated, user_identity_session_context_attributes_creation_date,
  user_identity_session_context_session_issuer_type, user_identity_session_context_session_issuer_principal_id,
  user_identity_session_context_session_issuer_arn, user_identity_session_context_session_issuer_account_id,
  user_identity_session_context_session_issuer_user_name, error_code, error_message, additional_event_data,
  api_version, read_only, resources, service_event_details, shared_event_id, vpc_endpoint_id
)
SELECT value raw
    , HASH(value) hash_raw
    , value:eventTime::TIMESTAMP_LTZ(9) event_time
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
    , value:userIdentity.sessionContext.attributes.mfaAuthenticated::STRING user_identity_session_context_attributes_mfa_authenticated
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
    , value:sharedEventID::STRING shared_event_id
    , value:vpcEndpointID::STRING vpc_endpoint_id
FROM DATA.{name}_STREAM, table(flatten(input => v:Records))
WHERE ARRAY_SIZE(v:Records) > 0
"""

    db.create_stream(name=name + '_STREAM', target=name+'_STAGING')

    sleep(7)  # We need to make sure the IAM change has time to take effect.
    pipe_sql = f"COPY INTO DATA.{name}_STAGING(v) FROM @DATA.{name}_STAGE/"
    db.create_pipe(name=name + '_PIPE', sql=pipe_sql, replace=True, autoingest=True)

    db.create_task(name=name + '_TASK', schedule='1 minute',
                   warehouse=WAREHOUSE, sql=cloudtrail_ingest_task)


def test(name):
    yield db.fetch(f'ls @DATA.{name}_STAGE')
    yield db.fetch(f'DESC TABLE DATA.{name}_STAGING')
    yield db.fetch(f'DESC STREAM DATA.{name}_STREAM')
    yield db.fetch(f'DESC PIPE DATA.{name}_PIPE')
    yield db.fetch(f'DESC TABLE DATA.{name}_EVENTS_CONNECTION')
    yield db.fetch(f'DESC TASK DATA.{name}_TASK')

