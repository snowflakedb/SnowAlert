-- ---
-- params:
-- - name: connection_name
--   default: ""
-- - name: bucket
-- - name: iam_role
--   required: true
-- - name: integration_name
--   required: true
-- - name: warehouse
-- varmap:
--   base_name : AWS_CLOUDTRAIL_{connection_name}_EVENTS
--   stage : data.{base_name}_STAGE
--   staging_table : data.{base_name}_STAGING
--   landing_table : data.{base_name}_CONNECTION
--   pipe : data.{base_name}_PIPE
--   task : data.{base_name}_TASK
--   stream : data.{base_name}_STREAM
--   storage_integration : data.{base_name}_STORAGE_INTEGRATION


CREATE SEQUENCE IF NOT EXISTS landing_table_seq START 1 INCREMENT 1;

CREATE TABLE IF NOT EXISTS {landing_table} (
    insert_id NUMBER DEFAULT landing_table_seq.nextval,
    insert_time TIMESTAMP_LTZ(9),
    raw VARIANT,
    hash_raw NUMBER,
    event_time TIMESTAMP_LTZ(9),
    aws_region STRING,
    event_id STRING,
    event_name STRING,
    event_source STRING,
    event_type STRING,
    event_version STRING,
    recipient_account_id STRING,
    request_id STRING,
    request_parameters VARIANT,
    response_elements VARIANT,
    source_ip_address STRING,
    user_agent STRING,
    user_identity VARIANT,
    user_identity_type STRING,
    user_identity_principal_id STRING,
    user_identity_arn STRING,
    user_identity_accountid STRING,
    user_identity_invokedby STRING,
    user_identity_access_key_id STRING,
    user_identity_username STRING,
    user_identity_session_context_attributes_mfa_authenticated BOOLEAN,
    user_identity_session_context_attributes_creation_date STRING,
    user_identity_session_context_session_issuer_type STRING,
    user_identity_session_context_session_issuer_principal_id STRING,
    user_identity_session_context_session_issuer_arn STRING,
    user_identity_session_context_session_issuer_account_id STRING,
    user_identity_session_context_session_issuer_user_name STRING,
    error_code STRING,
    error_message STRING,
    additional_event_data VARIANT,
    api_version STRING,
    read_only BOOLEAN,
    resources VARIANT,
    service_event_details STRING,
    shared_event_id STRING,
    vpc_endpoint_id STRING
);

-- Stage
CREATE OR REPLACE STAGE {stage}
    URL = ('s3://{bucket}')
    CREDENTIALS = {role}
    FILE_FORMAT = (
        TYPE=JSON
    )
;

DESC STAGE {stage};



--Storage Integration
CREATE STORAGE INTEGRATION {storage_integration}
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = {iam_role}
  STORAGE_ALLOWED_LOCATIONS = ('s3://{bucket}/')
;

--Provides information to write the role policies in AWS
DESC INTEGRATION {integration_name};

--Staging table
CREATE TABLE IF NOT EXISTS {staging_table} (
    recorded_at TIMESTAMP_LTZ(9),
    v VARIANT
);


--Stream
CREATE OR REPLACE STREAM {stream}
    ON {staging_table}
;



--Pipe
CREATE OR REPLACE PIPE {pipe}
    AUTO_INGEST = TRUE
    AS
    COPY INTO {staging_table}(CURRENT_TIMESTAMP(),v)
    FROM @{stage}/
;


--Task
CREATE TASK {task}
    WAREHOUSE = {warehouse}
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('{stream}')
    AS
    INSERT INTO {landing_table} (
        insert_time,
        raw,
        hash_raw,
        event_time,
        aws_region,
        event_id,
        event_name,
        event_source,
        event_type,
        event_version,
        recipient_account_id,
        request_id,
        request_parameters,
        response_elements,
        source_ip_address,
        user_agent,
        user_identity,
        user_identity_type,
        user_identity_principal_id,
        user_identity_arn,
        user_identity_accountid,
        user_identity_invokedby,
        user_identity_access_key_id,
        user_identity_username,
        user_identity_session_context_attributes_mfa_authenticated,
        user_identity_session_context_attributes_creation_date,
        user_identity_session_context_session_issuer_type,
        user_identity_session_context_session_issuer_principal_id,
        user_identity_session_context_session_issuer_arn,
        user_identity_session_context_session_issuer_account_id,
        user_identity_session_context_session_issuer_user_name,
        error_code,
        error_message,
        additional_event_data,
        api_version,
        read_only,
        resources,
        service_event_details,
        shared_event_id,
        vpc_endpoint_id
    )
    SELECT CURRENT_TIMESTAMP() insert_time
    , value raw
    , HASH(value) hash_raw
    --- In the rare event of an unparsable timestamp, the following COALESCE keeps the pipeline from failing.
    --- Compare event_time to TRY_TO_TIMESTAMP(raw:eventTime::STRING) to establish if the timestamp was parsed.
    , COALESCE(
        TRY_TO_TIMESTAMP_LTZ(value:eventTime::STRING),
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
;
