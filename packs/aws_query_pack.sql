create or replace view security.snowalert_event_definitions.aws_audit_log_configuration_changes_query as
select
      object_construct('cloud', 'AWS', 'deployment', cloudtrail.DEPLOYMENT, 'account', cloudtrail.recipient_account_id) as environment
    , array_construct('cloudtrail') as sources,
    , RAW:requestParameters:name::string as object,
    , 'AWS Audit Log Configuration Changes' as title,
    , event_time as event_time
    , current_timestamp() as alert_time
    , 'User ' || USER_IDENTITY_SESSION_CONTEXT_SESSION_ISSUER_USER_NAME || ' performed ' || event_name || ' on trail ' || RAW:requestParameters:name::string as description
    USER_IDENTITY_SESSION_CONTEXT_SESSION_ISSUER_USER_NAME as actor,
    event_name as action,
    , 'SnowAlert' as detector
    , RAW as event_data
    , 'High' as severity
    , 'cd4765a6e4e44eb3816799a50bc3dbf4' as event_hash
    , 'aws_audit_log_configuration_changes_v' as query_name
  from security.prod.cloudtrail_v
  where 1=1
    and event_name in ('StopLogging', 'UpdateTrail', 'DeleteTrail')
;

grant select on view security.snowalert_event_definitions.aws_audit_log_configuration_changes_query to role snowalert;

create or replace view security.snowalert_event_definitions.aws_permission_modification_denied_query as
select
      object_construct('cloud', 'AWS', 'deployment', cloudtrail.DEPLOYMENT, 'account', cloudtrail.recipient_account_id) as environment
    , array_construct('cloudtrail') as sources
    , USER_IDENTITY_ARN as object
    , 'AWS Permission Modification Denied' as title
    , event_time as event_time
    , current_timestamp() as alert_time
    , error_message as description
    , 'SnowAlert' as detector
    , RAW as event_data
    , user_identity_arn as actor
    , cloudtrail.event_name as action
    , 'Medium' as severity
    , 'dad7800f08ba4789a47d6d519be42886' as query_id
    , 'aws_permission_modification_denied_v' as query_name
  from security.prod.cloudtrail_v as cloudtrail
  where 1=1
    AND cloudtrail.error_code = 'AccessDenied'
    AND (cloudtrail.EVENT_NAME = 'AddUserToGroup'
      OR cloudtrail.EVENT_NAME = 'AttachGroupPolicy'
      OR cloudtrail.EVENT_NAME = 'AttachRolePolicy'
      OR cloudtrail.EVENT_NAME = 'AttachUserPolicy'
      OR cloudtrail.EVENT_NAME = 'CreateAccessKey'
      OR cloudtrail.EVENT_NAME = 'CreateLoginProfile'
      OR cloudtrail.EVENT_NAME = 'CreatePolicy'
      OR cloudtrail.EVENT_NAME = 'CreateRole'
      OR cloudtrail.EVENT_NAME = 'CreateUser'
      OR cloudtrail.EVENT_NAME = 'CreateVirtualMFADevice'
      OR cloudtrail.EVENT_NAME = 'DeactivateMFADevice'
      OR cloudtrail.EVENT_NAME = 'DeleteAccessKey'
      OR cloudtrail.EVENT_NAME = 'DeleteGroup'
      OR cloudtrail.EVENT_NAME = 'DeleteGroupPolicy'
      OR cloudtrail.EVENT_NAME = 'DeletePolicy'
      OR cloudtrail.EVENT_NAME = 'DeleteRole'
      OR cloudtrail.EVENT_NAME = 'DeleteRolePolicy'
      OR cloudtrail.EVENT_NAME = 'DeleteServerCertificate'
      OR cloudtrail.EVENT_NAME = 'DeleteUser'
      OR cloudtrail.EVENT_NAME = 'DeleteUserPolicy'
      OR cloudtrail.EVENT_NAME = 'DeleteVirtualMFADevice'
      OR cloudtrail.EVENT_NAME = 'DetachGroupPolicy')
;

grant select on view security.snowalert_event_definitions.aws_permission_modification_denied_query to role snowalert;

create or replace view security.snowalert_event_definitions.aws_root_account_activity_query as
select
      object_construct('cloud', 'AWS', 'deployment', cloudtrail.DEPLOYMENT, 'account', cloudtrail.recipient_account_id) as environment
    , array_construct('cloudtrail') as sources
    , recipient_account_id as object
    , 'AWS Root Account Activity' as title
    , event_time as event_time
    , current_timestamp() as alert_time
    , 'Root user performed ' || event_name || ' at account ' || recipient_account_id as description
    , 'SnowAlert' as detector
    , RAW as event_data
    , 'High' as severity
    , 'Root' as actor
    , event_name as action
    , '2337ac7e963f4ef89252834ae877258f' as query_id
    , 'aws_root_account_activity_v' as query_name
  from security.prod.cloudtrail_v as cloudtrail
  where 1=1
    and cloudtrail.USER_IDENTITY_TYPE = 'Root'
    and cloudtrail.SOURCE_IP_ADDRESS <> 'support.amazonaws.com'
;

grant select on view security.snowalert_event_definitions.aws_root_account_activity_query to role snowalert;

create or replace view security.snowalert_event_definitions.aws_internal_bucket_access_query as
select
      object_construct('cloud', 'AWS', 'deployment', cloudtrail.DEPLOYMENT, 'account', cloudtrail.recipient_account_id) as environment
    , array_construct('cloudtrail') as sources
    , REQUEST_PARAMETERS['bucketName']::string as object
    , 'Internal Bucket Accessed By External Account' as title
    , event_time as event_time
    , 'User from external account ' || USER_IDENTITY['accountId']::string || ' performed ' || event_name || ' at non-public bucket ' || REQUEST_PARAMETERS['bucketName']::string as description
    , 'SnowAlert' as detector
    , RAW as event_data
    , USER_IDENTITY['accountId']::string as actor
    , event_name as action
    , 'Critical' as severity
    , '1fda47b046ac4030a7cc7de536941e8a' as query_id
    , 'aws_internal_bucket_access_v' as query_name
  from security.prod.cloudtrail_v as cloudtrail
  where 1=1
    and affectedobject not like '%public'
    and USER_IDENTITY['accountId'] not in
        (
            select distinct account_id from security.prod.aws_account_map
        )
;

grant select on view security.snowalert_event_definitions.aws_root_account_activity_query to role snowalert;
