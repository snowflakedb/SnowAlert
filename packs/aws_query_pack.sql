create or replace view security.snowalert_event_definitions.aws_audit_log_configuration_changes_v as
select
      recipient_account_id as affectedenv
    , RAW:requestParameters:name::string as affectedobject
    , 'CloudTrail' as affectedobjecttype
    , 'AWS Audit Log Configuration Changes' as alerttype
    , event_time as event_time
    , current_timestamp() as alert_time
    , 'User ' || USER_IDENTITY_SESSION_CONTEXT_SESSION_ISSUER_USER_NAME || ' performed ' || event_name || ' on trail ' || RAW:requestParameters:name::string as description
    , 'SnowAlert' as detector
    , RAW as eventdata
    , '2' as severity
    , hash(eventdata) as event_hash
    , current_database() as database
    , current_schema() as schema
    , 'aws_audit_log_configuration_changes_v' as event_def
    , 1 as version
  from security.prod.cloudtrail_v
  where 1=1
    and event_name in ('StopLogging', 'UpdateTrail', 'DeleteTrail')
;

grant select on view security.snowalert_event_definitions.aws_audit_log_configuration_changes_v to role snowalert;

create or replace view security.snowalert_event_definitions.aws_permission_modification_denied_v as
select
      recipient_account_id as affectedenv
    , USER_IDENTITY_ARN as affectedobject
    , 'CloudTrail' as affectedobjecttype
    , 'AWS Permission Modification Denied' as alerttype
    , event_time as event_time
    , current_timestamp() as alert_time
    , error_message as description
    , 'SnowAlert' as detector
    , RAW as eventdata
    , '3' as severity
    , hash(eventdata) as event_hash
    , current_database() as database
    , current_schema() as schema
    , 'aws_permission_modification_denied_v' as event_def
    , 1 as version
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

grant select on view security.snowalert_event_definitions.aws_permission_modification_denied_v to role snowalert; 

create or replace view security.snowalert_event_definitions.aws_root_account_activity_v as
select
      recipient_account_id as affectedenv
    , recipient_account_id as affectedobject
    , 'Cloudtrail' as affectedobjecttype
    , 'AWS Root Account Activity' as alerttype
    , event_time as event_time
    , current_timestamp() as alert_time
    , 'Root user performed ' || event_name || ' at account ' || recipient_account_id as description
    , 'SnowAlert' as detector
    , RAW as eventdata
    , '2' as severity
    , hash(eventdata) as event_hash
    , current_database() as database
    , current_schema() as schema
    , 'aws_root_account_activity_v' as event_def
    , 1 as version
  from security.prod.cloudtrail_v as cloudtrail
  where 1=1
    and cloudtrail.USER_IDENTITY_TYPE = 'Root'
    and cloudtrail.SOURCE_IP_ADDRESS <> 'support.amazonaws.com'
;

grant select on view security.snowalert_event_definitions.aws_root_account_activity_v to role snowalert; 

create or replace view security.snowalert_event_definitions.aws_internal_bucket_access_v as
select
      recipient_account_id as affectedenv
    , REQUEST_PARAMETERS['bucketName']::string as affectedobject
    , 'CloudTrail' as affectedobjecttype
    , 'Internal Bucket Accessed By External Account' as alerttype
    , event_time as event_time
    , 'User from external account ' || USER_IDENTITY['accountId']::string || ' performed ' || event_name || ' at non-public bucket ' || REQUEST_PARAMETERS['bucketName']::string as description
    , 'SnowAlert' as detector
    , RAW as eventdata
    , '1' as severity
    , hash(eventdata) as event_hash
    , current_database() as database
    , current_schema() as schema
    , 'aws_internal_bucket_access_v' as event_def
    , 1 as version
  from security.prod.cloudtrail_v as cloudtrail
  where 1=1
    and affectedobject not like '%public'
    and USER_IDENTITY['accountId'] not in
        (
            select distinct account_id from security.prod.aws_account_map
        )
;

grant select on view security.snowalert_event_definitions.aws_root_account_activity_v to role snowalert;
