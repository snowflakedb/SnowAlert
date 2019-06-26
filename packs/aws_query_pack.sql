CREATE OR REPLACE VIEW rules.aws_audit_log_configuration_changes_alert_query COPY GRANTS
  COMMENT='Flags destructive CloudTrail actions for review
  @id cd4765a6e4e44eb3816799a50bc3dbf4'
AS
SELECT
      OBJECT_CONSTRUCT(
        'cloud', 'AWS',
        'deployment', cloudtrail.DEPLOYMENT,
        'account', cloudtrail.recipient_account_id
      ) AS environment
    , ARRAY_CONSTRUCT('cloudtrail') AS sources
    , raw:requestParameters:name::string AS object
    , 'AWS Audit Log Configuration Changes' AS title
    , event_time AS event_time
    , CURRENT_TIMESTAMP() AS alert_time
    , (
        'User ' || user_identity_session_context_session_issuer_user_name ||
        ' performed ' || event_name ||
        ' on trail ' || raw:requestParameters:name::string
      ) AS description
    , user_identity_session_context_session_issuer_user_name AS actor
    , event_name AS action
    , 'SnowAlert' AS detector
    , raw AS event_data
    , 'high' AS severity
    , 'cd4765a6e4e44eb3816799a50bc3dbf4' AS query_id
FROM data.cloudtrail_v
WHERE 1=1
  AND event_name IN ('StopLogging', 'UpdateTrail', 'DeleteTrail')
;

GRANT SELECT ON view rules.aws_audit_log_configuration_changes_alert_query TO ROLE snowalert;

CREATE OR REPLACE VIEW rules.aws_permission_modification_denied_alert_query COPY GRANTS
  COMMENT='Access Denied errors on administrative events
  @id dad7800f08ba4789a47d6d519be42886'
AS
SELECT
      OBJECT_CONSTRUCT(
        'cloud', 'AWS',
        'deployment', cloudtrail.deployment,
        'account', cloudtrail.recipient_account_id
      ) AS environment
    , ARRAY_CONSTRUCT('cloudtrail') AS sources
    , user_identity_arn AS object
    , 'AWS Permission Modification Denied' AS title
    , event_time AS event_time
    , CURRENT_TIMESTAMP() AS alert_time
    , error_message AS description
    , 'SnowAlert' AS detector
    , raw AS event_data
    , user_identity_arn AS actor
    , cloudtrail.event_name AS action
    , 'medium' AS severity
    , 'dad7800f08ba4789a47d6d519be42886' AS query_id
FROM data.cloudtrail_v
WHERE 1=1
  AND error_code = 'AccessDenied'
  AND event_name IN (
      'AddUserToGroup',
      'AttachGroupPolicy',
      'AttachRolePolicy',
      'AttachUserPolicy',
      'CreateAccessKey',
      'CreateLoginProfile',
      'CreatePolicy',
      'CreateRole',
      'CreateUser',
      'CreateVirtualMFADevice',
      'DeactivateMFADevice',
      'DeleteAccessKey',
      'DeleteGroup',
      'DeleteGroupPolicy',
      'DeletePolicy',
      'DeleteRole',
      'DeleteRolePolicy',
      'DeleteServerCertificate',
      'DeleteUser',
      'DeleteUserPolicy',
      'DeleteVirtualMFADevice',
      'DetachGroupPolicy'
  )
;

GRANT SELECT ON view rules.aws_permission_modification_denied_alert_query TO ROLE snowalert;

CREATE OR REPLACE VIEW rules.aws_root_account_activity_alert_query
  COMMENT='AWS Root account activity
  @id 2337ac7e963f4ef89252834ae877258f'
AS
SELECT OBJECT_CONSTRUCT(
         'cloud', 'AWS',
         'deployment', cloudtrail.DEPLOYMENT,
         'account', cloudtrail.recipient_account_id
       ) AS environment
     , ARRAY_CONSTRUCT('cloudtrail') AS sources
     , recipient_account_id AS object
     , 'AWS Root Account Activity' AS title
     , event_time AS event_time
     , CURRENT_TIMESTAMP() AS alert_time
     , 'Root user performed ' || event_name || ' at account ' || recipient_account_id AS description
     , 'SnowAlert' AS detector
     , raw AS event_data
     , 'High' AS severity
     , 'Root' AS actor
     , event_name AS action
     , '2337ac7e963f4ef89252834ae877258f' AS query_id
FROM data.cloudtrail_v AS cloudtrail
WHERE 1=1
  AND cloudtrail.USER_IDENTITY_TYPE = 'Root'
  AND cloudtrail.SOURCE_IP_ADDRESS <> 'support.amazonaws.com'
;

GRANT SELECT ON view rules.aws_root_account_activity_alert_query TO ROLE snowalert;

CREATE OR REPLACE VIEW rules.aws_internal_bucket_access_alert_query COPY GRANTS
  COMMENT='AWS Internal Bucket Access by external account
  @id 1fda47b046ac4030a7cc7de536941e8a'
AS
SELECT
      OBJECT_CONSTRUCT(
        'cloud', 'AWS',
        'deployment', cloudtrail.deployment,
        'account', cloudtrail.recipient_account_id
      ) AS environment
    , ARRAY_CONSTRUCT('cloudtrail') AS sources
    , request_parameters['bucketName']::string AS object
    , 'Internal Bucket Accessed By External Account' AS title
    , event_time AS event_time
    , (
        'User from external account ' || USER_IDENTITY['accountId']::string ||
        ' performed ' || event_name ||
        ' at non-public bucket ' || REQUEST_PARAMETERS['bucketName']::string
      ) AS description
    , 'SnowAlert' AS detector
    , raw AS event_data
    , user_identity['accountId']::string AS actor
    , event_name AS action
    , 'critical' AS severity
    , '1fda47b046ac4030a7cc7de536941e8a' AS query_id
FROM data.cloudtrail_v AS cloudtrail
WHERE 1=1
  AND affectedobject NOT LIKE '%public'
  AND user_identity['accountId'] NOT IN (
    SELECT DISTINCT account_id FROM prod.aws_account_map
  )
;

GRANT SELECT ON view rules.aws_root_account_activity_alert_query TO ROLE snowalert;
