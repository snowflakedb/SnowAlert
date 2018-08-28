query_spec aws_audit_log_configuration_changes {
  AffectedEnv = ["{}", 1.0]
  AffectedObject  = ["{}", 4.0]
  AffectedObjectType = ["CloudTrail"]
  AlertType = ["AWS Audit Log Configuration Changes"]
  EventTime = ["{}", 0.0]
  Description = ["User {} performed {} on trail {}.", 2.0, 3.0, 4.0]
  Detector = ["SnowAlert"]
  EventData = ["{}", 5.0]
  GUID = "c903d344b9c643f9b94421062e699ca8"
  Query = <<QUERY
select event_time
    , recipient_account_id
    , USER_IDENTITY_SESSION_CONTEXT_SESSION_ISSUER_USER_NAME as user_name
    , event_name
    , RAW:requestParameters:name::string as trail_name
    , RAW
FROM security.prod.cloudtrail_v
where event_time > dateadd('hour', -1, current_timestamp())
and event_name in (
'StopLogging', 'UpdateTrail', 'DeleteTrail');
QUERY
  Severity = ["3"]
}

query_spec aws_permission_modification_denied {
  AffectedEnv = ["{}", 1.0]
  AffectedObject  = ["{}", 2.0]
  AffectedObjectType = ["CloudTrail"]
  AlertType = ["AWS Permission Modification Denied"]
  EventTime = ["{}", 0.0]
  Description = ["{}", 3.0]
  Detector = ["SnowAlert"]
  EventData = ["{}", 4.0]
  GUID = "66f0f51c7cf6453390a8255bda9f07b2"
  Query = <<QUERY
select event_time
    , recipient_account_id as account_id
    , USER_IDENTITY_ARN as user_name
    , error_message
    , RAW
FROM security.prod.cloudtrail_v as cloudtrail
where event_time > dateadd('hour', -1, current_timestamp())
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
    OR cloudtrail.EVENT_NAME = 'DetachGroupPolicy');
QUERY
  Severity = ["3"]
}

query_spec aws_root_account_activity {
  AffectedEnv = ["{}", 1.0]
  AffectedObject  = ["{}", 1.0]
  AffectedObjectType = ["CloudTrail"]
  AlertType = ["AWS Root Account Activity"]
  EventTime = ["{}", 0.0]
  Description = ["Root user performed {} at account {}.", 2.0, 1.0]
  Detector = ["SnowAlert"]
  EventData = ["{}", 3.0]
  GUID = "8987e0b1ad5e410a832085e696d1f636"
  Query = <<QUERY
select event_time
    , recipient_account_id
    , event_name
    , RAW
FROM security.prod.cloudtrail_v as cloudtrail
where event_time > dateadd('hour', -1, current_timestamp())
    AND cloudtrail.USER_IDENTITY_TYPE = 'Root'
    AND cloudtrail.SOURCE_IP_ADDRESS <> 'support.amazonaws.com';
QUERY
  Severity = ["3"]
}



fc55a60ff0fa46c69109c8a829a9fa1f
