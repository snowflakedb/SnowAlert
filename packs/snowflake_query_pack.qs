query_spec snowflake_admin_role_grant_monitor {
  AffectedEnv = ["{}", 0.0]
  AffectedObject  = ["{}", 1.0]
  AffectedObjectType = ["{}", 2.0]
  AlertType = ["Snowflake - Admin role granted"]
  EventTime = ["{}", 3.0]
  Description = ["A new grant was added {} by user {} using role {}", 4.0, 5.0, 6.0]
  Detector = ["SnowAlert"]
  EventData = ["{}", 7.0]
  GUID = "476301ebdf5a4462834abc67b89ae7a3"
  Query = <<QUERY
SELECT
current_account(),
regexp_substr(query_text, '\\s([^\\s]+)\\sto\\s',1,1,'ie') as granted_role,
regexp_substr(query_text, '([^\\s]+)\\s[^\\s]+\\sto\\s', 1, 1, 'ie'),
start_time,
lower(regexp_substr(query_text, '\\s(to\\s[^\\s]+\\s[^\\s]+);?',1,1,'ie')),
user_name,
role_name,
query_text
from snowflake.account_usage.query_history
where start_time > dateadd('hour', -1, current_timestamp())
and query_type = 'GRANT'
and execution_status = 'SUCCESS'
and (granted_role ilike '%securityadmin%' or affectedobject ilike '%accountadmin%');
QUERY
  Severity = ["3"]
}

query_spec snowflake_authorization_error {
  AffectedEnv = ["{}", 0.0]
  AffectedObject  = ["Snowflake Query"]
  AffectedObjectType = ["Snowflake"]
  AlertType = ["Snowflake Access Control Error"]
  EventTime = ["{}", 2.0]
  Description = ["User {} received {}", 4.0, 5.0]
  Detector = ["SnowAlert"]
  EventData = ["{}", 5.0]
  GUID = "70a9becd13354958b670ba23bdd0331b"
  Query = <<QUERY
SELECT
current_account(),
START_TIME,
USER_NAME,
ERROR_MESSAGE
from snowflake.account_usage.query_history where
start_time > dateadd('hour',-1,CURRENT_TIMESTAMP())
and error_code in (1063, 3001, 3003, 3005, 3007, 3011, 3041);
QUERY
  Severity = ["5"]
}

query_spec snowflake_authentication_failure {
  AffectedEnv = ["{}", 0.0]
  AffectedObject  = ["{}", 1.0]
  AffectedObjectType = ["Snowflake"]
  AlertType = ["Snowflake Authentication Failure"]
  EventTime = ["{}", 2.0]
  Description = ["User {} failed to authenticate to Snowflake, from IP: {}", 3.0, 4.0]
  Detector = ["SnowAlert"]
  EventData = ["{}", 5.0]
  GUID = "4a7537513fa042f29643444d528caf73"
  Query = <<QUERY
SELECT
current_account(),
USER_NAME,
event_timestamp,
USER_NAME,
CLIENT_IP,
ERROR_MESSAGE
from snowflake.account_usage.login_history
where event_timestamp > dateadd('hour', -1, current_timestamp()) 
and IS_SUCCESS = 'NO';
QUERY
  Severity = ["5"]
}