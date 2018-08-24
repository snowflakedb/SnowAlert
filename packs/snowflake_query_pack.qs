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
and (granted_role ilike '%securityadmin%' or granted_role ilike '%accountadmin%');
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
  Description = ["User {} failed to authenticate to Snowflake, from IP: {}", 1.0, 3.0]
  Detector = ["SnowAlert"]
  EventData = ["{}", 4.0]
  GUID = "4a7537513fa042f29643444d528caf73"
  Query = <<QUERY
SELECT
current_account(),
USER_NAME,
event_timestamp,
CLIENT_IP,
ERROR_MESSAGE
from snowflake.account_usage.login_history
where event_timestamp > dateadd('hour', -1, current_timestamp()) 
and IS_SUCCESS = 'NO';
QUERY
  Severity = ["5"]
}

query_spec snowflake_multiple_authentication_failure {
  AffectedEnv = ["Snowflake Account: {}", 3.0]
  AffectedObject  = ["{}", 1.0]
  AffectedObjectType = ["User Account"]
  AlertType = ["Snowflake Multiple Authentication Failure"]
  EventTime = ["{}", 2.0]
  Description = ["User {} failed to authenticate to Snowflake {} times in the past hour", 0.0, 1.0]
  Detector = ["SnowAlert"]
  EventData = ["User {} failed to authenticate to Snowflake {} times in the past hour", 0.0, 1.0]
  GUID = "b8db853fc94547a9a1b40d3f49244478"
  Query = <<QUERY
select user_name, count(*) as number, current_timestamp(), current_account()
from snowflake.account_usage.login_history
where 1=1 and
datediff(hour, event_timestamp, current_timestamp()) < 24 AND
is_success = 'NO' and
user_name in (select distinct user_name from snowflake.account_usage.login_history) group by user_name
having count(*) >= 3
;
QUERY
  Severity = ["5"]
}