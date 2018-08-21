{
  "AffectedEnv": ["Snowflake"],
  "AffectedObject": ["{}", 1.0],
  "AffectedObjectType": ["Snowflake user"],
  "AlertType": ["Snowflake User Login Without MFA"],
  "Description": ["User {} logged in from {} using {} without MFA", 1.0, 2.0, 3.0],
  "Detector": ["SnowAlert"],
  "EventData": ["User {} logged in from {} using {} without MFA", 1.0, 2.0, 3.0],
  "EventTime": ["{}",0.0],
  "GUID": "60c99650bb2943cb844fa2cb6d58f448",
  "Query": "select event_timestamp, user_name, client_ip, reported_client_type from table(snowflake_sample_data.information_schema.login_history()) where second_authentication_factor is null and is_success = \'YES\' and (datediff(minute, event_timestamp, current_timestamp()) < 60);",
  "QueryName": "Sample Query",
  "Severity": ["2"]
}