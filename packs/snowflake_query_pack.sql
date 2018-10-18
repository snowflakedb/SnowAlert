create or replace view snowalert.rules.snowflake_admin_role_grant_monitor_alert_query as
select
      object_construct('cloud', 'Snowflake', 'account', current_account()) as environment
    , array_construct('snowflake') as sources
    , regexp_substr(query_text, '\\s([^\\s]+)\\sto\\s',1,1,'ie') as object
    , 'Snowflake - Admin role granted' as alerttype
    , start_time as event_time
    , current_timestamp() as alert_time
    , 'A new grant was added ' || lower(regexp_substr(query_text, '\\s(to\\s[^\\s]+\\s[^\\s]+);?',1,1,'ie')) || ' by user ' || user_name || ' using role ' || role_name as description
    , 'SnowAlert' as detector
    , query_text as event_data
    , 'Medium' as severity
    , user_name as actor
    , 'Granted Admin role' as action
    , 'c77cf311de094a0ab9599917d6d0c644' as query_id
    , 'snowflake_admin_role_grant_monitor_alert_query' as query_name
  from snowflake.account_usage.query_history
  where 1=1
    and query_type = 'GRANT'
    and execution_status = 'SUCCESS'
    and (object ilike '%securityadmin%' or object ilike '%accountadmin%')
;

grant select on view snowalert.rules.snowflake_admin_role_grant_monitor_alert_query to role snowalert;

create or replace view snowalert.rules.snowflake_authorization_error_alert_query as
select
      object_construct('cloud', 'Snowflake', 'account', current_account()) as environment
    , array_construct('snowflake') as sources
    , 'Snowflake Query' as object
    , 'Snowflake Access Control Error' as title
    , START_TIME as event_time
    , current_timestamp() as alert_time
    , 'User ' || USER_NAME || ' received ' || ERROR_MESSAGE as description
    , 'SnowAlert' as detector
    , ERROR_MESSAGE as event_data
    , USER_NAME as actor
    , 'Received an authorization error' as action
    , 'Low' as severity
    , 'b0724d64b40d4506b7bc4e0caedd1442' as query_id
    , 'snowflake_authorization_error_alert_query' as query_name
  from snowflake.account_usage.query_history
  where 1=1
  and error_code in (1063, 3001, 3003, 3005, 3007, 3011, 3041)
;

grant select on view snowalert.rules.snowflake_authorization_error_alert_query to role snowalert;

create or replace view snowalert.rules.snowflake_authentication_failure_alert_query as
select
      object_construct('cloud', 'Snowflake', 'account', current_account()) as environment
    , array_construct('snowflake') as sources
    , 'Snowflake' as object
    , 'Snowflake Authentication Failure' as title
    , event_timestamp as event_time
    , current_timestamp() as alert_time
    , 'User ' || USER_NAME || ' failed to authentication to Snowflake, from IP: ' || CLIENT_IP as description
    , 'SnowAlert' as detector
    , ERROR_MESSAGE as event_data
    , user_name as actor
    , 'failed to authenticate to Snowflake' as action
    , 'Low' as severity
    , hash(event_time || EVENT_TYPE) as event_hash
    , current_database() as database
    , current_schema() as schema
    , 'snowflake_authentication_failure_alert_query' as event_def
  from snowflake.account_usage.login_history
  where 1=1
    and IS_SUCCESS = 'NO'
;

grant select on view snowalert.rules.snowflake_authentication_failure_alert_query to role snowalert;
