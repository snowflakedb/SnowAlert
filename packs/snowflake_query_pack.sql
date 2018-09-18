create or replace view security.snowalert_event_definitions.snowflake_admin_role_grant_monitor_v as
select
      current_account() as affectedenv
    , regexp_substr(query_text, '\\s([^\\s]+)\\sto\\s',1,1,'ie') as affectedobject
    , regexp_substr(query_text, '([^\\s]+)\\s[^\\s]+\\sto\\s', 1, 1, 'ie') as affectedobjecttype
    , 'Snowflake - Admin role granted' as alerttype
    , start_time as event_time
    , current_timestamp() as alert_time
    , 'A new grant was added ' || lower(regexp_substr(query_text, '\\s(to\\s[^\\s]+\\s[^\\s]+);?',1,1,'ie')) || ' by user ' || user_name || ' using role ' || role_name as description
    , 'SnowAlert' as detector
    , query_text as eventdata
    , '3' as severity
    , hash(description || event_data) as event_hash
    , current_database() as database
    , current_schema() as schema
    , 'snowflake_admin_role_grant_monitor_v' as event_def
    , 1 as version
  from snowflake.account_usage.query_history 
  where 1=1 
    and query_type = 'GRANT'
    and execution_status = 'SUCCESS'
    and (granted_role ilike '%securityadmin%' or granted_role ilike '%accountadmin%')
;

grant select on view security.snowalert_event_definitions.snowflake_admin_role_grant_monitor_v to role snowalert; 

create or replace view security.snowalert_event_definitions.snowflake_authorization_error_v as
select
      current_account() as affectedenv
    , 'Snowflake Query' as affectedobject
    , 'Snowflake' as affectedobjecttype
    , 'Snowflake Access Control Error' as alerttype
    , START_TIME as event_time
    , current_timestamp() as alert_time
    , 'User ' || USER_NAME || ' received ' || ERROR_MESSAGE as description
    , 'SnowAlert' as detector
    , ERROR_MESSAGE as eventdata
    , '5' as severity
    , hash(description || event_data) as event_hash
    , current_database() as database
    , current_schema() as schema
    , 'snowflake_authorization_error_v' as event_def
    , 1 as version
  from snowflake.account_usage.query_history 
  where 1=1
  and error_code in (1063, 3001, 3003, 3005, 3007, 3011, 3041)
;

grant select on view security.snowalert_event_definitions.snowflake_authorization_error_v to role snowalert;

create or replace view security.snowalert_event_definitions.snowflake_authentication_failure_v as
select
      current_account as affectedenv
    , USER_NAME as affectedobject
    , 'Snowflake' as affectedobjecttype
    , 'Snowflake Authentication Failure' as alerttype
    , event_timestamp as event_time
    , current_timestamp() as alert_time
    , 'User ' || USER_NAME || ' failed to authentication to Snowflake, from IP: ' || CLIENT_IP as description
    , 'SnowAlert' as detector
    , ERROR_MESSAGE as eventdata
    , '5' as severity
    , hash(ERROR_CODE || EVENT_TYPE) as event_hash
    , current_database() as database
    , current_schema() as schema
    , 'snowflake_authentication_failure_v' as event_def
    , 1 as version
  from snowflake.account_usage.login_history
  where 1=1
    and IS_SUCCESS = 'NO'
;

grant select on view security.snowalert_event_definitions.snowflake_authentication_failure_v to role snowalert;
