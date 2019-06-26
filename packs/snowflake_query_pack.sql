CREATE OR REPLACE VIEW rules.snowflake_admin_role_grant_monitor_alert_query COPY GRANTS AS
SELECT
      OBJECT_CONSTRUCT('cloud', 'Snowflake', 'account', current_account()) AS environment
    , ARRAY_CONSTRUCT('snowflake') AS sources
    , REGEXP_SUBSTR(query_text, '\\s([^\\s]+)\\sto\\s',1,1,'ie') AS object
    , 'Snowflake - Admin role granted' AS alerttype
    , start_time AS event_time
    , CURRENT_TIMESTAMP() AS alert_time
    , 'A new grant was added ' || LOWER(REGEXP_SUBSTR(query_text, '\\s(to\\s[^\\s]+\\s[^\\s]+);?',1,1,'ie')) || ' by user ' || user_name || ' using role ' || role_name AS description
    , 'SnowAlert' AS detector
    , query_text AS event_data
    , 'Medium' AS severity
    , user_name AS actor
    , 'Granted Admin role' AS action
    , 'c77cf311de094a0ab9599917d6d0c644' AS query_id
    , 'snowflake_admin_role_grant_monitor_alert_query' AS query_name
FROM snowflake.account_usage.query_history
WHERE 1=1
  AND query_type='GRANT'
  AND execution_status='SUCCESS'
  AND (object ILIKE '%securityadmin%' OR object ILIKE '%accountadmin%')
;

GRANT SELECT ON VIEW rules.snowflake_admin_role_grant_monitor_alert_query TO ROLE snowalert;

CREATE OR REPLACE VIEW rules.snowflake_authorization_error_alert_query COPY GRANTS AS
SELECT
      OBJECT_CONSTRUCT('cloud', 'Snowflake', 'account', current_account()) AS environment
    , ARRAY_CONSTRUCT('snowflake') AS sources
    , 'Snowflake Query' AS object
    , 'Snowflake Access Control Error' AS title
    , START_TIME AS event_time
    , current_timestamp() AS alert_time
    , 'User ' || USER_NAME || ' received ' || ERROR_MESSAGE AS description
    , 'SnowAlert' AS detector
    , ERROR_MESSAGE AS event_data
    , USER_NAME AS actor
    , 'Received an authorization error' AS action
    , 'Low' AS severity
    , 'b0724d64b40d4506b7bc4e0caedd1442' AS query_id
    , 'snowflake_authorization_error_alert_query' AS query_name
from snowflake.account_usage.query_history
WHERE 1=1
  AND error_code in (1063, 3001, 3003, 3005, 3007, 3011, 3041)
;

GRANT SELECT ON VIEW rules.snowflake_authorization_error_alert_query TO ROLE snowalert;

CREATE OR REPLACE VIEW rules.snowflake_authentication_failure_alert_query COPY GRANTS AS
SELECT
      OBJECT_CONSTRUCT('cloud', 'Snowflake', 'account', current_account()) AS environment
    , ARRAY_CONSTRUCT('snowflake') AS sources
    , 'Snowflake' AS object
    , 'Snowflake Authentication Failure' AS title
    , event_timestamp AS event_time
    , CURRENT_TIMESTAMP() AS alert_time
    , 'User ' || USER_NAME || ' failed to authentication to Snowflake, from IP: ' || CLIENT_IP AS description
    , 'SnowAlert' AS detector
    , error_message AS event_data
    , user_name AS actor
    , 'failed to authenticate to Snowflake' AS action
    , 'Low' AS severity
    , 'c24675c89deb4e5ba6ecc57104447f90' AS query_id
    , 'snowflake_authentication_failure_alert_query' AS query_name
FROM snowflake.account_usage.login_history
WHERE 1=1
  AND IS_SUCCESS='NO'
;

GRANT SELECT ON VIEW rules.snowflake_authentication_failure_alert_query TO ROLE snowalert;
