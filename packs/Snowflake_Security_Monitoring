-- Too many bad logins by a single IP
WITH ip_login_fails AS (
  SELECT 
    client_ip,
    reported_client_type,
    ARRAY_AGG(DISTINCT error_code),
    ARRAY_AGG(DISTINCT error_message),
    COUNT(event_id) AS counts
  FROM account_usage.login_history
  WHERE DATEDIFF(HOUR,  event_timestamp, CURRENT_TIMESTAMP) < 24
    AND error_code IS NOT NULL
  GROUP BY client_ip, reported_client_type
)

SELECT *
FROM ip_login_fails
WHERE counts > 5
;

-- Too many bad logins by a single User
WITH user_login_fails AS (
  SELECT 
    user_name,
    reported_client_type,
    ARRAY_AGG(DISTINCT error_code),
    ARRAY_AGG(DISTINCT error_message),
    COUNT(event_id) AS counts
  FROM account_usage.login_history
  WHERE DATEDIFF(HOUR, event_timestamp, CURRENT_TIMESTAMP) < 12
    AND error_code IS NOT NULL
  GROUP BY user_name, reported_client_type
)

SELECT *
FROM user_login_fails
WHERE counts > 3
; 

-- Modifications to sensitive roles
SELECT
  query_id,
  query_text,
  user_name,
  role_name,
  start_time
FROM snowflake.account_usage.query_history
WHERE QUERY_TYPE = 'ALTER_USER'
and QUERY_TEXT ilike '%ADMIN%'
; 

-- Password changes
SELECT
  query_id,
  query_text,
  user_name,
  role_name,
  start_time
FROM snowflake.account_usage.query_history
WHERE query_type = 'ALTER_USER'
  AND query_text ILIKE '%password%'
; 

-- New Users Created
SELECT
  query_id,
  query_text,
  user_name,
  role_name,
  start_time
FROM snowflake.account_usage.query_history
WHERE query_type = 'CREATE_USER'
; 

-- User not using Multi Factor Authentication
SELECT
  event_timestamp,
  user_name,
  client_IP,
  reported_client_type,
  first_authentication_factor,
  second_authentication_factor
FROM snowflake.account_usage.login_history
WHERE second_authentication_factor IS NULL
  AND (
    reported_client_type = 'SNOWFLAKE_UI'
    OR reported_client_type = 'OTHER'
  )
; 

-- Suspicious increase in queries by a user
WITH average_queries AS (
  SELECT
    user_name,
    COUNT(query_id) AS total_queries,
    COUNT(query_id) / 28 AS avg_queries
  FROM snowflake.account_usage.query_history
  WHERE DATEDIFF(DAY, start_time, CURRENT_TIMESTAMP) < 28
  GROUP BY 1
)
SELECT
  w.user_name,
  SUM(w.query_id) AS ld_queries,
  a.avg_queries
FROM snowflake.account_usage.query_history w
JOIN average_queries a
ON w.user_name = a.user_name
WHERE DATEDIFF(DAY, start_time, CURRENT_TIMESTAMP) < 2
GROUP BY 1,3
HAVING ld_queries > (a.avg_queries * 10)
;
