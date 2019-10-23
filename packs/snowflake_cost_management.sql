-- Automatic Clustering Spend
-- this query finds tables where the automatic clustering spend has gone over 10 credits in the past 5 hours
WITH table_spend AS (
  SELECT
    table_id,
    table_name,
    SUM(credits_used) AS credits
  FROM snowflake.account_usage.automatic_clustering_history
  WHERE DATEDIFF(HOUR, end_time, CURRENT_TIMESTAMP) < 5
  GROUP BY 1, 2
  ORDER BY 3 DESC
)
SELECT *
FROM table_spend
WHERE credits > 10
; 

-- Materialized View Spend
-- this query finds tables where the materialized view spend has gone over 10 credits in the past 5 hours
WITH table_spend AS (
  SELECT
    table_id,
    table_name,
    SUM(credits_used) AS credits
  FROM snowflake.account_usage.materialized_view_refresh_history
  WHERE DATEDIFF(HOUR, end_time, CURRENT_TIMESTAMP) < 5
  GROUP BY 1, 2
  ORDER BY 3 DESC)
SELECT * FROM table_spend
WHERE credits > 10
;

-- Snowpipe spend
-- this query finds tables where the snowpipe spend has gone over 10 credits in the past 12 hours
WITH pipe_spend AS (
  SELECT
    pipe_id,
    pipe_name,
    SUM(credits_used) AS credits_used
  FROM snowflake.account_usage.pipe_usage_history
  WHERE DATEDIFF(HOUR, end_time, CURRENT_TIMESTAMP) < 12
  GROUP BY 1, 2
  ORDER BY 3 DESC
)
SELECT *
FROM pipe_spend
WHERE credits_used > 10
;

-- Warehouse Spending Spike
-- this query compares the last day credit spend vs. the last 28 day average for the account
WITH average_use AS (
  SELECT
    warehouse_id,
    warehouse_name,
    SUM(credits_used) AS total_credits_used,
    SUM(credits_used) / 28 AS avg_credits_used
  FROM snowflake.account_usage.warehouse_metering_history
  WHERE DATEDIFF(DAY, start_time, CURRENT_TIMESTAMP) < 28
  GROUP BY 1, 2
)
SELECT
  w.warehouse_id,
  w.warehouse_name,
  SUM(w.credits_used) AS ld_credits_used,
  a.avg_credits_used
FROM snowflake.account_usage.warehouse_metering_history w
JOIN average_use a
ON w.warehouse_id = a.warehouse_id
WHERE DATEDIFF(DAY, start_time, CURRENT_TIMESTAMP) < 2
GROUP BY 1, 2, 4
HAVING ld_credits_used > (a.avg_credits_used * 2)
;
