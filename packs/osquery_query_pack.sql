CREATE OR REPLACE VIEW rules.missing_server_osquery_logs_violation_query COPY GRANTS
  COMMENT='Server not shipping osquery logs
  @id 2060a772e50e4a4598808cdb76d2a315'
AS
SELECT OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'deployment', map.deployment,
         'account', map.account_alias
       ) AS environment
     , instance_id AS object
     , 'Missing osquery agent logs' AS title
     , CURRENT_TIMESTAMP() AS alert_time
     , 'The affected server does not appear to be shipping osquery logs.' AS description
     , details AS event_data
     , 'SnowAlert' AS detector
     , 'medium' AS severity
     , '2060a772e50e4a4598808cdb76d2a315' AS query_id
FROM (
  SELECT snapshot_at
       , data:InstanceId::String AS instance_id
       , data:Tags::String AS tags
       , data:State:Name::String AS state
       , accountid AS account_id
       , data AS details
  FROM aws_inventory.snapshots.instances AS snap
  WHERE snap.snapshot_at > DATEADD(hour, -1, CURRENT_TIMESTAMP)
    AND tags NOT LIKE '%{"Key":"SomeKey","Value":"ServersToIgnore"}%'
    AND state NOT IN ('stopped', 'terminated')
) AS snap
LEFT JOIN (
  SELECT distinct instance_id::string AS instance_id
  FROM data.osquery_v
  WHERE osquery_v.event_time > DATEADD(hour, -1, CURRENT_TIMESTAMP)
) AS osquery
USING instance_id
JOIN aws_inventory.snapshots.aws_account_map
USING account_id
WHERE 1=1
  AND id IS NULL
;
