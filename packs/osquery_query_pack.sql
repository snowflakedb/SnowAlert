CREATE OR REPLACE VIEW snowalert.rules.SERVER_MISSING_OSQUERY_AGENT_VIOLATION_QUERY AS
SELECT OBJECT_CONSTRUCT('cloud', 'aws', 'deployment', MAP.DEPLOYMENT, 'account', MAP.ACCOUNT_ALIAS) AS environment
     , instances AS object
     , 'Server Missing Osquery Agent' AS title
     , current_timestamp() AS alert_time
     , 'The affected server does not appear to be shipping Osquery logs.' AS description
     , details AS event_data
     , 'SnowAlert' AS detector
     , 'medium' AS severity
     , '2060a772e50e4a4598808cdb76d2a315' AS query_id
     , 'SERVER_MISSING_OSQUERY_AGENT_VIOLATION_QUERY' AS query_name
FROM (
SELECT snapshot_at
, Data:InstanceId::String as instances
, Data:Tags::String as tags
, Data:State:Name::String as state
, accountid
, Data as details
FROM aws_inventory.snapshots.instances as snap
WHERE snap.snapshot_at > dateadd(hour,-1,current_timestamp)
AND tags not like '%{"Key":"SomeKey","Value":"ServersToIgnore"}%'
and state not in ('stopped', 'terminated')
) snap
LEFT JOIN
(
SELECT distinct instance_id::string as id
FROM snowalert.data.osquery_v
WHERE osquery_v.event_time > dateadd(hour,-1,current_timestamp)
) osquery
ON snap.instances = osquery.id
JOIN aws_inventory.snapshots.aws_account_map as map
ON accountid = map.account_id
WHERE id is null;