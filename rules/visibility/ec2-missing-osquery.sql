CREATE OR REPLACE VIEW rules.SERVER_MISSING_OSQUERY_AGENT_VIOLATION_QUERY COPY GRANTS
  COMMENT='Violation rule for identifying visibility gaps where a server is missing the osquery agent
  @id 2060a772e50e4a4598808cdb76d2a315
  @tags servers, osquery, visibility gap'
AS
SELECT OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'deployment', MAP.DEPLOYMENT,
         'account', MAP.ACCOUNT_ALIAS,
'fix_parameters', OBJECT_CONSTRUCT('ip_address', details:"PrivateIpAddress", 'account_name', MAP.account_alias, 'account_id', account_id, 'instance_id', details:"InstanceId", 'deployment', deployment)
       ) AS environment
     , instances AS object
     , 'AWS EC2 instance expected osquery data missing' AS title
     , CURRENT_TIMESTAMP() AS alert_time
     , 'The affected server does not appear to be shipping osquery logs. Agent may be missing, broken, or unable to transmit.' AS description
     , any_value(details) AS event_data
     , 'SnowAlert' AS detector
     , 'high' AS severity
     , '2060a772e50e4a4598808cdb76d2a315' AS query_id
     , 'owner-name' as owner
     , OBJECT_CONSTRUCT('query_id', query_id, 'cloud', 'aws', 'account_id', accountid, 'instance_id', instances) AS identity
FROM (
  SELECT timestamp as snapshot_at
       , instance:InstanceId::String as instances
       , instance:Tags::String as tags
       , instance:State:Name::String as state
       , instance:"AccountId" as accountid
       , instance:Platform::String as platform
  , instance:"LaunchTime" as launch_time
       , instance as details
  FROM snowalert.base_data.ec2_instance_snapshots_t as snap
  WHERE timestamp = (SELECT max(timestamp) from snowalert.base_data.ec2_instance_snapshots_t)
    AND tags NOT LIKE '%{"Key":"some-tag","Value":"some-value"}%'
    AND state NOT IN ('stopped', 'terminated')
    AND (platform != 'windows' or platform is null)
    AND NOT accountid = 'some-account'
) snap

-- pull the instance IDs from recent osquery logs and from AWS server inventory
LEFT JOIN (
  SELECT distinct instance_id::string AS id
  FROM security.prod.osquery_v
  WHERE osquery_v.event_time >= DATEADD(minute, -60, (SELECT max(timestamp) FROM snowalert.base_data.ec2_instance_snapshots_t))
) osquery
ON snap.instances = osquery.id

-- combine with account details and look for cases where instance is not found in recent logs
LEFT OUTER JOIN (SELECT * FROM snowalert.base_data.aws_accounts_t where timestamp = (SELECT max(timestamp) from snowalert.base_data.aws_accounts_t)) AS map
ON accountid = map.account_id
WHERE 1=1
  AND id IS NULL AND launch_time <= dateadd(minute, -15, current_timestamp) -- ignore servers started in the last 15 minutes
  GROUP BY 1,2,3,4,5,7,8,9,10,11
;