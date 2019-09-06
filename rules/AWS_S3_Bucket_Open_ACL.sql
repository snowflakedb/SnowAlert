CREATE OR REPLACE VIEW rules.aws_s3_bucket_open_acl_violation_query COPY GRANTS
  COMMENT='S3 Bucket ACLs can grant or deny access to specific entities. This rule checks for public bucket access.
  @id 6rn7ad2y7tv
  @tags aws, s3, scout2'
AS
SELECT OBJECT_CONSTRUCT(
         'account_id', raw:"awsAccountId",
         'resource_type', raw:"resourceType",
         'name', raw:"resourceName",
         'unique_keys', OBJECT_CONSTRUCT(
                          'query_id', '6rn7ad2y7tv',
                          'cloud', 'aws',
                          'account_id', raw:"awsAccountId"
                        )
       ) AS environment
     , raw:"resourceName"::STRING AS object
     , 'AWS S3 Bucket Open ACL' AS title
     , (
         'Grantee ' || this:"grantee" || ' ' ||
         'has permission ' || this:"permission" || ' ' ||
         'on bucket ' || object
       ) AS description
     , CURRENT_TIMESTAMP() AS alert_time
     , OBJECT_CONSTRUCT('config', raw) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , '6rn7ad2y7tv' AS query_id
     , 'Cloud Engineering' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', raw:"awsAccountId"
       ) AS identity
FROM (
  SELECT *
  FROM (
    SELECT *
    FROM (
      SELECT raw, value AS value2
      FROM (
        SELECT DISTINCT PARSE_JSON(value) AS value2
             , raw
             , event_time
        FROM data.aws_config_default_events_connection
           , LATERAL FLATTEN(input => raw:supplementaryConfiguration)
        WHERE raw:resourceType = 'AWS::S3::Bucket'
          AND path = 'AccessControlList'
          AND event_time > dateadd(hour, -25, CURRENT_TIMESTAMP())
      ), LATERAL FLATTEN(input => value2:grantList)
    ), LATERAL FLATTEN(input => value2)
  )
  WHERE value2:grantee IN ('AllUsers', 'AuthenticatedUsers')
    AND LOWER(value2:permission) IN ('read', 'read_acp', 'write', 'write_acp')
)
WHERE 1=1
  AND 2=2
;
