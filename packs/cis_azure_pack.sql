CREATE OR REPLACE VIEW rules.AZURE_CIS_1_3_VIOLATION_QUERY COPY GRANTS
  COMMENT='queries users collection where type is "Guest"
  @id PLSEUOLMOH
  @tags cis, azure, iam'
AS
SELECT 'PLSEUOLMOH' AS query_id
     , 'Azure CIS 1.3: Ensure that there are no guest users' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'account', tenant_id
       ) AS environment
     , user_principal_name || ' in tenant ' || environment:account AS object
     , object || ' is a guest user in volation of AZ CIS 1.3' AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'user_principal_name', user_principal_name
       ) AS identity
FROM data.azure_collect_users
WHERE 1=1
  AND user_type='Guest'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_1_23_VIOLATION_QUERY COPY GRANTS
  COMMENT='queries role definitions with assignableScopes "/" or "/subscrptions/*" and actions "*"
  @id 7MDFB8Z0NKS
  @tags cis, azure, iam'
AS
SELECT '7MDFB8Z0NKS' AS query_id
     , 'Azure CIS 1.23: No custom subscription owner roles are created' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'account', tenant_id
       ) AS environment
     , id || ' in tenant ' || environment:account AS object
     , 'Role Definition in violation of AZ CIS 1.3: ' || id AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'role_definition_id', id
       ) AS identity
FROM (
  SELECT
    id,
    tenant_id,
    BOOLOR_AGG(
      path RLIKE '^assignableScopes\\[[0-9]+\\]$'
      AND (value = '/' OR value RLIKE '^/subscriptions/[0-9a-f-]+$')
    ) is_assigned_to_root_or_subscription_scope,
    BOOLOR_AGG(
      path RLIKE '^permissions\\[[0-9]+\\].actions\\[[0-9]+\\]'
      AND value = '*'
    ) is_permitting_all_actions,
    properties
  FROM data.azure_collect_role_definitions
     , LATERAL FLATTEN(input => properties, recursive => true)
  WHERE
    recorded_at > CURRENT_DATE - 3
  GROUP BY
    id, properties, tenant_id
)
WHERE 1=1
  AND is_assigned_to_root_or_subscription_scope
  AND is_permitting_all_actions
  AND properties:type <> 'BuiltInRole'
;
