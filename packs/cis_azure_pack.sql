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

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_1_VIOLATION_QUERY COPY GRANTS
  COMMENT='standard pricing tier should be selected
  @id AY64LVA734B
  @tags cis, azure, security-center'
AS
SELECT 'AY64LVA734B' AS query_id
     , 'Azure CIS 2.1: Ensure that standard pricing tier is selected' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'account', tenant_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'named "' || name || '", ' ||
         'in tenant `' || tenant_id || '`'
       ) AS object
     , 'Subscription in violation of AZ CIS 2.1: ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    name,
    type,
    properties:pricingTier::STRING pricing_tier
  FROM data.azure_collect_pricings
  WHERE recorded_at > CURRENT_DATE - 2
)
WHERE 1=1
  AND pricing_tier != 'Standard'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_2_VIOLATION_QUERY COPY GRANTS
  COMMENT='Automatic provisioning of monitoring agent should be "On"
  @id I9QOIRZ53QG
  @tags cis, azure, security-center'
AS
SELECT 'I9QOIRZ53QG' AS query_id
     , 'Azure CIS 2.2: Ensure that "Automatic provisioning of monitoring agent" is set to "On"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'account', tenant_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'Subscription in violation of AZ CIS 2.2: ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    name,
    type,
    properties:autoProvision::STRING auto_provision
  FROM data.azure_collect_auto_provisioning_settings
  WHERE recorded_at > CURRENT_DATE - 2
)
WHERE 1=1
  AND auto_provision != 'On'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_3_TO_15_VIOLATION_QUERY COPY GRANTS
  COMMENT='Various ASC Default policy settings should not be "Disabled"
  @id 6QL7YIUFM6L
  @tags cis, azure, security-center'
AS
SELECT '6QL7YIUFM6L' AS query_id
     , 'Azure CIS 2.3 to 2.15: Ensure various ASC Default policy settings are not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'account', tenant_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`,' ||
         'named "' || name || '"'
       ) AS object
     , 'Subscription in violation of AZ CIS 2.3—15: ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    name,
    type,
    sku,
    properties:parameters params
  FROM data.azure_collect_policy_assignments
  WHERE recorded_at > CURRENT_DATE - 2
)
WHERE 1=1
  AND 'Disabled' IN (
    params:systemUpdatesMonitoringEffect,
    params:systemConfigurationsMonitoringEffect,
    params:endpointProtectionMonitoringEffect,
    params:diskEncryptionMonitoringEffect,
    params:networkSecurityGroupsMonitoringEffect,
    params:webApplicationFirewallMonitoringEffect,
    params:nextGenerationFirewallMonitoringEffect,
    params:vulnerabilityAssesmentMonitoringEffect,
    params:storageEncryptionMonitoringEffect,
    params:jitNetworkAccessMonitoringEffect,
    params:adaptiveApplicationControlsMonitoringEffect,
    params:sqlAuditingMonitoringEffect,
    params:sqlEncryptionMonitoringEffect
 )
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_16_TO_19_VIOLATION_QUERY COPY GRANTS
  COMMENT='security contacts should have email, phone, alerting to "On"
  @id 0MZTQ654TBM
  @tags cis, azure, security-center'
AS
SELECT '0MZTQ654TBM' AS query_id
     , 'Azure CIS 2.16 to 2.19: Ensure security contacts are configured right' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'account', tenant_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'Subscription in violation of AZ CIS 2.16—19: ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'query_id', query_id,
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    name,
    type,
    properties props
  FROM data.azure_collect_security_contacts
  WHERE recorded_at > CURRENT_DATE - 2
    AND type IS NOT NULL
)
WHERE 1=1
  AND (
    LENGTH(props:email) < 1
    OR LENGTH(props:phone) < 1
    OR props:alertNotifications <> 'On'
    OR props:alertsToAdmins <> 'On'
  )
;
