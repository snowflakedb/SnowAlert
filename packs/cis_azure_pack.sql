CREATE OR REPLACE VIEW rules.AZURE_CIS_1_3_VIOLATION_QUERY COPY GRANTS
  COMMENT='queries users collection where type is "Guest"
  @id PLSEUOLMOH
  @tags cis, azure, iam'
AS
SELECT 'PLSEUOLMOH' AS query_id
     , 'Azure CIS 1.3: there are no guest users' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id
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
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
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
         'role_definition_id', id
       ) AS identity
FROM (
  SELECT
    id,
    tenant_id,
    subscription_id,
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
    id, properties, tenant_id, subscription_id
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
     , 'Azure CIS 2.1: standard pricing tier is selected' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
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
     , 'Azure CIS 2.2: "Automatic provisioning of monitoring agent" is set to "On"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
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

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_3_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor System Updates" should not be "Disabled"
  @id 6QL7YIUFM6L
  @tags cis, azure, security-center'
AS
SELECT '6QL7YIUFM6L' AS query_id
     , 'Azure CIS 2.3: ASC Default policy setting "Monitor System Updates" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.3: ' || object AS description
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
  AND IFNULL(params:systemUpdatesMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_4_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor OS Vulnerabilities" should not be "Disabled"
  @id X52F9H0VP3C
  @tags cis, azure, security-center'
AS
SELECT 'X52F9H0VP3C' AS query_id
     , 'Azure CIS 2.4: ASC Default policy setting "Monitor OS Vulnerabilities" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.4: ' || object AS description
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
  AND IFNULL(params:systemConfigurationsMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_5_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor Endpoint Protection" should not be "Disabled"
  @id E429KPTCRA
  @tags cis, azure, security-center'
AS
SELECT 'E429KPTCRA' AS query_id
     , 'Azure CIS 2.5: ASC Default policy setting "Monitor Endpoint Protection" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.5: ' || object AS description
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
  AND IFNULL(params:endpointProtectionMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_6_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor Disk Encryption" should not be "Disabled"
  @id BVT8Z6CIGMR
  @tags cis, azure, security-center'
AS
SELECT 'BVT8Z6CIGMR' AS query_id
     , 'Azure CIS 2.6: ASC Default policy setting "Monitor Disk Encryption" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.6: ' || object AS description
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
  AND IFNULL(params:diskEncryptionMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_7_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor Network Security Groups" should not be "Disabled"
  @id M70FBQDUO
  @tags cis, azure, security-center'
AS
SELECT 'M70FBQDUO' AS query_id
     , 'Azure CIS 2.7: ASC Default policy setting "Monitor Network Security Groups" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.7: ' || object AS description
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
  AND IFNULL(params:networkSecurityGroupsMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_8_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor Web Application Firewall" should not be "Disabled"
  @id ZZ7T8U4VXV
  @tags cis, azure, security-center'
AS
SELECT 'ZZ7T8U4VXV' AS query_id
     , 'Azure CIS 2.8: ASC Default policy setting "Monitor Web Application Firewall" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.8: ' || object AS description
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
  AND IFNULL(params:webApplicationFirewallMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_9_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "NGFW Monitoring" should not be "Disabled"
  @id NPL91M5IRD
  @tags cis, azure, security-center'
AS
SELECT 'NPL91M5IRD' AS query_id
     , 'Azure CIS 2.9: ASC Default policy setting "NGFW Monitoring" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.9: ' || object AS description
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
  AND IFNULL(params:nextGenerationFirewallMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_10_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor Vulnerability Assessment" should not be "Disabled"
  @id 6XQKJV63MGW
  @tags cis, azure, security-center'
AS
SELECT '6XQKJV63MGW' AS query_id
     , 'Azure CIS 2.10: ASC Default policy setting "Monitor Vulnerability Assessment" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.10: ' || object AS description
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
  AND IFNULL(params:vulnerabilityAssesmentMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_11_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor Storage Blob Encryption" should not be "Disabled"
  @id MIZAVMRZFV
  @tags cis, azure, security-center'
AS
SELECT 'MIZAVMRZFV' AS query_id
     , 'Azure CIS 2.11: ASC Default policy setting "Monitor Storage Blob Encryption" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.11: ' || object AS description
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
  AND IFNULL(params:storageEncryptionMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_12_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor JIT Network Access" should not be "Disabled"
  @id 0606OV2Q7EP4
  @tags cis, azure, security-center'
AS
SELECT '0606OV2Q7EP4' AS query_id
     , 'Azure CIS 2.12: ASC Default policy setting "Monitor JIT Network Access" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.12: ' || object AS description
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
  AND IFNULL(params:jitNetworkAccessMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_13_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor Adaptive Application Whitelisting" should not be "Disabled"
  @id GCIEER9BOH
  @tags cis, azure, security-center'
AS
SELECT 'GCIEER9BOH' AS query_id
     , 'Azure CIS 2.13: ASC Default policy setting "Monitor Adaptive Application Whitelisting" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.13: ' || object AS description
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
  AND IFNULL(params:adaptiveApplicationControlsMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_14_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor SQL Auditing" should not be "Disabled"
  @id IDDTR9L5XRK
  @tags cis, azure, security-center'
AS
SELECT 'IDDTR9L5XRK' AS query_id
     , 'Azure CIS 2.14: ASC Default policy setting "Monitor SQL Auditing" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.14: ' || object AS description
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
  AND IFNULL(params:sqlAuditingMonitoringEffect, 'Disabled') = 'Disabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_15_VIOLATION_QUERY COPY GRANTS
  COMMENT='ASC setting "Monitor SQL Encryption" should not be "Disabled"
  @id GWYJUFKLHNQ
  @tags cis, azure, security-center'
AS
SELECT 'GWYJUFKLHNQ' AS query_id
     , 'Azure CIS 2.15: ASC Default policy setting "Monitor SQL Encryption" not "Disabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'AZ Subscription violating CIS 2.15: ' || object AS description
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
  AND IFNULL(params:sqlEncryptionMonitoringEffect, 'Disabled') = 'Disabled'
;


CREATE OR REPLACE VIEW rules.AZURE_CIS_2_16_VIOLATION_QUERY COPY GRANTS
  COMMENT='security contact should have email set
  @id JBD8BU7YWHJ
  @tags cis, azure, security-center'
AS
SELECT 'JBD8BU7YWHJ' AS query_id
     , 'Azure CIS 2.16: security contacts email is set' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'Subscription in violation of AZ CIS 2.16: ' || object AS description
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
  AND LENGTH(props:email) < 1
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_17_VIOLATION_QUERY COPY GRANTS
  COMMENT='security contacts should have phone number set
  @id OL06B7S4S2K
  @tags cis, azure, security-center'
AS
SELECT 'OL06B7S4S2K' AS query_id
     , 'Azure CIS 2.17: security contacts phone number is set' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'Subscription in violation of AZ CIS 2.17: ' || object AS description
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
  AND LENGTH(props:phone) < 1
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_18_VIOLATION_QUERY COPY GRANTS
  COMMENT='security contacts have high severity alert email notifications on
  @id 1URJFBNUAWH
  @tags cis, azure, security-center'
AS
SELECT '1URJFBNUAWH' AS query_id
     , 'Azure CIS 2.18: "Send email notification for high severity alerts" is set to "On"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'Subscription in violation of AZ CIS 2.18: ' || object AS description
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
  AND props:alertNotifications <> 'On'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_2_19_VIOLATION_QUERY COPY GRANTS
  COMMENT='security alerts emails to subscriptions owners should be on
  @id OZMX8LMRY6E
  @tags cis, azure, security-center'
AS
SELECT 'OZMX8LMRY6E' AS query_id
     , 'Azure CIS 2.19: "Send email also to subscription owners" is set to "On"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`, ' ||
         'named "' || name || '"'
       ) AS object
     , 'Subscription in violation of AZ CIS 2.19: ' || object AS description
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
  AND props:alertsToAdmins <> 'On'
;
