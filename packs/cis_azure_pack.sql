CREATE VIEW snowalert.rules.azure_cis_1_1_violation_query
  COMMENT='MFA must be enabled for all privileged users
  @id R6Q4AB22WH9
  @tags cis, azure, iam'
AS
SELECT 'R6Q4AB22WH9' AS query_id
     , 'Azure CIS 1.1: Enable MFA for privileged users' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id
       ) AS environment
     , (
         'User `' || user_principal_name || '`' ||
         '(' || user_display_name || ')'
       ) AS object
     , 'Violating AzCIS 1.1: ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'user_principal_name', user_principal_name,
         'cloud', 'azure',
         'tenant_id', tenant_id
       ) AS identity
FROM (
  SELECT DISTINCT
    usr.tenant_id,
    usr.id user_id,
    user_principal_name,
    user_display_name,
    is_mfa_registered,
    ras.id role_assignment_id,
    ras.properties:roleDefinitionId::STRING role_definition_id,
    rds.name role_name,
    rds.properties role_props
  FROM (
    SELECT *
    FROM data.azure_collect_reports_credential_user_registration_details
    WHERE recorded_at > CURRENT_TIMESTAMP - INTERVAL '1 days'
  ) rcs
  LEFT OUTER JOIN (
    SELECT *
    FROM data.azure_collect_users
    WHERE recorded_at > CURRENT_TIMESTAMP - INTERVAL '1 days'
  ) usr
  USING (user_principal_name)
  LEFT OUTER JOIN (
    SELECT *
    FROM data.azure_collect_role_assignments
    WHERE recorded_at > CURRENT_TIMESTAMP - INTERVAL '1 days'
  ) ras
  ON ras.properties:principalId = usr.id
  LEFT OUTER JOIN (
    SELECT *
    FROM data.azure_collect_role_definitions
    WHERE recorded_at > CURRENT_TIMESTAMP - INTERVAL '1 days'
  ) rds
  ON rds.id = ras.properties:roleDefinitionId
)
WHERE 1=1
  AND is_mfa_registered = FALSE
  AND (
    role_props:roleName LIKE '%Contributor'
    OR role_props:roleName ILIKE 'owner'
    OR role_props:roleName ILIKE 'admin'
  )
;

CREATE VIEW snowalert.rules.azure_cis_1_2_violation_query
  COMMENT='MFA must be enabled for all non-privileged users
  @id YRHDIMOSP6K
  @tags cis, azure, iam'
AS
SELECT 'YRHDIMOSP6K' AS query_id
     , 'Azure CIS 1.2: Enable MFA for non-privileged users' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id
       ) AS environment
     , (
         'User `' || user_principal_name || '`' ||
         '(' || user_display_name || ')'
       ) AS object
     , 'Violating AzCIS 1.2: ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'user_principal_name', user_principal_name,
         'cloud', 'azure',
         'tenant_id', tenant_id
       ) AS identity
FROM (
  SELECT DISTINCT
    usr.tenant_id,
    usr.id user_id,
    user_principal_name,
    user_display_name,
    is_mfa_registered,
    ras.id role_assignment_id,
    ras.properties:roleDefinitionId::STRING role_definition_id,
    rds.name role_name,
    rds.properties role_props
  FROM (
    SELECT *
    FROM data.azure_collect_reports_credential_user_registration_details
    WHERE recorded_at > CURRENT_TIMESTAMP - INTERVAL '1 days'
  ) rcs
  LEFT OUTER JOIN (
    SELECT *
    FROM data.azure_collect_users
    WHERE recorded_at > CURRENT_TIMESTAMP - INTERVAL '1 days'
  ) usr
  USING (user_principal_name)
  LEFT OUTER JOIN (
    SELECT *
    FROM data.azure_collect_role_assignments
    WHERE recorded_at > CURRENT_TIMESTAMP - INTERVAL '1 days'
  ) ras
  ON ras.properties:principalId = usr.id
  LEFT OUTER JOIN (
    SELECT *
    FROM data.azure_collect_role_definitions
    WHERE recorded_at > CURRENT_TIMESTAMP - INTERVAL '1 days'
  ) rds
  ON rds.id = ras.properties:roleDefinitionId
)
WHERE 1=1
  AND is_mfa_registered = FALSE
  AND NOT (
    role_props:roleName LIKE '%Contributor'
    OR role_props:roleName ILIKE 'owner'
    OR role_props:roleName ILIKE 'admin'
  )
;

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

CREATE OR REPLACE VIEW rules.AZURE_CIS_3_1_VIOLATION_QUERY COPY GRANTS
  COMMENT='storage accounts should require https
  @id TQLDIHBL0P
  @tags cis, azure, storage-accounts, https'
AS
SELECT 'TQLDIHBL0P' AS query_id
     , 'Azure CIS 3.1: storage accounts should require secure transfer' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Storage account "' || name || '", ' ||
         'in subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`'
       ) AS object
     , 'Storage account in violation of AZ CIS 3.1: ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'query_id', query_id,
         'tenant_id', tenant_id,
         'subscription_id', subscription_id,
         'storage_account_name', name
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    name,
    kind,
    properties:supportsHttpsTrafficOnly secure_transfer_required,
    tags
  FROM data.azure_collect_storage_accounts
  WHERE recorded_at > CURRENT_DATE - 2
)
WHERE 1=1
  AND secure_transfer_required = false
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_3_3_VIOLATION_QUERY COPY GRANTS
  COMMENT='Enable Storage logging for Queue service read, write, and delete requests
  @id 15V7N4XMSJE
  @tags cis, azure, storage-accounts'
AS
SELECT '15V7N4XMSJE' AS query_id
     , 'Azure CIS 3.3: Storage logging' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , 'Queue logging in storage account ' || account_name AS object
     , 'AZ CIS 3.3 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'tenant_id', tenant_id,
         'subscription_id', subscription_id,
         'account_name', account_name
       ) AS identity
FROM (
  SELECT
    tenant_id,
    subscription_id,
    account_name,
    logging
  FROM data.azure_collect_queue_services_properties
  QUALIFY 1=ROW_NUMBER() OVER (
    PARTITION BY tenant_id, subscription_id, account_name
    ORDER BY recorded_at DESC
  )
)
WHERE 1=1
  AND NOT (
    logging:Delete = 'true'
    AND logging:Read = 'true'
    AND logging:Write = 'true'
  )
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_3_6_VIOLATION_QUERY COPY GRANTS
  COMMENT='storage accounts should have no public access
  @id Y1GWLA9G4K
  @tags cis, azure, storage-accounts, public-access'
AS
SELECT 'Y1GWLA9G4K' AS query_id
     , 'Azure CIS 3.6: "Public access level" is set to Private for blob containers' AS title
     , OBJECT_CONSTRUCT(
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Storage account "' || account_name || '", ' ||
         'in container "' || container_name || '", ' ||
         'in subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`'
       ) AS object
     , 'AZ CIS 3.6 violated by public access on ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'query_id', query_id,
         'tenant_id', tenant_id,
         'subscription_id', subscription_id,
         'account_name', account_name,
         'container_name', container_name
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    account_name,
    name container_name,
    properties
  FROM data.azure_collect_storage_accounts_containers
  WHERE recorded_at > CURRENT_DATE - 2
)
WHERE 1=1
  AND properties:PublicAccess IS NOT NULL
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_3_7_VIOLATION_QUERY COPY GRANTS
  COMMENT='Storage Account default network access should not be "Allow"
  @id 421R8Y8EVAB
  @tags cis, azure, storage-accounts'
AS
SELECT '421R8Y8EVAB' AS query_id
     , 'Azure CIS 3.7: Storage Account default network access rule should not be "Allow"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Storage account "' || account_name || '", ' ||
         'in subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`'
       ) AS object
     , 'AZ CIS 3.7 violated by network default access set to "Allow" on ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'storage_account_id', storage_account_id
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    id storage_account_id,
    kind,
    name account_name,
    properties:networkAcls.defaultAction::STRING network_default_action,
    tags
  FROM data.azure_collect_storage_accounts
  WHERE recorded_at > CURRENT_DATE - 2
)
WHERE 1=1
  AND network_default_action = 'Allow'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_4_1_VIOLATION_QUERY COPY GRANTS
  COMMENT='SQL Server Auditing should be Enabled
  @id E9WUH828JAQ
  @tags cis, azure, storage-accounts'
AS
SELECT 'E9WUH828JAQ' AS query_id
     , 'Azure CIS 4.1: SQL Server Auditing Enabled' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , 'SQL Server `' || server_full_id || '`' AS object
     , 'AZ CIS 4.1 (audit enabled) violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'server_full_id', server_full_id
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    REGEXP_SUBSTR(server_full_id, '/subscriptions/([^/]+)', 1, 1, 'e', 1) subscription_id,
    server_full_id,
    properties:state::STRING auditing_state
  FROM data.azure_collect_sql_servers_auditing_settings
  WHERE recorded_at > CURRENT_DATE - 2
)
WHERE 1=1
  AND auditing_state != 'Enabled'
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_3_8_VIOLATION_QUERY COPY GRANTS
  COMMENT='"Trusted Microsoft Services" is enabled for Storage Account access
  @id D4K5N625QNJ
  @tags cis, azure, storage-accounts'
AS
SELECT 'D4K5N625QNJ' AS query_id
     , 'Azure CIS 3.8: "Trusted Microsoft Services" is enabled for Storage Account access' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Storage account "' || account_name || '", ' ||
         'in subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`'
       ) AS object
     , 'AZ CIS 3.8 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'storage_account_id', storage_account_id
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    id storage_account_id,
    kind,
    name account_name,
    properties:networkAcls.bypass::STRING network_bypass,
    tags
  FROM data.azure_collect_storage_accounts
  WHERE recorded_at > CURRENT_DATE - 2
)
WHERE 1=1
  AND REGEXP_INSTR(network_bypass, '\\bAzureServices\\b') = 0
;


CREATE OR REPLACE VIEW rules.AZURE_CIS_5_1_1_VIOLATION_QUERY COPY GRANTS
  COMMENT='Log Profiles exist for every subscription
  @id 05R5437IZC2F
  @tags cis, azure, log-profiles'
AS
SELECT '05R5437IZC2F' AS query_id
     , 'Azure CIS 5.1.1: Every Subscription should have a Log Profile' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`.'
       ) AS object
     , 'CIS 5.1.1 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id,
         'log_profile_id', log_profile_id
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    id log_profile_id,
    identity,
    kind,
    location,
    name,
    properties,
    tags,
    type
  FROM data.azure_collect_log_profiles
  WHERE recorded_at > CURRENT_DATE - 1
)
WHERE 1=1
  AND name IS NULL
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_5_1_2_VIOLATION_QUERY COPY GRANTS
  COMMENT='Set Log Profiles retention to 365 days or greater
  @id 6E90XE64X3K
  @tags cis, azure, log-profiles'
AS
SELECT '6E90XE64X3K' AS query_id
     , 'Azure CIS 5.1.2: Log Profile retention length' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`.'
       ) AS object
     , 'CIS 5.1.2 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id,
         'log_profile_id', log_profile_id
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    id log_profile_id,
    identity,
    kind,
    location,
    name,
    properties,
    IFNULL(properties:retentionPolicy.days, 0) retention_days,
    IFNULL(properties:retentionPolicy.enabled, FALSE) retention_enabled,
    tags,
    type
  FROM data.azure_collect_log_profiles
  WHERE recorded_at > CURRENT_DATE - 1
    AND retention_enabled = TRUE
)
WHERE 1=1
  AND retention_days < 365
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_5_1_3_VIOLATION_QUERY COPY GRANTS
  COMMENT='Log Profiles should retain all categories
  @id 2JJNE5ZV9WY
  @tags cis, azure, log-profiles'
AS
SELECT '2JJNE5ZV9WY' AS query_id
     , 'Azure CIS 5.1.3: Log Profile retention categories' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`.'
       ) AS object
     , 'CIS 5.1.3 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id,
         'log_profile_id', log_profile_id
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    id log_profile_id,
    identity,
    kind,
    location,
    name,
    properties,
    IFNULL(properties:categories, ARRAY_CONSTRUCT()) log_profile_categories,
    tags,
    type
  FROM data.azure_collect_log_profiles
  WHERE recorded_at > CURRENT_DATE - 1
    AND name IS NOT NULL  -- disclude the 5.1.1 violations (no log profile)
)
WHERE 1=1
  AND (
    NOT ARRAY_CONTAINS('Write'::VARIANT, log_profile_categories)
    OR NOT ARRAY_CONTAINS('Delete'::VARIANT, log_profile_categories)
    OR NOT ARRAY_CONTAINS('Action'::VARIANT, log_profile_categories)
  )
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_5_1_4_VIOLATION_QUERY COPY GRANTS
  COMMENT='log profile captures activity logs for all regions including global
  @id M63QX83WJXL
  @tags cis, azure, log-profiles'
AS
-- TODO: add global location coverage
SELECT 'M63QX83WJXL' AS query_id
     , 'Azure CIS 5.1.4: Log Profile retention regions' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`.'
       ) AS object
     , 'CIS 5.1.4 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id,
         'location_name', location_name
       ) AS identity
FROM (
  SELECT
    locs.tenant_id,
    locs.subscription_id,
    locs.location_name location_name,
    profs.location_name log_profile_location
  FROM (
    SELECT DISTINCT
      tenant_id,
      subscription_id,
      id location_id,
      name location_name,
      display_name location_display_name
    FROM data.azure_collect_subscriptions_locations
    WHERE recorded_at > CURRENT_DATE - 1
  ) locs
  LEFT OUTER JOIN (
    SELECT DISTINCT
      tenant_id,
      subscription_id,
      id log_profile_id,
      identity,
      kind,
      value::STRING location_name,
      name log_profile_name,
      properties,
      tags,
      type
    FROM data.azure_collect_log_profiles p,
    LATERAL FLATTEN (input => properties:locations)
    WHERE recorded_at > CURRENT_DATE - 1
  ) profs
  ON (
    locs.tenant_id = profs.tenant_id
    AND locs.subscription_id = profs.subscription_id
    AND locs.location_name = profs.location_name
  )
)
WHERE 1=1
  AND log_profile_location IS NULL
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_5_1_5_VIOLATION_QUERY COPY GRANTS
  COMMENT='storage container storing the activity logs should not be publicly accessible
  @id WE59BTELH49
  @tags cis, azure, log-profiles'
AS
SELECT 'WE59BTELH49' AS query_id
     , 'Azure CIS 5.1.5: storage container storing the activity logs should not be publicly accessible' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'account', tenant_id
       ) AS environment
     , (
         'Container  "' || sa_container_name || '"' ||
         'in Storage Account `' || storage_account_name || '`, ' ||
         'in Subscription `' || subscription_id || '`, ' ||
         'in Tenant `' || tenant_id || '`.'
       ) AS object
     , 'AZ Subscription violating CIS 5.1.5: ' || object AS description
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
  SELECT *
  FROM (
    SELECT
      properties:storageAccountId::STRING storage_account_id,
      SPLIT(storage_account_id, '/')[8]::STRING storage_account_name,
      'insight-operational-logs' sa_container_name
    FROM data.azure_collect_log_profiles
    WHERE recorded_at > CURRENT_DATE - 1
      AND storage_account_id IS NOT NULL
  ) log_profile
  INNER JOIN (
    SELECT DISTINCT
      tenant_id,
      subscription_id,
      account_name storage_account_name,
      name sa_container_name,
      properties:PublicAccess public_access,
      properties
    FROM data.azure_collect_storage_accounts_containers
    WHERE recorded_at > CURRENT_DATE - 7
  ) storage_container
  USING (
    storage_account_name,
    sa_container_name
  )
)
WHERE 1=1
  AND public_access IS NOT NULL
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_5_1_6_VIOLATION_QUERY COPY GRANTS
  COMMENT='storage account containing the container with activity logs should be encrypted with BYOK
  @id QC0ASF70MI8
  @tags cis, azure, log-profiles'
AS
SELECT 'QC0ASF70MI8' AS query_id
     , 'Azure CIS 5.1.6: storage container storing the activity logs should be encrypted with BYOK' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'account', tenant_id
       ) AS environment
     , (
         'Storage Account `' || storage_account_name || '`, ' ||
         'in Subscription `' || subscription_id || '`, ' ||
         'in Tenant `' || tenant_id || '`.'
       ) AS object
     , 'AZ Subscription violating CIS 5.1.6: ' || object AS description
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
  SELECT *
  FROM (
    SELECT
      properties:storageAccountId::STRING storage_account_id,
      SPLIT(storage_account_id, '/')[8]::STRING storage_account_name
    FROM data.azure_collect_log_profiles
    WHERE recorded_at > CURRENT_DATE - 1
      AND storage_account_id IS NOT NULL
  ) log_profile
  INNER JOIN (
    SELECT DISTINCT
      tenant_id,
      subscription_id,
      name storage_account_name,
      properties:encryption.keySource::STRING key_source,
      properties:encryption.keyVaultProperties::STRING key_vault_properties,
      properties
    FROM data.azure_collect_storage_accounts
    WHERE recorded_at > CURRENT_DATE - 7
  ) storage_account
  USING (
    storage_account_name
  )
)
WHERE 1=1
  AND NOT (
    key_source = 'Microsoft.Keyvault'
    AND key_vault_properties IS NOT NULL
    -- todo: make example and test this
  )
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_5_1_7_VIOLATION_QUERY COPY GRANTS
  COMMENT='logging for Azure KeyVault is Enabled
  @id 1OMJCL2ANXN
  @tags cis, azure, log-profiles'
AS
SELECT '1OMJCL2ANXN' AS query_id
     , 'Azure CIS 5.1.7: logging for Azure KeyVault is "Enabled"' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'account', tenant_id
       ) AS environment
     , (
         'Vault `' || vault_id || '`, ' ||
         'in Subscription `' || subscription_id || '`, ' ||
         'in Tenant `' || tenant_id || '`.'
       ) AS object
     , 'AZ Subscription violating CIS 5.1.7: ' || object AS description
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
  SELECT
    properties:logs logs,
    logs[0]:category::STRING log_category,
    logs[0]:enabled::BOOLEAN log_enabled,
    logs[0]:retentionPolicy.days::NUMBER log_retention_days,
    logs[0]:retentionPolicy.enabled::BOOLEAN log_retention_enabled,
    *
  FROM (
    SELECT
      tenant_id,
      subscription_id,
      id vault_id,
      name
    FROM data.azure_collect_vaults
    WHERE recorded_at > CURRENT_DATE - 3
      AND name IS NOT NULL
  ) vaults
  LEFT JOIN (
    SELECT DISTINCT
      resource_uri vault_id,
      properties
    FROM data.azure_collect_diagnostic_settings
    WHERE recorded_at > CURRENT_DATE - 7
  ) storage_account
  USING (
    vault_id
  )
)
WHERE 1=1
  AND (
    logs IS NULL
    OR log_category <> 'AuditEvent'
    OR log_enabled != TRUE
    OR (
      -- TODO: check with support if logic is same as 5.1.2
      log_retention_enabled = TRUE
      AND log_retention_days = 0
    )
  )
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_6_1_VIOLATION_QUERY COPY GRANTS
  COMMENT='RDP access is restricted from the internet
  @id U2MV5Z68P3C
  @tags cis, azure, networking'
AS
SELECT 'U2MV5Z68P3C' AS query_id
     , 'Azure CIS 6.1: RDP access is restricted from the internet' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'NSG with the name "' || nsg_name || '", ' ||
         'in subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`'
       ) AS object
     , 'AZ CIS 6.1 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'tenant_id', tenant_id,
         'subscription_id', subscription_id,
         'nsg_id', subscription_id
       ) AS identity
FROM (
  SELECT DISTINCT
    tenant_id,
    subscription_id,
    id nsg_id,
    etag nsg_etag,
    name nsg_name,
    location nsg_location,
    properties nsg_properties,
    value security_rule,
    value:properties.access::STRING access,
    value:properties.destinationPortRange::STRING destination_port_range,
    value:properties.direction::STRING direction,
    value:properties.protocol::STRING protocol,
    value:properties.sourceAddressPrefix::STRING source_address_prefix
  FROM data.azure_collect_network_security_groups
  , LATERAL FLATTEN (input => properties:securityRules)
  WHERE recorded_at > CURRENT_DATE - 2
)
WHERE 1=1
  AND access = 'Allow'
  AND direction = 'Inbound'
  AND protocol = 'TCP'
  AND (
    destination_port_range = '3389'
    OR (
      IFF(
        CONTAINS(destination_port_range, '-'),
        TO_NUMBER(SPLIT(destination_port_range, '-')[0]) <= 3389
        AND TO_NUMBER(SPLIT(destination_port_range, '-')[1]) >= 3389,
        FALSE
      )
    )
  )
  -- TODO: handle multiple port ranges like '22,101,103,200-210'
  AND source_address_prefix IN (
    '*',
    '0.0.0.0',
    '<nw>/0',
    '/0',
    'internet',
    'any'
  )
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_6_2_VIOLATION_QUERY COPY GRANTS
  COMMENT='SSH access is restricted from the internet
  @id OJWU2K5B4WO
  @tags cis, azure, networking'
AS
SELECT 'OJWU2K5B4WO' AS query_id
     , 'Azure CIS 6.2: SSH access is restricted from the internet' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'NSG with the name "' || nsg_name || '", ' ||
         'in subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`'
       ) AS object
     , 'AZ CIS 6.2 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'tenant_id', tenant_id,
         'subscription_id', subscription_id,
         'nsg_id', nsg_id
       ) AS identity
FROM (
  SELECT
    tenant_id,
    subscription_id,
    id nsg_id,
    etag nsg_etag,
    name nsg_name,
    location nsg_location,
    properties nsg_properties,
    value security_rule,
    value:properties.access::STRING access,
    value:properties.destinationPortRange::STRING destination_port_range,
    value:properties.direction::STRING direction,
    value:properties.protocol::STRING protocol,
    value:properties.sourceAddressPrefix::STRING source_address_prefix
  FROM data.azure_collect_network_security_groups
  , LATERAL FLATTEN (input => properties:securityRules)
  WHERE recorded_at > CURRENT_DATE - 2
)
WHERE 1=1
  AND access = 'Allow'
  AND direction = 'Inbound'
  AND protocol = 'TCP'
  AND (
    destination_port_range = '22'
    OR (
      IFF(
        CONTAINS(destination_port_range, '-'),
        TO_NUMBER(SPLIT(destination_port_range, '-')[0]) <= 22
        AND TO_NUMBER(SPLIT(destination_port_range, '-')[1]) >= 22,
        FALSE
      )
    )
  )
  -- TODO: handle multiple port ranges like '22,101,103,200-210'
  AND source_address_prefix IN (
    '*',
    '0.0.0.0',
    '<nw>/0',
    '/0',
    'internet',
    'any'
  )
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_6_5_VIOLATION_QUERY COPY GRANTS
  COMMENT='Network Watcher enabled for each Subscription Location
  @id P5N44TUVJ9N
  @tags cis, azure, networking'
AS
SELECT 'P5N44TUVJ9N' AS query_id
     , 'Azure CIS 6.5: Network Watcher enabled for each Subscription Location' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Location "' || location_name || '", ' ||
         'in subscription `' || subscription_id || '`, ' ||
         'in tenant `' || tenant_id || '`'
       ) AS object
     , 'AZ CIS 6.5 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'tenant_id', tenant_id,
         'subscription_id', subscription_id,
         'location_name', location_name
       ) AS identity
FROM (
  SELECT
    tenant_id,
    subscription_id,
    location_name,
    location_id,
    location_display_name
  FROM (
    SELECT DISTINCT
      tenant_id,
      subscription_id,
      id location_id,
      name location_name,
      display_name location_display_name
    FROM data.azure_collect_subscriptions_locations
    WHERE recorded_at > CURRENT_DATE - 1
  ) subs
  LEFT OUTER JOIN (
    SELECT DISTINCT
      tenant_id,
      subscription_id,
      id nw_id,
      etag nw_etag,
      name nw_name,
      location location_name,
      properties nw_properties
    FROM data.azure_collect_network_watchers
    WHERE recorded_at > CURRENT_DATE - 1
      AND properties:provisioningState = 'Succeeded'
  ) nws
  USING (
    tenant_id,
    subscription_id,
    location_name
  )
  WHERE nw_id IS NULL
)
WHERE 1=1
;


CREATE OR REPLACE VIEW rules.AZURE_CIS_7_1_VIOLATION_QUERY COPY GRANTS
  COMMENT='OS Disk must be encrypted
  @id F7HQ2BVPBQG
  @tags cis, azure, virtual-machines'
AS
SELECT 'F7HQ2BVPBQG' AS query_id
     , 'Azure CIS 7.1: OS Disk must be encrypted' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'VM ' || vm_id
       ) AS object
     , 'AZ CIS 7.1 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'vm_id', vm_id
       ) AS identity
FROM (
  SELECT
    subscription_id,
    tenant_id,
    vm_id,
    os_disk_id,
    encryption
  FROM (
    SELECT DISTINCT
      subscription_id,
      tenant_id,
      id vm_id,
      properties vm_properties,
      properties:storageProfile.osDisk.managedDisk.id::STRING os_disk_id
    FROM data.azure_collect_virtual_machines
    WHERE recorded_at > CURRENT_DATE - 1
      AND id IS NOT NULL
  ) vm
  LEFT OUTER JOIN (
    SELECT DISTINCT
      id disk_id,
      properties:encryption encryption
    FROM data.azure_collect_disks
    WHERE recorded_at > CURRENT_DATE - 1
  ) disk
  ON (os_disk_id = disk_id)
  -- TODO(afedorov): data is missing some disks, and
  -- so the following removes those visibility errors
  -- from triggering this violation. once we find out
  -- why, we can remove this WHERE condition.
  WHERE disk_id IS NOT NULL
)
WHERE 1=1
  AND encryption:type NOT IN (
    'EncryptionAtRestWithCustomerKey',
    'EncryptionAtRestWithPlatformAndCustomerKeys'
  )
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_7_2_VIOLATION_QUERY COPY GRANTS
  COMMENT='Data Disks must be encrypted
  @id JF1IPB3TZ
  @tags cis, azure, virtual-machines'
AS
SELECT 'JF1IPB3TZ' AS query_id
     , 'Azure CIS 7.2: Data Disks must be encrypted' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'VM ' || vm_id
       ) AS object
     , 'AZ CIS 7.2 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'vm_id', vm_id
       ) AS identity
FROM (
  SELECT
    subscription_id,
    tenant_id,
    vm_id,
    data_disk_id,
    encryption
  FROM (
    SELECT DISTINCT
      subscription_id,
      tenant_id,
      id vm_id,
      properties vm_properties,
      value:managedDisk.id::STRING data_disk_id
    FROM data.azure_collect_virtual_machines,
         LATERAL FLATTEN(input => properties:storageProfile.dataDisks)
    WHERE recorded_at > CURRENT_DATE - 1
      AND id IS NOT NULL
  ) vm
  LEFT OUTER JOIN (
    SELECT DISTINCT
      id disk_id,
      properties:encryption encryption
    FROM data.azure_collect_disks
    WHERE recorded_at > CURRENT_DATE - 1
  ) disk
  ON (data_disk_id = disk_id)
  -- TODO(afedorov): data is missing some disks, and
  -- so the following removes those visibility errors
  -- from triggering this violation. once we find out
  -- why, we can remove this WHERE condition.
  WHERE disk_id IS NOT NULL
)
WHERE 1=1
  AND encryption:type NOT IN (
    'EncryptionAtRestWithCustomerKey',
    'EncryptionAtRestWithPlatformAndCustomerKeys'
  )
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_7_3_VIOLATION_QUERY COPY GRANTS
  COMMENT='Unattached disks must be encrypted
  @id CN4YBO0X01B
  @tags cis, azure, virtual-machines'
AS
SELECT 'CN4YBO0X01B' AS query_id
     , 'Azure CIS 7.3: Unattached disks must be encrypted' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id,
         'subscription_id', subscription_id
       ) AS environment
     , (
         'Disk ' || disk_id
       ) AS object
     , 'AZ CIS 7.3 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'disk_id', disk_id
       ) AS identity
FROM (
    SELECT DISTINCT
      id disk_id,
      tenant_id,
      subscription_id,
      managed_by,
      properties:encryption encryption
    FROM data.azure_collect_disks
    WHERE recorded_at > CURRENT_DATE - 1
      AND disk_id IS NOT NULL
      AND managed_by IS NULL
)
WHERE 1=1
  AND encryption:type NOT IN (
    'EncryptionAtRestWithCustomerKey',
    'EncryptionAtRestWithPlatformAndCustomerKeys'
  )
;


CREATE OR REPLACE VIEW rules.AZURE_CIS_7_4_VIOLATION_QUERY COPY GRANTS
  COMMENT='Only approved VM extensions installed
  @id 58CYJ8J9MC4
  @tags cis, azure, virtual-machines'
AS
SELECT '58CYJ8J9MC4' AS query_id
     , 'Azure CIS 7.4: Only approved VM extensions installed' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id
       ) AS environment
     , vm_id AS object
     , 'AzCIS 7.4 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'vm_id', query_id
       ) AS identity
FROM (
  SELECT DISTINCT tenant_id, vm_id, name extension_name
  FROM data.azure_collect_virtual_machines_extensions
  WHERE recorded_at > CURRENT_DATE - 1
    AND name IS NOT NULL
)
WHERE 1=1
  AND extension_name NOT IN (
    'LinuxDiagnostic',
    'AzureNetworkWatcherExtension'
  )
;


CREATE OR REPLACE VIEW rules.AZURE_CIS_8_1_VIOLATION_QUERY COPY GRANTS
  COMMENT='Expiration date is set on all keys
  @id J9SXTR77OP
  @tags cis, azure, security-considerations'
AS
SELECT 'J9SXTR77OP' AS query_id
     , 'Azure CIS 8.1: Expiration date is set on all keys' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id
       ) AS environment
     , (
         'key ' || key_id
       ) AS object
     , 'AzCIS 8.1 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'key_id', key_id
       ) AS identity
FROM (
  SELECT
    tenant_id,
    kid key_id,
    attributes,
    attributes:enabled enabled,
    attributes:exp::TIMESTAMP expires
  FROM azure_collect_vaults_keys
  WHERE error IS NULL
    AND recorded_at > CURRENT_TIMESTAMP - INTERVAL '1 days'
)
WHERE 1=1
  AND enabled
  AND expires IS NULL
;

CREATE OR REPLACE VIEW rules.AZURE_CIS_8_2_VIOLATION_QUERY COPY GRANTS
  COMMENT='Expiration date is set on all secrets
  @id HSUI200N9J
  @tags cis, azure, security-considerations'
AS
SELECT 'HSUI200N9J' AS query_id
     , 'Azure CIS 8.2: Expiration date is set on all keys' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'azure',
         'tenant_id', tenant_id
       ) AS environment
     , (
         'secret ' || secret_id
       ) AS object
     , 'AzCIS 8.2 violated by ' || object AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'High' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'secret_id', secret_id
       ) AS identity
FROM (
  SELECT
    tenant_id,
    id secret_id,
    attributes,
    attributes:enabled enabled,
    attributes:exp::TIMESTAMP expires
  FROM azure_collect_vaults_secrets
  WHERE error IS NULL
    AND recorded_at > CURRENT_TIMESTAMP - INTERVAL '1 days'
)
WHERE 1=1
  AND enabled
  AND expires IS NULL
;
