CREATE OR REPLACE VIEW rules.VQ_QBYAC8Z2RBF_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.1: Avoid the use of the "root" account
  queries 30d of CloudTrail records for non-support root account use
  @id QBYAC8Z2RBF
  @tags cis, aws, activity, user'
AS
SELECT 'QBYAC8Z2RBF' AS query_id
     , 'AWS CIS 1.1: Avoid the use of the "root" account' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', IFNULL(account_alias, account_id)
       ) AS environment
     , actor_id || ' in account ' || environment:account AS object
     , (
         'IAM user or access key ' || object ||
         ' acted as root in the last 30 days.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , raw AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'query_id', query_id,
         'account_id', account_id,
         'actor_id', actor_id
       ) AS identity
FROM (
  SELECT raw
       , COALESCE(
           user_identity_username,
           user_identity_access_key_id
         ) actor_id
       , recipient_account_id account_id
  FROM data.cloudtrail
  WHERE event_time > DATEADD(DAY, -30, CURRENT_TIMESTAMP)
    AND user_identity_type = 'Root'
    AND source_ip_address <> 'support.amazonaws.com'
) results
LEFT OUTER JOIN (
  SELECT DISTINCT account_id, account_alias
  FROM data.aws_collect_iam_list_account_aliases
) a
USING (account_id)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_GVR3N9WQLGG_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.3: Ensure credentials unused for 90 days or greater are disabled
  @id GVR3N9WQLGG
  @tags cis, aws, activity, user'
AS
SELECT 'GVR3N9WQLGG' AS query_id
     , 'AWS CIS 1.3: Ensure credentials unused for 90 days or greater are disabled' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', IFNULL(account_alias, account_id)
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'environment', environment,
         'user_name', user_name
       ) AS identity
     , user_name || ' at ' || environment:account AS object
     , (
         'IAM user ' || user_name ||
         ' in ' || environment:account ||
         ' has not used their login credentials in over 90 days.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  SELECT user_name, account_id
  FROM (
    -- all login users in all accounts
    SELECT DISTINCT user_name, account_id
    FROM data.aws_collect_iam_get_login_profile
    WHERE recorded_at > DATEADD(DAY, -1, CURRENT_TIMESTAMP)
  ) u
  LEFT OUTER JOIN (
    -- all login records in past 90d
    SELECT DISTINCT user_identity_username user_name
                  , recipient_account_id account_id
                  , TRUE login_record
    FROM data.cloudtrail
    WHERE user_identity_invokedby = 'signin.amazonaws.com'
      AND event_time > DATEADD(day, -90, CURRENT_TIMESTAMP)
  ) recent
  USING (user_name, account_id)
  WHERE login_record IS NULL
) results
LEFT OUTER JOIN (
  SELECT DISTINCT account_id, account_alias
  FROM data.aws_collect_iam_list_account_aliases
) accounts
USING (account_id)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_WM00E51BLCE_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.4: Ensure access keys are rotated every 90 days or less
  @id WM00E51BLCE
  @tags cis, aws, configuration, user'
AS
SELECT 'WM00E51BLCE' AS query_id
     , 'AWS CIS 1.4: Ensure credentials unused for 90 days or greater are disabled' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', IFNULL(account_alias, account_id)
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id,
         'user_name', user_name
       ) AS identity
     , user_name || ' at ' || environment:account AS object
     , (
         'Access key for IAM user ' || object
         || ' has not been rotated in over 90 days.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  SELECT account_alias, user_name, create_date, results.account_id
  FROM (
    SELECT DISTINCT account_id, user_name, create_date, status
    FROM data.aws_collect_iam_list_access_keys
    WHERE create_date < DATEADD(day, -90, CURRENT_TIMESTAMP)
      AND status <> 'Inactive'
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
  ORDER BY create_date ASC
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_F85S78KK42_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.5: Ensure IAM password policy requires at least one uppercase letter
  @id F85S78KK42
  @tags cis, aws, configuration, account'
AS
SELECT 'F85S78KK42' AS query_id
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id
       ) AS identity
     , IFNULL(account_alias, account_id) AS object
     , 'AWS CIS 1.5: Ensure IAM password policy requires at least one uppercase letter' AS title
     , 'Password policy at ' || object || ' does not require at least one uppercase letter.' AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  SELECT results.account_id, account_alias
  FROM (
    SELECT DISTINCT account_id, require_uppercase_characters
    FROM data.aws_collect_iam_get_account_password_policy
    WHERE require_uppercase_characters <> 'true'
       OR require_uppercase_characters IS NULL
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_NUNJCFNQ13_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.6: Ensure IAM password policy require at least one lowercase letter
  @id NUNJCFNQ13
  @tags cis, aws, configuration, account'
AS
SELECT 'NUNJCFNQ13' AS query_id
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id
       ) AS identity
     , IFNULL(account_alias, account_id) AS object
     , 'AWS CIS 1.6: Ensure IAM password policy require at least one lowercase letter' AS title
     , (
         'Password policy at ' || object
         || ' does not require at least one lowercase letter.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  SELECT results.account_id, account_alias
  FROM (
    SELECT DISTINCT account_id, require_lowercase_characters
    FROM data.aws_collect_iam_get_account_password_policy
    WHERE require_lowercase_characters <> 'true'
       OR require_lowercase_characters is null
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_ASQCIZO9VC_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.7: Ensure IAM password policy require at least one symbol
  @id ASQCIZO9VC
  @tags cis, aws, configuration, account'
AS
SELECT 'ASQCIZO9VC' AS query_id
     , 'AWS CIS 1.7: Ensure IAM password policy require at least one symbol' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , IFNULL(account_alias, account_id) AS object
     , (
         'Password policy at ' || object ||
         ' does not require at least one symbol.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id
       ) AS identity
FROM (
  SELECT results.account_id, account_alias
  FROM (
    SELECT DISTINCT account_id, require_symbols
    FROM data.aws_collect_iam_get_account_password_policy
    WHERE require_symbols <> 'true'
       OR require_symbols IS NULL
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_SKNR99XCFYS_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.8: Ensure IAM password policy require at least one number
  @id SKNR99XCFYS
  @tags cis, aws, configuration, account'
AS
SELECT 'SKNR99XCFYS' AS query_id
     , 'AWS CIS 1.8: Ensure IAM password policy require at least one number' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id
       ) AS identity
     , IFNULL(account_alias, account_id) AS object
     , (
         'Password policy at ' || object
         || ' does not require at least one number.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  SELECT results.account_id, account_alias
  FROM (
    SELECT DISTINCT account_id, require_numbers
    FROM data.aws_collect_iam_get_account_password_policy
    WHERE require_numbers <> 'true'
       OR require_numbers IS NULL
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_NDHSGN5MT9L_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.9: Ensure IAM password policy requires minimum length of 14 or greater
  @id NDHSGN5MT9L
  @tags cis, aws, configuration, account'
AS
SELECT 'NDHSGN5MT9L' AS query_id
     , 'AWS CIS 1.9: Ensure IAM password policy requires minimum length of 14 or greater' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id
       ) AS identity
     , IFNULL(account_alias, account_id) AS object
     , 'Password policy at ' || object || ' does not require at least one number.' AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  SELECT results.account_id, account_alias
  FROM (
    SELECT DISTINCT account_id, minimum_password_length
    from data.aws_collect_iam_get_account_password_policy
    where minimum_password_length < 14
    or minimum_password_length is null
  ) as results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_A80WZDM7JP_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.10: Ensure IAM password policy prevents password reuse
  @id A80WZDM7JP
  @tags cis, aws, configuration, account'
AS
SELECT 'A80WZDM7JP' AS query_id
     , 'AWS CIS 1.10: Ensure IAM password policy prevents password reuse' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id
       ) AS identity
     , IFNULL(account_alias, account_id) AS object
     , 'Password policy at ' || object || ' does not prevent password reuse.' AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  SELECT results.account_id, account_alias
  FROM (
    SELECT DISTINCT account_id, password_reuse_prevention
    FROM data.aws_collect_iam_get_account_password_policy
    WHERE password_reuse_prevention < 24
       OR password_reuse_prevention IS NULL
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_SAVBJIUB6OH_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.11: Ensure IAM password policy expires passwords within 90 days or less
  @id SAVBJIUB6OH
  @tags cis, aws, configuration, account'
AS
SELECT 'SAVBJIUB6OH' AS query_id
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id
       ) AS identity
     , IFNULL(account_alias, account_id) AS object
     , 'AWS CIS 1.11: Ensure IAM password policy expires passwords within 90 days or less' AS title
     , (
         'Password policy at ' || object
         || ' does not expire passwords within 90 days.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  SELECT results.account_id, account_alias
  FROM (
    SELECT DISTINCT account_id, max_password_age, expire_passwords
    FROM data.aws_collect_iam_get_account_password_policy
    WHERE expire_passwords <> 'true'
       OR expire_passwords IS NULL
       OR max_password_age > 90
       OR max_password_age IS NULL
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_YO2KZE2JCG9_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.12: Ensure no root account access key exists
  @id YO2KZE2JCG9
  @tags cis, aws, configuration, account'
AS
SELECT 'YO2KZE2JCG9' AS query_id
     , 'AWS CIS 1.12: Ensure no root account access key exists' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'query_id', query_id,
         'account_id', account_id
       ) AS identity
     , 'root user at account ' || account_id AS object
     , (
         'The ' || object || ' ' ||
         '(' || COALESCE(account_name, account_alias) || ') ' ||
         'has an access key.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , OBJECT_CONSTRUCT(
         'account_name', account_name,
         'account_alias', account_alias,
         'account_id', account_id,
         'user_arn', arn,
         'key_active', key_active
       ) AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  WITH latest_report_at AS (
    SELECT account_id, MAX(recorded_at) recorded_at
    FROM data.aws_collect_iam_get_credential_report
    GROUP BY account_id
  ),
  latest_alias AS (
    SELECT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
    QUALIFY 1=ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY recorded_at DESC)
  ),
  latest_name AS (
    SELECT id account_id, name account_name
    FROM data.aws_collect_organizations_list_accounts_connection
    QUALIFY 1=ROW_NUMBER() OVER (PARTITION BY id ORDER BY recorded_at DESC)
  )
  SELECT account_id, account_alias, account_name, user, arn, key_active
  FROM (
    SELECT recorded_at
         , account_id
         , value:arn::STRING arn
         , value:user::STRING user
         , (
             value:access_key_1_active::BOOLEAN
             OR value:access_key_2_active::BOOLEAN
           ) key_active
    FROM data.aws_collect_iam_get_credential_report r
    JOIN latest_report_at USING (account_id, recorded_at)
    , LATERAL FLATTEN(input => r.content_csv_parsed)
  )
  LEFT OUTER JOIN latest_alias USING (account_id)
  LEFT OUTER JOIN latest_name USING (account_id)
)
WHERE 1=1
  AND user='<root_account>'
  AND key_active
;


CREATE OR REPLACE VIEW rules.VQ_NLDRNE9GIQB_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.13: Ensure MFA is enabled for the "root" account
  @id NLDRNE9GIQB
  @tags cis, aws, configuration, account'
AS
SELECT 'NLDRNE9GIQB' AS query_id
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id
       ) AS identity
     , IFNULL(account_alias, account_id) AS object
     , 'AWS CIS 1.13: Ensure MFA is enabled for the "root" account' AS title
     , (
         'The root user at the ' || object
         || ' account does not have MFA enabled.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  WITH accounts AS (
    SELECT t1.account_id AS id
         , recorded_at AS latest
    FROM data.aws_collect_iam_get_account_summary t1
    INNER JOIN (
      SELECT account_id
           , MAX(recorded_at) AS latest
      FROM data.aws_collect_iam_get_account_summary
      GROUP BY account_id
    ) t2
    ON (
      t2.account_id = t1.account_id
      AND t1.recorded_at = t2.latest
    )
  )
  SELECT a.account_id, account_alias
  FROM (
    SELECT account_id
    FROM data.aws_collect_iam_get_account_summary, accounts
    WHERE account_id = accounts.id
      AND recorded_at = accounts.latest
      AND (
        account_mfa_enabled <> '1'
        OR account_mfa_enabled IS NULL
      )
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_ZDIAEIEAMP_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.14: Ensure hardware MFA is enabled for the "root" account
  @id ZDIAEIEAMP
  @tags cis, aws, configuration'
AS
SELECT 'ZDIAEIEAMP' AS query_id
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT('query_id', 'ZDIAEIEAMP', 'cloud', 'aws', 'account_id', account_id) AS identity
     , IFNULL(account_alias, account_id) AS object
     , 'AWS CIS 1.14: Ensure hardware MFA is enabled for the "root" account' AS title
     , (
         'The root user at the ' || object
         || ' account does not have a hardware MFA enabled.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  WITH accounts AS (
    SELECT t1.account_id AS id, recorded_at AS latest
    FROM data.aws_collect_iam_get_account_summary t1
    INNER JOIN (
      SELECT account_id, MAX(recorded_at) AS latest
      FROM data.aws_collect_iam_get_account_summary
      GROUP BY account_id
    ) t2 on t2.account_id = t1.account_id and t1.recorded_at = t2.latest
  ),
  devices AS (
    SELECT serial_number
    FROM data.aws_collect_iam_list_virtual_mfa_devices
    WHERE serial_number LIKE '%root%'
  )
  SELECT a.account_id, account_alias
  FROM (
    SELECT DISTINCT account_id
    FROM data.aws_collect_iam_get_account_summary, accounts, devices
    WHERE (
      account_id = accounts.id
      AND recorded_at = accounts.latest
      AND (
        account_mfa_enabled <> '1'
        OR account_mfa_enabled IS NULL
        OR (
          account_mfa_enabled = '1'
          AND serial_number LIKE '%root%'
        )
      )
    )
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_ASKJ865AQ9_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.16: Ensure IAM policies are attached only to groups or roles
  @id ASKJ865AQ9
  @tags cis, aws, configuration, policy'
AS
SELECT 'ASKJ865AQ9' AS query_id
     , 'AWS CIS 1.16: Ensure IAM policies are attached only to groups or roles' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id,
         'policy_arn', policy_arn
       ) AS identity
     , policy_arn AS object
     ,
         'The policy ' || policy_arn || ' '
         'is attached to user ' || user_name || '.'
       ) AS description
     , recorded_last AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  SELECT account_id
       , account_alias
       , policy_arn
       , user_name
       , MAX(recorded_at) AS recorded_last
  FROM data.aws_collect_iam_list_entities_for_policy
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) accounts
  USING (account_id)
  WHERE (
    user_id IS NOT NULL
    OR user_name IS NOT NULL
  )
  GROUP BY account_alias, account_id, policy_arn, user_name
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_ZIXF9ISIDDB_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.20: Ensure a support role has been created to manage incidents with AWS Support
  @id ZIXF9ISIDDB
  @tags cis, aws'
AS
SELECT 'ZIXF9ISIDDB' AS query_id
     , 'AWS CIS 1.20: Ensure a support role has been created to manage incidents with AWS Support' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id,
         'policy_arn', policy_arn
       ) AS identity
     , IFNULL(account_alias, account_id) AS object
     , 'The account ' || object || ' does not have an attached AWS Support policy.' AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  WITH accounts AS (
    SELECT t1.account_id AS id
         , t1.policy_arn AS policy
         , t1.recorded_at AS latest
    FROM data.aws_collect_iam_list_entities_for_policy t1
    INNER JOIN (
      SELECT account_id, policy_arn, MAX(recorded_at) AS latest
      FROM data.aws_collect_iam_list_entities_for_policy
      GROUP BY policy_arn, account_id
    ) t2
    ON (
      t1.account_id = t2.account_id
      AND t1.policy_arn = t2.policy_arn
      AND t1.recorded_at = t2.latest
    )
  )
  SELECT a.account_id, account_alias, policy_arn, user_name
  FROM (
    SELECT recorded_at, account_id, policy_arn, user_name
    FROM data.aws_collect_iam_list_entities_for_policy, accounts
    WHERE policy_arn = accounts.policy
      AND recorded_at = accounts.latest
      AND account_id = accounts.id
      AND (
        policy_arn = 'arn:aws:iam::aws:policy/AWSSupportAccess'
        AND group_name IS NULL
        AND user_name IS NULL
        AND role_name IS NULL
      )
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_1PZWDBTHHVS_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 1.22: Ensure IAM policies that allow full "*:*" administrative privileges are not created
  @id 1PZWDBTHHVS
  @tags cis, aws, configuration, policy'
AS
SELECT '1PZWDBTHHVS' AS query_id
     , 'AWS CIS 1.22: Ensure IAM policies that allow full "*:*" administrative privileges are not created' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id,
         'policy_arn', policy_arn
       ) AS identity
     , policy_arn AS object
     , 'The policy ' || policy_arn  || ' allows full administrative privileges.' AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  WITH latest_policies AS (
    SELECT DISTINCT t1.account_id AS account
                  , t1.policy_arn AS arn
                  , recorded_at AS latest_time
    FROM data.aws_collect_iam_get_policy_version t1
    INNER JOIN (
      SELECT DISTINCT account_id
                    , policy_arn
                    , MAX(recorded_at) AS latest
      FROM data.aws_collect_iam_get_policy_version
      GROUP BY policy_arn, account_id
    ) t2
    ON (
      t1.account_id = t2.account_id
      AND t1.policy_arn = t2.policy_arn
      AND t1.recorded_at = t2.latest
    )
  )
  SELECT a.account_id
       , account_alias
       , policy_arn
  FROM (
    SELECT account_id
         , policy_arn
         , value:Effect::STRING AS effect
         , value:Action::STRING AS action
         , value:Resource::STRING AS resource
    FROM data.aws_collect_iam_get_policy_version
       , LATERAL FLATTEN( input => document:Statement )
       , latest_policies
    WHERE policy_arn = latest_policies.arn
      AND recorded_at = latest_policies.latest_time
      AND account_id = latest_policies.account
      AND effect = 'Allow'
      AND (
        action LIKE '%"*"%'
        OR action = '*'
      )
      AND (
        resource LIKE '%"*"%'
        OR resource = '*'
      )
    AND policy_arn <> 'arn:aws:iam::aws:policy/AdministratorAccess'
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_G8HQTE899ZL_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 2.2: Ensure CloudTrail log file validation is enabled
  @id G8HQTE899ZL
  @tags cis, aws, configuration, trail'
AS
SELECT 'G8HQTE899ZL' AS query_id
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id,
         'trail_name', trail_name
       ) AS identity
     , trail_name AS object
     , 'AWS CIS 2.2: Ensure CloudTrail log file validation is enabled' AS title
     , (
         'The trail ' || trail_name
         || ' does not have log file validation enabled.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , object AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  WITH latest_results AS (
    SELECT DISTINCT t1.account_id AS account
                  , t1.name AS trail_name
                  , recorded_at AS latest_time
    FROM data.aws_collect_cloudtrail_describe_trails t1
    INNER JOIN (
      SELECT DISTINCT account_id
                    , name
                    , MAX(recorded_at) AS latest
      FROM data.aws_collect_cloudtrail_describe_trails
      GROUP BY name, account_id
    ) t2
    ON (
      t1.account_id = t2.account_id
      AND t1.name = t2.name
      AND t1.recorded_at = t2.latest
    )
  )
  SELECT a.account_id, account_alias, trail_name
  FROM (
    SELECT DISTINCT account_id, name AS trail_name
    FROM data.aws_collect_cloudtrail_describe_trails t
    JOIN latest_results
    ON (
      account_id = latest_results.account
      AND name = latest_results.trail_name
      AND recorded_at = latest_results.latest_time
    )
    WHERE LOG_FILE_VALIDATION_ENABLED = 'FALSE'
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.account_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_0XADVOX2M5CQ_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 2.3: Ensure the S3 bucket used to store CloudTrail logs is not publicly accessible
  @id 0XADVOX2M5CQ
  @tags cis, aws, configuration, s3'
AS
SELECT '0XADVOX2M5CQ' AS query_id
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', a_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', a_id,
         'bucket', bucket
       ) AS identity
     , bucket AS object
     , 'AWS CIS 2.3: Ensure the S3 bucket used to store CloudTrail logs is not publicly accessible' AS title
     , (
         'The bucket ' || bucket
          || ' allows public access to the data from trail ' || trail_name
          || '.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , policy AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  WITH trail_buckets AS (
    SELECT account_id
         , name AS trail_name
         , s3_bucket_name
    FROM (
      WITH latest_results AS (
        SELECT DISTINCT t1.account_id AS account
                      , t1.name AS trail_name
                      , recorded_at AS latest_time
        FROM data.aws_collect_cloudtrail_describe_trails t1
        INNER JOIN (
          SELECT DISTINCT account_id
                        , name
                        , MAX(recorded_at) AS latest
          FROM data.aws_collect_cloudtrail_describe_trails
          GROUP BY name, account_id
        ) t2
        ON t1.account_id = t2.account_id
           AND t1.name = t2.name
           AND t1.recorded_at = t2.latest
      )
      SELECT recorded_at
           , account_id
           , name
           , S3_BUCKET_NAME
      FROM data.aws_collect_cloudtrail_describe_trails
      JOIN latest_results
        ON account_id = latest_results.account
           AND name = latest_results.trail_name
           AND recorded_at = latest_results.latest_time
    )
  )
  SELECT DISTINCT account_alias
                , a_id
                , bucket
                , policy
                , trail_name
  FROM (
    SELECT recorded_at
         , bucket_policies.account_id AS a_id
         , bucket
         , policy
         , value AS policy_block
         , trail_name
    FROM data.aws_collect_s3_get_bucket_policy as bucket_policies
    JOIN trail_buckets
      ON trail_buckets.account_id = bucket_policies.account_id
         AND trail_buckets.s3_bucket_name = bucket_policies.bucket
    JOIN lateral flatten ( input => PARSE_JSON(policy):Statement )
    WHERE (
      -- Get policies where a policy block ALLOWS some action for ANYONE
      policy_block LIKE '%"Effect":"Allow"%'
      AND (
        policy_block LIKE '%"Principal":"*"%'
        OR policy_block LIKE '%"Principal":{"AWS":"*"}%'
      )
      -- Unless the policy block limits by SourceArn condition
      AND NOT (
        policy_block RLIKE '.*"Condition":\\s*{\\s*"ArnEquals":\\s*{\\s*"aws:SourceArn":\\s*"arn:aws:iam.*'
      )
      -- Unless the policy denies all except specific matches
      AND NOT (
          policy LIKE '%"Effect":%"Deny"%'
          AND policy LIKE '%"StringNotEquals"%'
      )
      AND NOT (
          policy LIKE '%"Effect":%"Deny"%'
          AND policy LIKE '%"NotPrincipal"%'
      )
    )
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.account_id = results.a_id
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_PWXDLM6H16_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 2.5: Ensure AWS Config is enabled in all regions
  @id PWXDLM6H16
  @tags cis, aws, configuration, config'
AS
SELECT 'PWXDLM6H16' AS query_id
     , 'AWS CIS 2.5: Ensure AWS Config is enabled in all regions' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id
       ) AS identity
     , account_alias AS object
     , (
         'The account ' || account_alias
         || ' does not have Config service enabled in all regions.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , 'Check Config settings' AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  WITH all_accounts AS (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  )
  SELECT all_accounts.account_id, account_alias
  FROM (
    SELECT DISTINCT account_id
    FROM data.aws_collect_config_describe_configuration_recorders
    WHERE recording_group:allSupported = TRUE
      AND recording_group:includeGlobalResourceTypes = TRUE
  ) recorded_accounts
  RIGHT OUTER JOIN all_accounts
  ON all_accounts.account_id = recorded_accounts.account_id
  WHERE recorded_accounts.account_id IS NULL
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_JW0HBLN2EW_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 4.1: Ensure no security groups allow ingress from 0.0.0.0/0 to port 22
  @id JW0HBLN2EW
  @tags cis, aws, configuration, security group, sg'
AS
SELECT 'JW0HBLN2EW' AS query_id
     , 'AWS CIS 4.1: Ensure no security groups allow ingress from 0.0.0.0/0 to port 22' AS title
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id,
         'group_id', group_id
       ) AS identity
     , group_id AS object
     , (
         'The security group ' || group_name
         || ' allows ingress to TCP/22 from any IP address.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , group_description AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  WITH latest_groups AS (
    SELECT DISTINCT t1.group_id AS id
                  , recorded_at AS latest_time
    FROM data.aws_collect_ec2_describe_security_groups t1
    INNER JOIN (
      SELECT DISTINCT group_id, max(recorded_at) AS latest
      FROM data.aws_collect_ec2_describe_security_groups
      GROUP BY group_id
    ) t2
    ON (
      t1.group_id = t2.group_id
      AND t1.recorded_at = t2.latest
    )
  )
  SELECT account_id
       , account_alias
       , description AS group_description
       , group_name
       , group_id
  FROM (
    SELECT DISTINCT recorded_at
                  , account_id
                  , description
                  , group_name
                  , group_id
                  , value:IpProtocol::STRING AS prot
                  , value:FromPort AS range_start
                  , value:ToPort AS range_end
                  , value:IpRanges::STRING AS source_ips_v4
                  , value:Ipv6Ranges::STRING AS source_ips_v6
    FROM data.aws_collect_ec2_describe_security_groups t
    JOIN latest_groups
    ON (
      group_id = latest_groups.id
      AND recorded_at = latest_groups.latest_time
    )
    , LATERAL FLATTEN( input => t.ip_permissions )
    WHERE (
      prot = 'tcp'
      AND (
        (
          range_start IS NULL
          AND range_end IS NULL
        )
        OR (
          range_start <= 22
          AND range_end >= 22
        )
      )
      AND (
        source_ips_v4 LIKE '%0.0.0.0%'
        OR source_ips_v6 LIKE '::/0'
      )
    )
  ) results
  LEFT OUTER JOIN (
      SELECT DISTINCT account_id, account_alias
      FROM data.aws_collect_iam_list_account_aliases
  ) a
  USING (account_id)
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_M6Y4F1UEW3P_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 4.2: Ensure no security groups allow ingress from 0.0.0.0/0 to port 3389
  @id M6Y4F1UEW3P
  @tags cis, aws, configuration, security group, sg'
AS
SELECT 'M6Y4F1UEW3P' AS query_id
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id,
         'group_id', group_id
       ) AS identity
     , group_id AS object
     , 'AWS CIS 4.2: Ensure no security groups allow ingress from 0.0.0.0/0 to port 3389' AS title
     , (
         'The security group ' || group_name
         || ' allows ingress to RDP (TCP/UDP 3389)  from any IP address.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , group_description AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  WITH latest_groups AS (
    SELECT DISTINCT t1.group_id AS id, recorded_at AS latest_time
    from data.aws_collect_ec2_describe_security_groups t1
    INNER JOIN (
      SELECT DISTINCT group_id, max(recorded_at) AS latest
      FROM data.aws_collect_ec2_describe_security_groups
      GROUP BY group_id
    ) t2
    ON (
      t1.group_id = t2.group_id
      AND t1.recorded_at = t2.latest
    )
  )
  SELECT account_id
       , account_alias
       , description AS group_description
       , group_name
       , group_id
       , ip_permissions
  FROM (
    SELECT DISTINCT recorded_at
                  , account_id
                  , description
                  , group_name
                  , group_id
                  , ip_permissions
                  , value:IpProtocol::STRING AS prot
                  , value:FromPort AS range_start
                  , value:ToPort AS range_end
                  , value:IpRanges::STRING AS source_ips_v4
                  , value:Ipv6Ranges::STRING AS source_ips_v6
    FROM data.aws_collect_ec2_describe_security_groups t
    JOIN latest_groups
    ON (
      group_id = latest_groups.id
      AND recorded_at = latest_groups.latest_time
    )
    , lateral flatten(input => t.ip_permissions)
    WHERE (
      prot = 'tcp'
      OR prot = 'udp'
    )
    AND (
      (
        range_start IS NULL
        AND range_end IS NULL
      )
      OR (
        range_start <= 3389
        AND range_end >= 3389
      )
    )
    AND (
      source_ips_v4 LIKE '%0.0.0.0%'
      OR source_ips_v6 LIKE '::/0'
    )
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  USING (account_id)
)
WHERE 1=1
  AND 2=2
;


CREATE OR REPLACE VIEW rules.VQ_P7QZMWJ6Z0O_VIOLATION_QUERY COPY GRANTS
  COMMENT='AWS CIS 4.3: Ensure the default security group of every VPC restricts all traffic
  @id P7QZMWJ6Z0O
  @tags cis, aws, configuration, security group, sg'
AS
SELECT 'P7QZMWJ6Z0O' AS query_id
     , OBJECT_CONSTRUCT(
         'cloud', 'aws',
         'account', account_alias,
         'account_id', account_id
       ) AS environment
     , OBJECT_CONSTRUCT(
         'query_id', query_id,
         'cloud', 'aws',
         'account_id', account_id,
         'group_id', group_id
       ) AS identity
     , group_id AS object
     , 'AWS CIS 4.3: Ensure the default security group of every VPC restricts all traffic' AS title
     , (
         'The default security group ' || group_id
         || ' in account ' || account_alias
         || ' does not restrict all traffic.'
       ) AS description
     , CURRENT_TIMESTAMP AS alert_time
     , group_description AS event_data
     , 'SnowAlert' AS detector
     , 'Medium' AS severity
     , 'devsecops' AS owner
FROM (
  WITH latest_groups AS (
    SELECT DISTINCT t1.group_id AS id
                  , recorded_at AS latest_time
    FROM data.aws_collect_ec2_describe_security_groups t1
    INNER JOIN (
      SELECT DISTINCT group_id, max(recorded_at) AS latest
      FROM data.aws_collect_ec2_describe_security_groups
      GROUP BY group_id
    ) t2
    ON (
      t1.group_id = t2.group_id
      AND t1.recorded_at = t2.latest
    )
  )
  SELECT account_id
       , account_alias
       , description AS group_description
       , group_name
       , group_id
       , ip_permissions
  FROM (
    SELECT DISTINCT recorded_at
                  , account_id
                  , description
                  , group_name
                  , group_id
                  , ip_permissions
    FROM data.aws_collect_ec2_describe_security_groups t
    JOIN latest_groups
    ON (
      group_id = latest_groups.id
      AND recorded_at = latest_groups.latest_time
    )
    , LATERAL FLATTEN(input => t.ip_permissions)
    WHERE group_name = 'default'
      AND value:IpProtocol::STRING <> '-1'
  ) results
  LEFT OUTER JOIN (
    SELECT DISTINCT account_id id, account_alias
    FROM data.aws_collect_iam_list_account_aliases
  ) a
  ON a.id = results.account_id
)
WHERE 1=1
  AND 2=2
;

