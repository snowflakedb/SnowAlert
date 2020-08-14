-- Users

CREATE OR REPLACE VIEW data.okta_users_snapshots
  COMMENT='all user snapshots'
AS
SELECT
  event_time recorded_at,
  raw,
  raw:id::STRING id,
  raw:status::STRING status,
  raw:created::TIMESTAMP_LTZ created,
  raw:activated::TIMESTAMP_LTZ activated,
  raw:statusChanged::TIMESTAMP_LTZ status_changed,
  raw:lastLogin::TIMESTAMP_LTZ last_login,
  raw:lastUpdated::TIMESTAMP_LTZ last_updated,
  raw:passwordChanged::TIMESTAMP_LTZ password_changed,
  raw:profile::VARIANT profile,
  raw:credentials::VARIANT credentials,
  raw:_links::VARIANT links
FROM data.okta_users_connection
;

CREATE OR REPLACE VIEW data.okta_users
  COMMENT='latest entry seen for each user'
AS
SELECT *
FROM data.okta_users_snapshots
QUALIFY 1=ROW_NUMBER() OVER (
  PARTITION BY id
  ORDER BY recorded_at DESC
)
;

-- Groups

CREATE OR REPLACE VIEW data.okta_groups_snapshots
  COMMENT='all groups snapshots'
AS
SELECT
  event_time recorded_at,
  raw,
  raw:id::STRING id,
  raw:created::TIMESTAMP_LTZ created,
  raw:lastUpdated::TIMESTAMP_LTZ last_updated,
  raw:lastMembershipUpdated::TIMESTAMP_LTZ last_membership_updated,
  raw:objectClass::VARIANT object_class,
  raw:type::STRING type,
  raw:profile::VARIANT profile,
  raw:_links::VARIANT links,
  raw:users::VARIANT users,
  raw:apps::VARIANT apps
FROM data.okta_groups_connection
;

CREATE OR REPLACE VIEW data.okta_groups
  COMMENT='latest entry seen for each group'
AS
SELECT *
FROM data.okta_groups_snapshots
QUALIFY 1=ROW_NUMBER() OVER (
  PARTITION BY id
  ORDER BY recorded_at DESC
)
;

-- System Logs

CREATE OR REPLACE VIEW data.okta_system_logs
  COMMENT='all system logs'
AS
SELECT
  event_time recorded_at,
  raw,
  raw:uuid::STRING uuid,
  raw:published::TIMESTAMP_LTZ published,
  raw:eventType::STRING event_type,
  raw:version::STRING version,
  raw:severity::STRING severity,
  raw:legacyEventType::STRING legacy_event_type,
  raw:displayMessage::STRING display_message,
  raw:actor::VARIANT actor,
  raw:client::VARIANT client,
  raw:request::VARIANT request,
  raw:outcome::VARIANT outcome,
  raw:target::VARIANT target,
  raw:transaction::VARIANT transaction,
  raw:debugContext::VARIANT debug_context,
  raw:authenticationContext::VARIANT authentication_context,
  raw:securityContext::VARIANT security_context
FROM data.okta_system_log_connection
;

SELECT * FROM data.okta_system_logs;
SELECT * FROM data.okta_users;
SELECT * FROM data.okta_groups;
