-- --
-- name: ZenGRC Inventory
-- params:
-- - name: auth
--   secret: true
-- - name: zengrc_subdomain
-- - name: sfk_api_integration
-- - name: aws_apigateway_prefix
-- - name: aws_apigateway_region

CREATE OR REPLACE SECURE EXTERNAL FUNCTION data.zengrc_snowflake_api(path STRING)
  RETURNS VARIANT
  RETURNS NULL ON NULL INPUT
  VOLATILE
  COMMENT='https://docs.api.zengrc.com/'
  API_INTEGRATION={sfk_api_integration}
  HEADERS=(
    'base-url'='https://{zengrc_subdomain}.api.zengrc.com'
    'url'='{0}'
    'auth'='{auth}'
  )
  AS 'https://{aws_apigateway_prefix}.execute-api.{aws_apigateway_region}.amazonaws.com/prod/https'
;

CREATE OR REPLACE TABLE zengrc_connection (
  recorded_at TIMESTAMP_LTZ,
  href STRING,
  result VARIANT,
  next_href STRING
)
;

CREATE OR REPLACE PROCEDURE data.zengrc_connection(resources VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
USING TEMPLATE 'zengrc_connection.js'
;

CREATE OR REPLACE TASK zengrc_connection
WAREHOUSE=snowalert_warehouse
SCHEDULE='USING CRON 0 0 * * * UTC'
AS
CALL zengrc_connection(ARRAY_CONSTRUCT(
  'assessments',
  'audits',
  'issues',
  'requests',
  'controls',
  'people',
  'objectives',
  'programs',
  'systems',
  'risks'
));
