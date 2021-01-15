-- --
-- name: Jamf Inventory
-- params:
-- - name: basicauth
--   secret: true
-- - name: jamf_account
-- - name: sfk_api_integration
-- - name: aws_apigateway_prefix
-- - name: aws_apigateway_region

CREATE OR REPLACE SECURE EXTERNAL FUNCTION data.jamf_api(path STRING)
RETURNS VARIANT
RETURNS NULL ON NULL INPUT
VOLATILE
MAX_BATCH_ROWS=1
COMMENT='jamf_api: (path STRING) -> response'
API_INTEGRATION={sfk_api_integration}
HEADERS=(
  'host'='{jamf_account}.jamfcloud.com'
  'path'='/JSSResource/{0}'
  'headers'='accept=application%2Fjson'
  'basicauth'='{basicauth}'
)
AS 'https://{aws_apigateway_prefix}.execute-api.{aws_apigateway_region}.amazonaws.com/prod/https'
;

CREATE OR REPLACE TABLE data.jamf_inventory(
  recorded_at TIMESTAMP_LTZ,
  id NUMBER,
  name STRING,
  details VARIANT
)
;

CREATE OR REPLACE TASK jamf_inventory_computers
WAREHOUSE=snowalert_warehouse
SCHEDULE='USING CRON 0,12 * * * * UTC'
AS
INSERT INTO data.jamf_inventory
SELECT
  CURRENT_TIMESTAMP,
  value:id::NUMBER id,
  value:name::STRING name,
  data.jamf_api('computers/id/' || id) details
FROM (
  SELECT data.jamf_api('computers') r
)
, LATERAL FLATTEN (input => r:computers)
;
ALTER TASK jamf_inventory_computers RESUME;
