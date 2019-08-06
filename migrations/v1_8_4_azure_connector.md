## Azure Log Connector

> Note: we say "like" below because if you set a custom name on your AWS Config Data Connector, you'll need to replace `default` in the above snippets with that custom name.

### File Ingestion

In the v1.8.3 Azure Log DC, we now ingest data using an external table over the Azure Stage; this removes our need to store credentials and call the Azure API, as well as allows us to ingest logs at the event level as opposed to the file level.

If you'd like to migrate the Azure Log DC, you can do so with a snippet like —

~~~
USE ROLE snowalert;
DROP PIPE data.azure_log_default_operation_default_pipe;
DROP STAGE data.azure_log_default_operation_default_stage;
ALTER TABLE data.azure_log_default_operation_connection SET COMMENT='
---
module: azure_log
';
~~~

And recreate the connection via the Data Connector WebUI. Since you are not dropping the connection table, you will not lose any of your historical data. When dropping an Azure Log DC, make sure that you specify both the log type (operation | signin | audit) and the custom name for your connector.
