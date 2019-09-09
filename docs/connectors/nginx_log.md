## NGINX Log Connector

In order to use the NGINX Log Connector, you need to first ingest your NGINX logs to an Amazon S3 bucket, e.g. using Fluentd. The Connector expects two types of logs to be collected: NGINX Access Logs and NGINX Error Logs. Both of these log types should be collected to two side-by-side folders, named "access" and "error", in one bucket (they may be in a sub-folder).

For example, if you have an S3 bucket called 'operational-data', you might have a folder inside that bucket called "nginx" which contains two folders "access" and "error". In this case, the bucket you would specify for the connector would be "operational-data", and the prefix would be "nginx/".
