## NGINX Log configuration

In order to use the NGINX Log connector, you must be ingesting your NGINX logs to an Amazon S3 bucket using something like Fluentd. The connector expects two types of logs to be collected: NGINX Access Logs and NGINX Error Logs. Both of these log types should be collected to the same bucket, but separated within folders inside of the bucket. Specifically, the Access logs should be collected to a folder named 'access', and the error logs should be collected to a folder named 'error'.

For example, if you have an S3 bucket called 'operational-data', you might have a folder inside that bucket called 'nginx', which itself contains two folders, 'access' and 'error'. In this case, the bucket you would specify for the connector would be 'operational-data', and the prefix would be 'nginx/'.
