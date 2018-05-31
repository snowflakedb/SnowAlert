SnowAlert - Snowflake Security Analytics
****************************************


SnowAlert is a security analytics framework that uses the Snowflake data warehouse for identifying security incidents across diverse data sources and time ranges.


Overview
========

As Snowflake's security team, we need to know quickly and reliably when we're under attack. We use the Snowflake data warehouse to store system, network and application logs and we analyze those logs for indications of active threats.

Some very large enterprise teams have developed in-house security analytics solutions on top of Snowflake but we want to make it easier for any team to use Snowflake for its security analytics. That's where SnowAlert comes in.

SnowAlert is how we run scheduled queries on machine data in Snowflake and use the results to drive action at the security team. We built it to be easy to use and extend, without any servers required and Snowflake as the only data store.

How It Works
=============

SnowAlert queries Snowflake data from AWS Lambda. We chose Lambda because we love AWS and it runs our Python without needing servers, but the same code should work on Azure Functions as well.

The queries that drive the alerts are all defined in JSON format within your deployment's configuration files. The framework includes a loader script to store and update queries in Snowflake, see :ref:`Managing Alerts <managing-alerts>` for more info.

Alerts are generated when queries match data in your warehouse, and they are saved to Snowflake as well. We use another Lambda function to create incident tickets based on alerts but the same code can be extended to message or page your incident responders.