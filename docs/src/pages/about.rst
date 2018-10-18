About SnowAlert
***************

SnowAlert is a security analytics framework that uses the Snowflake data warehouse for identifying security incidents across diverse data sources and time ranges.


Overview
========

As Snowflake's security team, we need to know quickly and reliably when we're under attack. We use the Snowflake data warehouse to store system, network and application logs and we analyze those logs for indications of active threats.

Some very large enterprise teams have developed in-house security analytics solutions on top of Snowflake but we want to make it easier for any team to use Snowflake for their security analytics. That's where SnowAlert comes in.

SnowAlert is how we run scheduled queries on machine data in Snowflake and use the results to drive action on the part of the security team. We built it to be easy to use and extend, without any servers required and Snowflake as
the only data store.


How It Works
=============

SnowAlert queries Snowflake data on a regular schedule. We provide a Docker container so you can deploy it anywhere: on a server in the closet, with AWS Fargate, or even on your laptop.

The queries that drive the alerts are all defined as views that get stored in Snowflake; if you can write SQL, you can write queries for SnowAlert. We also provide some prebuilt packs of queries to help you get started and to serve as a model for what informative alerts should look like.

Alerts are generated when queries match data in your warehouse, and they are saved to Snowflake as well. We use a Python script to create incident tickets from alerts but the same code can be extended to message or page your incident responders.

SnowAlert can also manage Violations, which are like alerts that require a less immediate response: a server not following best practices, a user without MFA enabled, and so on. These gets stored in a table similar to alerts, but are intended to be visualized with a tool like Sigma or Looker for ease of processing.
