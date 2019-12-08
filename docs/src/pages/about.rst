About SnowAlert
***************

SnowAlert is a security analytics framework that uses the Snowflake Cloud Data Platform to identify security incidents across diverse data sources and time ranges.


Overview
========

As Snowflake's security team, we need to know quickly and reliably when we're under attack. We use the Snowflake Cloud Data Platform to store system, network and application logs and we analyze those logs for indications of active threats.

Some very large enterprise teams have developed in-house security analytics solutions on top of Snowflake but we want to make it easier for any team to use Snowflake for their security analytics. That's where SnowAlert comes in.

SnowAlert is how we run scheduled queries on machine data in Snowflake and use the results to drive action on the part of the security team. We built it to be easy to use and extend, without any servers required and Snowflake as
the only data store.


How It Works
=============

SnowAlert queries Snowflake data on a regular schedule. We provide a Docker container and orchestration templates, so that you can deploy it anywhere: on your laptop or on a server, or orchestrated with AWS Fargate or Google Kubernetes.

The queries that drive the alerts are all defined as views that get stored in Snowflake; if you can write SQL, you can write queries for SnowAlert. We also provide some prebuilt packs of queries to help you get started and to serve as a model for what informative alerts should look like.

Alerts are generated when Query Rules match Data in your database and are saved into Snowflake. There, they are de-duplicated, passed through Suppression Rules for sorting into expected and unexpected behavior, correlated by action or object, and dispatched to Handlers (e.g. Slack, Jira) for review by users or analysts, who then circle back to correct Data (e.g. update vulnerable software) or refine the Rules (e.g. whitelist access of a certaint type) that power SnowAlert.

SnowAlert can also manage Violations, which are like alerts that require a less immediate response: a server not following best practices, a user without MFA enabled, and so on. These gets stored in a table similar to alerts, but are intended to be visualized with a tool like Sigma or Looker for ease of processing.
