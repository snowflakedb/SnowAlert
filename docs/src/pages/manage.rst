Managing Alerts
***************

SnowAlert Rules are a set of views defined in your Snowflake account that manage the lifecycle of Alerts. Query Rules create alerts and Suppression Rules automatically handle alerts. The containers published `on DockerHub`_ use these rules to create alerts. Since these rules are Snowflake views, they can be managed using snowsql command line client or your Snowflake Web UI.

We are also actively working on a ``snowsec/snowalert-webui`` service container which will help you manage your rules in SnowAlert.

Query Rules
===========

Before creating new alerts, take a look at the `Snowflake Query Pack`_ to see examples of useful Queries.

Note that each Query Rule defines a set of columns, of which some are static, e.g. ``'Medium' AS severity`` and ``'SnowAlert' AS detector``, while others are based on the data queries, such as ``user_name AS actor`` and ``start_time AS event_time``. Together, these define alerts that are saved in the ``results`` schema in your Snowflake account.


Creating New Alert Queries
==========================

Since a Query Rule is a view over data in Snowflake, they can be managed by writing SQL statements to create those views and grant the SnowAlert role ``SELECT`` privileges on them. Those statements can be saved in a .sql file and either pasted into the SnowFlake worksheet UI or executed via snowsql. A single .sql file can contain multiple rules.


Viewing Alert Results
=====================

When alert queries return data from your warehouse, the results will be added to the alerts table in your SnowAlert database. You can select them in the database to confirm that they are being generated as expected.

If you've configured the alert handlers to notify you on alerts, e.g. through Jira, you can expect to see those notifications as soon as the Dispatch step is done.


Adding Suppressions
===================

SnowAlert supports Suppression Queries to prevent known behavior from being dispatched for review. Suppression Queries are configured similarly to Alert Queries, but end with ``_ALERT_SUPPRESSION``. Suppressions can suppress Alerts from multiple Queries or from a specific query, with a suppression for, e.g. ``AWS_ACCESS_DENIED_ALERT_QUERY``, being called ``AWS_ACCESS_DENIED_ALERT_SUPPRESSION``.

Suppressions are views over the Alerts table, like Queries are views over data, and so can be managed in the same way: by writing .sql files with statements to create the view and make the appropriate grants.

When the suppression step runs, it marks Alerts as suppressed or not. Alerts that have been through the suppression step and were not suppressed are then dispatched to handlers (e.g. Jira, Slack) by the Alert Dispatcher.


SnowAlert Query Packs
=====================

SnowAlert is shipped with some sample queries, categorized by the type of monitoring it provides and grouped into query packs.

To enable a query pack, copy the query pack file from packs/ into the Snowflake Worksheet UI and run the SQL statements to create the appropriate views and enable the appropriate grants.

The current query packs and queries are documented below:

- Snowflake Query Pack

	#. Snowflake Admin Role Grants - This query generates an alert whenever a new user or role is granted the SECURITYADMIN or ACCOUNTADMIN role.
	#. Snowflake Authorization Failures - This query generates an alert whenever a user runs a query that returns an authorization error, indicating that the user does not have the appropriate grants to query the data.
	#. Snowflake Authentication Failures - This query generates an alert whenever a user fails to authenticate with Snowflake, indicating misconfigured scripts attempting to authenticate to your Snowflake account or potentially malicious brute force attempts.


Note: Using the Snowflake query pack will require you to grant ``imported privileges`` on the ``snowflake`` database to the SnowAlert role in your Snowflake account.

SnowAlert Violations
====================

Sometimes there are events you want to track and resolve, but which don't require immediate action; for example, if you want all of your Snowflake users to require MFA before authenticating, you want to know about a user who doesn't have that turned on, but it's not something that requires your Security team to track down the offending user that instant.

For cases like this, SnowAlert can manage violations, which are similar to alerts but can run less often (for example. daily instead of hourly). Violation queries are managed and suppressed identically to to Alert queries (so you might have ``USER_NO_MFA_VIOLATION_QUERY`` and ``USER_NO_MFA_VIOLATION_SUPPRESSION``), but the results are stored in a Violations table, which you can visualize and process using a BI tool like Sigma, Looker, or Superset.

.. _on DockerHub: https://hub.docker.com/r/snowsec/snowalert
.. _Snowflake Query Pack: https://github.com/snowflakedb/SnowAlert/blob/master/packs/snowflake_query_pack.sql
