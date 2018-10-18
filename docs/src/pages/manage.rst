Managing Alerts
***************

An Alert is created when a query matches data in your database. The queries are defined in a series of views defined
in ``SNOWALERT.RULES`` ending in ``ALERT_QUERY``.


Alert Configuration Files
=========================

Before creating new alerts, take a look at the alert configuration file ``sample_rules/sample_query.sql`` where a sample
alert query is defined.

Each alert query has a set of fields that can be assigned static values or dynamic values that will contain data from
the query result. The test query can be used to verify that the SnowAlert deployment was successful.


Creating New Alert Queries
==========================

A query is simply a view over data in Snowflake. As such, queries can be managed by writing SQL statements to create
those views and grant the SnowAlert user `SELECT` privileges on them. Those statements can be saved in a .sql file
and either pasted into the SnowFlake worksheet UI or executed via snowsql. A single .sql file can contain multiple rules.


Viewing Alert Results
=====================

When alert queries return data from your warehouse, the results will be added to the alerts table in your SnowAlert
database. You can select them in the database to confirm that they are being generated as expected.

If you've configured the alert handler to notify you on alerts, for example through JIRA, you can expect to see those
notifications as soon as the container finishes executing all three Python scripts.


Adding Suppressions
===================

SnowAlert supports suppression queries to prevent false positives from creating alerts. Suppression queries are
configured similarly to alert queries, within .sql files in the alerts/suppressions folder. Suppressions should be
targeted for specific queries, and a suppression for ``AWS_ACCESS_DENIED_ALERT_QUERY`` should be called
``AWS_ACCESS_DENIED_ALERT_SUPPRESSION``.

Suppressions are views over the Alerts table, just like how queries are views over log data, and can be managed
in the same way as queries: by writing .sql files with statements to create the view and make the appropriate grants.

When the suppression function runs, it marks new alerts as suppressed or not. Only alerts that have been
assessed by the suppression function are then processed by the alert handler.

The suppression_wrapper.py function is what flags alerts as unsuppressed; you should ensure that
suppression_wrapper.py runs even if there are no suppression queries.


SnowAlert Query Packs
=====================

SnowAlert is shipped with some sample queries, categorized by the type of monitoring it provides and grouped into
query packs.

To enable a query pack, copy the query pack file from packs/ into the Snowflake Worksheet UI and run the SQL statements
to create the appropriate views and enable the appropriate grants.

The current query packs and the queries are documented below:

- Snowflake Query Pack

	#. Snowflake Admin Role Grants - This query generates an alert whenever a new user or role is granted the SECURITYADMIN or ACCOUNTADMIN role.
	#. Snowflake Authorization Failures - This query generates an alert whenever a user runs a query that returns an authorization error, indicating that the user does not have the appropriate grants to query the data.
	#. Snowflake Authentication Failures - This query generates an alert whenever a user fails to authenticate with Snowflake, indicating misconfigured scripts attempting to authenticate to your Snowflake account or potentially malicious brute force attempts.


Note: Using the Snowflake query pack will require you to grant ``imported privileges`` on the ``snowflake`` database to the SnowAlert role in your Snowflake account.

SnowAlert Violations
====================

Sometimes there are events you want to track and resolve, but which don't require immediate action; for example, if you want all
of your Snowflake users to require MFA before authenticating, you want to know about a user who doesn't have that turned on,
but it's not something that requires your Security team to track down the offending user that instant.

For cases like this, SnowAlert can manage violations, which are similar to alerts but can run less often (for example. daily
instead of hourly). Violation queries are managed and suppressed identically to to Alert queries (so you might have
``USER_NO_MFA_VIOLATION_QUERY`` and ``USER_NO_MFA_VIOLATION_SUPPRESSION``), but the results are stored in a Violations table,
which you can visualize and process using a BI tool like Sigma, Looker, or Superset.
