.. _managing-alerts:

Managing Alerts
***************

Alerts triggered when an alert query matches data in your database. These queries are defined in configuration files and need to be loaded into Snowflake before they can be used. Luckily, the framework includes helper scripts to streamline the process.

Alert Configuration Files
=========================

Before creating new alerts, take a look at the alert configuration file ``QaC/queries/sample_query.qs`` where a sample alert query is defined.

Each alert query has a set of fields that can be assigned static values or dynamic values that will contain data from the query result. The test query can be used to verify that the SnowAlert deployment was successful.

Query Helper and Suppression Helper
===================================

The query helper and suppression helper tools are created to assist with loading alerts and suppressions into SnowAlert. These tools will display the queries and suppressions you're trying to add, remove or change by modifying the files in the query directory.

``query_helper.go`` and ``suppression_helper.go`` must be compiled to binaries in order to be used. They have the following dependencies:
    * 'github.com/hashicorp/hcl'
    * 'github.com/google/go-cmp/cmp'
    * 'github.com/snowflakedb/gosnowflake'

The dependencies can be installed using ``go get``. Simply add the dependency URL at the end, ``go get github.com/hashicorp/hcl`` like so.

With those dependencies installed, you can compile the binaries with ``go build -o QaC/queries/query_helper query_helper.go`` and ``go build -o QaC/suppressions/suppression_helper suppression_helper.go``. 

The binaries will pick up all configuration files (files ending with ``.qs``) in the same directory as the binary (if you followed the above ``go build`` command, it'll be the files in the QaC/queries and QaC/suppressions directories respectively). 

Note that both of these tools require the following environment variables to be set:
    * SNOWALERT_ACCOUNT: The Snowflake account name where SnowAlert is deployed
    * UPDATE_WAREHOUSE: The warehouse you want to use to update queries in SnowAlert. 
    * UPDATE_ROLE: The role that you want to use while updating queries in SnowAlert.
    * UPDATE_USER: The user that you want to use while updating queries in SnowAlert.

Creating New Alert Queries
==========================

To add new alert queries, create additional definition files in the alert query directory, ``QaC/queries``. Each definition file can contain multiple alert definitions, and all will be executed once they are imported.

After creating the alert definitions and saving them to the query directory, import the alert queries into the SnowAlert database using the ``query_helper`` tool. Once imported, the new alerts will run at the set schedule. 

Viewing Alert Results
=====================

When alert queries return data from your warehouse, the results will be added to the alerts table in your SnowAlert database. You can select them in the database to confirm that they are being generated as expected.

If you've configured alert handlers to notify you on alerts, for example through JIRA, you can expect to see those notifications as soon as the handler Lambda function runs.

Adding Suppressions
===================

SnowAlert supports suppression queries to prevent false positives from creating alerts. Suppression queries are configured similarly to alert queries, within tf files in the alerts/suppressions folder. 

When the corresponding Lambda function runs, it marks new alerts as suppressed or not. Only alerts that have been assessed by the suppression function are then processed by the alert handler.

The suppression_wrapper.py function is what flags alerts as unsuppressed; you should ensure that suppression_wrapper.py runs even if there are no suppression queries.

SnowAlert Query Packs
=================

SnowAlert is shipped with some sample queries, categorized by the type of monitoring it provides and grouped into query packs.

To enable a query pack, copy the query pack file from packs/ to QaC/queries and load them using the query_helper tool.

The current query packs and the queries are documented below:

- Snowflake Query Pack

	#. Snowflake Admin Role Grants - This query generates an alert whenever a new user or role is granted the SECURITYADMIN or ACCOUNTADMIN role.
	#. Snowflake Authorization Failures - This query generates an alert whenever a user runs a query that returns an authorization error, indicating that the user does not have the appropriate grants to query the data.
	#. Snowflake Authentication Failures - This query generates an alert whenever a user fails to authenticate with Snowflake, indicating misconfigured scripts attempting to authenticate to your Snowflake account or potentially malicious brute force attempts.

As of now, the Snowflake Query Pack is preincluded in the QaC/queries folder, so you only need to load the queries in using the ``query_helper`` tool.

Note: Using the Snowflake query pack will require you to grant ``imported privileges`` on the ``snowflake`` database to the SnowAlert role in your Snowflake account.