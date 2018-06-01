.. _managing-alerts:

Managing Alerts
***************

Alerts triggered when an alert query matches data in your database. These queries are defined in configuration files and need to be loaded into Snowflake before they can be used. Luckily, the framework includes helper scripts to streamline the process.

Alert Configuration Files
=========================

Before creating new alerts, take a look at the alert configuration file ``test.tf`` where a sample alert query is defined.

Each alert query has a set of fields that can be assigned static values or dynamic values that will contain data from the query result. The test query can be used to verify that the SnowAlert deployment was successful.

Creating New Alert Queries
==========================

To add new alert queries, create additional definition files in the alert query directory. Each definition file can contain multiple alert definitions, and all will be executed once they are imported.

Import the alert queries into the SnowAlert database using the ``query_helper.go`` tool. Once imported, the new alerts will run at the set schedule. Note that ``query_helper.go`` will require the following environment variables to be set:
    * SNOWALERT_ACCOUNT: The Snowflake account name where SnowAlert is deployed
    * UPDATE_WAREHOUSE: The warehouse you want to use to update queries in SnowAlert. 
    * UPDATE_ROLE: The role that you want to use while updating queries in SnowAlert.

Viewing Alert Results
=====================

When alert queries return data from your warehouse, the results will be added to the alerts table in your SnowAlert database. You can select them in the database to confirm that they are being generated as expected.

If you've configured alert handlers to notify you on alerts, for example through JIRA, you can expect to see those notifications as soon as the handler Lambda function runs.

Adding Suppressions
===================

SnowAlert supports suppression queries to prevent false positives from creating alerts. Suppression queries are configured similarly to alert queries, within tf files in the alerts/suppressions folder. Note that ``suppression_helper.go`` will require the same environment variables as ``query_helper``.

When the corresponding Lambda function runs, it marks new alerts as suppressed or not. Only alerts that have been assessed by the suppression function are then processed by the alert handler.

The suppression_wrapper.py function is what flags alerts as unsuppressed; you should ensure that suppression_wrapper.py runs even if there are no suppression queries.
