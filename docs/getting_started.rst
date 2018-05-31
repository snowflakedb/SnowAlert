Getting Started with SnowAlert
==============================

Requirements
------------

In order to use SnowAlert, you'll need administrator access to a Snowflake warehouse for your data and an AWS account for Lambda.

Downloading
-----------
Use git to clone the project from: https://github.com/snowflakedb/SnowAlert.git

Setting Up
-----------
Follow the steps below to set up Snowflake and AWS Lambda for SnowAlert.

1. Configure your Snowflake warehouse
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The SQL commands below will help you configure your Snowflake environment for SnowAlert.
You'll need to replace the placeholders with an appropriate user, database, and warehouse for your SnowAlert deployment. We recommend using a dedicated "snowalert" user.

.. code-block:: sql

    -- change role to SYSADMIN for warehouse / database steps
    use role SYSADMIN;

    -- create a warehouse for snowalert
    create warehouse if not exists snowalert
    warehouse_size = xsmall
    warehouse_type = standard
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true;

    -- create database for snowalert
    create database if not exists snowalert;

    -- change role to ACCOUNTADMIN for user / role steps
    use role ACCOUNTADMIN;

    -- create role for SnowAlert
    create role if not exists snowalert_role;
    grant role snowalert_role to role SYSADMIN;

    -- grant snowalert access to warehouse
    grant all privileges
    on warehouse snowalert 
    to role snowalert_role;

    -- create a user for snowalert
    create user if not exists snowalert;
    alter user snowalert set
    default_role = snowalert_role
    default_warehouse = snowalert;
    alter user snowalert set rsa_public_key='<pubkey>'
    grant role snowalert_role to user snowalert;


    -- grant snowalert access to database
    grant all privileges
    on database snowalert
    to role snowalert_role;

    -- create table for alerts
    create table alerts (
        alert variant,
        ticket string,
        suppressed bool,
        suppression_rule string default null,
        counter integer default 1
    );
    grant all privileges on table alerts to role snowalert_role;

    -- create table for queries
    create table snowalert_queries (
        query_spec variant
    );
    grant all privileges on table snowalert_queries to role snowalert_role;

    --create table for suppressions
    create table suppression_queries (
        suppression_spec variant
    );
    grant all privileges on table suppression_queries to role snowalert_role;


2. Prepare authentication key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Snowflake recommends using keypair-based authentication for programmatic access to a Snowflake account. This involves creating a public and private keypair like so:

.. code-block:: bash

    $ openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
    $ openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

Then associate that keypair with a Snowflake user using the ALTER USER command in the Snowflake warehouse:

.. code-block:: sql

    alter user snowalert set rsa_public_key='<PUBLIC KEY>';

More details can be found at https://docs.snowflake.net/manuals/user-guide/snowsql-start.html#using-key-pair-authentication

If you intend to use Snowpipe to automatically ingest data from S3 into Snowflake, then follow the instructions at https://docs.snowflake.net/manuals/user-guide/data-load-snowpipe.html for configuration help.

3. Set up AWS Lambda to run SnowAlert
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
SnowAlert used five lambda functions for basic functionality. 

* Query Wrapper
    * This lambda function should run the query_wrapper.py code. This lambda is responsible for dispatching queries to the Query Runner.
    * This lambda should run once per hour at the start of the hour.
    * This lambda requires the following environment variables to be configured:
        * SNOWALERT_QUERY_EXECUTOR_FUNCTION: The name of the lambda function that executes query_runner.py
        * auth: The KMS-encrypted password for the SnowAlert user
        * SNOWALERT_ACCOUNT: The Snowflake account where SnowAlert is deployed

* Query Executor
    * This lambda function should run the query_runner.py code. This lambda is responsible for executing queries against data in Snowflake and generating alerts based on the results of those queries.
    * This lambda does not need to be scheduled on its own; it will get run by the Query Wrapper.
    * This lambda requires the following environment variables to be configured:
        * auth: The KMS-encrypted password for the SnowAlert user
        * SNOWALERT_ACCOUNT: The Snowflake account where SnowAlert is deployed

* Suppression Wrapper
    * This lambda function should run the suppression_wrapper.py code. This lambda is responsible for dispatching queries to the Suppression Runner, as well as flagging alerts as unsuppressed.
    * This lambda should run once per hour after the Query Executor has finished running queries. Run this lambda even if you have no suppressions configured.
    * This lambda requires the following environment variables to be configured:
        * SNOWALERT_SUPPRESSION_EXECUTOR_FUNCTION: The name of the lambda function that executes suppression_runner.py
        * auth: The KMS-encrypted password for the SnowAlert user
        * SNOWALERT_ACCOUNT: The Snowflake account where SnowAlert is deployed

* Suppression Runner
    * This lambda function should run the suppression_runner.py code. This lambda is responsible for executing suppression queries against unchecked alerts in the alerts table, and flagging alerts which should be suppressed. 
    * This lambda does not need to be scheduled on its own; it will get run by the Suppression Wrapper.
    * This lambda requires the following environment variables to be configured:
        * auth: The KMS-encrypted password for the SnowAlert user
        * SNOWALERT_ACCOUNT: The Snowflake account where SnowAlert is deployed

* Alert Handler
    * This lambda function should run the alert_handler.py code (which itself requires the code in /plugins/create_jira.py to function). This lambda is responsible for looking through the alerts table in Snowflake and creating Jira tickets for unsuppressed alerts. 
    * This lambda should run once per hour, after alerts have been suppressed.
    * This lambda requires the following environment variables to be configured:
        * PROD_FLAG: Set this to indicate that the environment is production
        * SNOWALERT_PASSWORD: The KMS-encrypted password for the SnowAlert user
        * SNOWALERT_ACCOUNT: The Snowflake account where SnowAlert is deployed

Queries and suppressions can be managed manually by inserting the query spec or suppression spec into the appropriate table, but it is easier to manage them as configuration files. ``query.tf`` and ``suppression.tf`` are sample files; you can use the ``query_helper.go`` and ``suppression_helper.go`` files to manage your queries along with those files. 

``query_helper.go`` and ``suppression_helper.go`` must be compiled to binaries in order to be used. They have the following dependencies:
    * 'github.com/hashicorp/hcl'
    * 'github.com/google/go-cmp/cmp'
    * 'github.com/snowflakedb/gosnowflake'

With those dependencies installed, you can compile the binaries with ``go build query_helper.goi`` and ``go build suppression_helper.go``. Invoking the binaries with no arguments will print usage instructions.


Testing
-------
After deployment is completed, run the command ``GRANT FOO`` in the Snowflake UI. This should trigger the test alert which looks for GRANT and REVOKE commands in your command history. If you don't want to wait for the next scheduled run, use AWS's Lambda Test button on the Query Wrapper function.

If you see a new alert created in the alerts table, you have successfully deployed SnowAlert.

Any issues? Reach out to us at snowalert@snowflake.net.
