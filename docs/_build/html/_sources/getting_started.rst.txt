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
    create warehouse if not exists security_warehouse
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
    on warehouse security_warehouse
    to role snowalert_role;

    -- create a user for snowalert
    create user if not exists snowalert;
    alter user snowalert set
    default_role = snowalert_role
    default_warehouse = security_warehouse;
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
Run the provided lambda_setup.sh script to add the Lambda functions that will run the SnowAlert code against the data in Snowflake.

Testing
-------
After deployment is completed, run the command ``GRANT FOO`` in the Snowflake UI. This should trigger the test alert which looks for GRANT and REVOKE commands in your command history. If you don't want to wait for the next scheduled run, use AWS's Lambda Test button on the Query Wrapper function.

If you see a new alert created in the alerts table, you have successfully deployed SnowAlert.

Any issues? Reach out to us at snowalert@snowflake.net.