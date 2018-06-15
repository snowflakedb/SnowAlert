Getting Started with SnowAlert
==============================

Requirements
------------

In order to use SnowAlert, you'll need administrator access to a Snowflake warehouse for your data and an AWS account for Lambda.

Downloading
-----------
Use git to clone the project from: https://github.com/snowflakedb/SnowAlert.git

Installer
---------

Snowflake provides an installer in SnowAlert/IaC which will configure your Snowflake environment and AWS resources automatically. The installer has a few prerequisites:
    * Python3
    * AWS CLI
    * Docker
    * Terraform

Before you start the installer, you should verify that the AWS CLI, Docker, and Terraform are all installed and usable. This will involve setting the required variables in SnowAlert/IaC/aws.tf to configure the AWS provider for terraform (see https://www.terraform.io/docs/providers/aws/index.html for details). You should also make sure that you have the credentials for your Snowflake account, for a user with accountadmin privileges. If you are making use of the optional Jira integration, you should also have the Jira environment set up for SnowAlert; this will require having a Jira user for SnowAlert, as well as having a project set up for the alerts to live in. 

Please note that the installer makes use of some shell scripts for helper functionality, and it is not intended to work on Windows machines. Installation on Windows is on the product roadmap; please let us know if you want this feature!

Once those preparations are complete, you can start the installer by typing `python3 install-snowalert.py` into your terminal when you are in the correct directory (SnowAlert/IaC).

You will initially be prompted for your Snowflake account and user credentials; please provide credentials for an account which can assume the accountadmin role in your Snowflake account.

You will also be prompted to provide a password which will be used to encrypt a private key; the installer will use openssl to generate a public and private keypair, and SnowAlert will use that private key to authenticate to SnowAlert. During the installation process, you'll need to provide this password four times: three times to set up the keypair (inital password, verification, and then for setting up the public key), and once more to decrypt the private key for a test authentication of the SnowAlert user after Snowflake has been configured. After configuring and testing the user, the installer will automatically load a sample test query and test suppression into the relevant tables; the sample query will look for users who authenticated to the Snowflake account without MFA, and the suppression targets users who are designated as NoMFAUsers.

Once that test authentication is complete, the installer will ask if you want to integrate Jira with your SnowAlert deployment. If yes, it will prompt you for the Jira username of your SnowAlert user, password, the URL of your Jira deployment, and the project you have configured for alerts.

The installer is configured by default to use prebuilt packages included with the project. If you want to build the packages yourself, then uncomment line 419 in the installer. Note that building the packages can take up to ten minutes!

Once the packages are built, the installer will start using Terraform to create the AWS resources that SnowAlert will need. It will create a KMS key and use that to encrypt the password for the private key, as well as the password for the Jira user if provided; those encrypted values will be stored as environmental variables in the Lambdas that require them. It will also create an IAM role for SnowAlert, along with a policy that gives the lambdas the ability to invoke the runner functions and use the KMS key for decryption. It will also create an S3 bucket used for deploying code to the lambdas, and upload the zipped packages to the S3 bucket. The terraform file also has sample event rules for Cloudwatch written, but commented out; if you want to schedule the lambdas to run, please uncomment those lines in the base-config.tf file and run Terraform again.

If your Snowflake account requires a whitelisted IP for access, you'll need to configure the lambdas to run from a specific IP and whitelist that IP in your Snowflake configuration; this is beyond the scope of the installer.

After configuring the AWS resources, the installer will automatically invoke the Query Wrapper and Suppression Wrapper functions; this should run the sample query that was loaded during Snowflake configuration. Since the SnowAlert user authenticated to Snowflake during configuration and does not have MFA configured, this should result in an alert appearing in your alerts table. If Jira is configured, then the Jira alert handler will run, creating a ticket in the Jira project for the alert. 

Jira Plugin
-----------

SnowAlert supports optional integration with Jira, which will allow it to automatically create tickets in a specified Jira project for alerts that should be investigated. The Jira integration runs in a separate lambda which should run after the Query Wrapper and Suppression Wrapper lambdas have run. 

In order to configure the Jira integration, you will need to provide a user for SnowAlert to authenticate as, as well as a project where the tickets will list. We recommend creating a dedicated user and project.

The Jira Integration Lambda will require the following environment variables to function properly:
    * JIRA_API_USER: The username that SnowAlert will use to authenticate to Jira.
    * SNOWALERT_JIRA_PROJECT: The project name for tickets
    * SNOWALERT_JIRA_URL: The URL of the Jira deployment
    * JIRA_API_PASSWORD: The password for the Jira user that SnowAlert uses. This password should be encrypted with a KMS key that the lambda has access to.
    * private_key_password: The password that encrypts the private key used for key-pair authentication. This password should be encrypted with a KMS key that the lambda has access to.
    * private_key: the encrypted private key for key-pair authentication
    * SNOWALERT_USER: The name of the SnowAlert user in SnowFlake. This should be something like "snowalert"
    * PROD_FLAG: This should be "True". This flag is used for debugging purposes; if it is not set, tickets will not be created and KMS will not be used to decrypt passwords.


Setting Up
-----------
If you want to configure SnowAlert manually without using the installer, follow the steps below to set up Snowflake and AWS Lambda for SnowAlert.

1. Prepare authentication key
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


2. Configure your Snowflake warehouse
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The SQL commands below will help you configure your Snowflake environment for SnowAlert. Below is a script that can be copied and pasted into the Snowflake web UI. After copying the script into Snowflake, highlight the entire script and press "Command+Enter" to run each of the commands in sequence.

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
    create role if not exists snowalert;

    -- grant snowalert access to warehouse
    grant all privileges
    on warehouse snowalert 
    to role snowalert;

    -- grant privileges on schemas
    grant all privileges on all schemas in database snowalert to role snowalert;
    grant usage on warehouse snowalert to role snowalert

    -- create a user for snowalert
    create user if not exists snowalert;
    alter user snowalert set
    default_role = snowalert
    default_warehouse = snowalert;
    alter user snowalert set rsa_public_key='<pubkey>'
    grant role snowalert to user snowalert;


    -- grant snowalert access to database
    grant all privileges
    on database snowalert
    to role snowalert;

    -- create table for alerts
    create table alerts (
        alert variant,
        ticket string,
        suppressed bool,
        suppression_rule string default null,
        counter integer default 1
    );
    grant all privileges on table alerts to role snowalert;

    -- create table for queries
    create table snowalert_queries (
        query_spec variant
    );
    grant all privileges on table snowalert_queries to role snowalert;

    --create table for suppressions
    create table suppression_queries (
        suppression_spec variant
    );
    grant all privileges on table suppression_queries to role snowalert;


3. Set up AWS Lambda to run SnowAlert
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Please note that these instructions require some familiarity with configuring and using some AWS resources, including:
    * S3 Buckets
    * IAM Roles and Policies
    * Lambda functions
    * KMS

SnowAlert used five lambda functions for basic functionality. If you want to modify the code in these lambdas and deploy new versions, an update script is provided to streamline the experience. You can invoke update-snowalert.sh with the name of the python file you want to package, and the aws cli profile you want to use to upload the package to S3. If you want to build and upload all five python files, then run `update-snowalert.sh all <profile>`.

The update-snowalert.sh script will start a Docker container that will pip install the required plugins and package everything together into zip files, then upload the zip files to an S3 bucket defined by the environmental variable LAMBDA_DEPLOYMENT_BUCKET and update the relevant lambda functions.

* Query Wrapper
    * This lambda function should run the query_wrapper.py code. This lambda is responsible for dispatching queries to the Query Runner.
    * This lambda should run once per hour at the start of the hour.
    * This lambda requires the following environment variables to be configured:
        * SNOWALERT_QUERY_EXECUTOR_FUNCTION: The name of the lambda function that executes query_runner.py
        * private_key_password: The KMS-encrypted password for the private key associated with the SnowAlert user
        * private_key: The base64-encoded private key associated with the Snowflake user
        * account: The Snowflake account where SnowAlert is deployed

* Query Executor
    * This lambda function should run the query_runner.py code. This lambda is responsible for executing queries against data in Snowflake and generating alerts based on the results of those queries.
    * This lambda does not need to be scheduled on its own; it will get run by the Query Wrapper.
    * This lambda requires the following environment variables to be configured:
        * private_key_password: The KMS-encrypted password for the private key associated with the SnowAlert user
        * private_key: The base64-encoded private key associated with the Snowflake user
        * account: The Snowflake account where SnowAlert is deployed

* Suppression Wrapper
    * This lambda function should run the suppression_wrapper.py code. This lambda is responsible for dispatching queries to the Suppression Runner, as well as flagging alerts as unsuppressed.
    * This lambda should run once per hour after the Query Executor has finished running queries. Run this lambda even if you have no suppressions configured.
    * This lambda requires the following environment variables to be configured:
        * SNOWALERT_SUPPRESSION_EXECUTOR_FUNCTION: The name of the lambda function that executes suppression_runner.py
        * private_key_password: The KMS-encrypted password for the private key associated with the SnowAlert user
        * private_key: The base64-encoded private key associated with the Snowflake user
        * account: The Snowflake account where SnowAlert is deployed

* Suppression Runner
    * This lambda function should run the suppression_runner.py code. This lambda is responsible for executing suppression queries against unchecked alerts in the alerts table, and flagging alerts which should be suppressed. 
    * This lambda does not need to be scheduled on its own; it will get run by the Suppression Wrapper.
    * This lambda requires the following environment variables to be configured:
        * private_key_password: The KMS-encrypted password for the private key associated with the SnowAlert user
        * private_key: The base64-encoded private key associated with the Snowflake user
        * account: The Snowflake account where SnowAlert is deployed

* Alert Handler
    * The Alert Handler is the function which handles the integration with a task management system. Right now, the only supported integration is Jira; please see the Jira Plugin documentation for details on that integration.

Queries and suppressions can be managed manually by inserting the query spec or suppression spec into the appropriate table, but it is easier to manage them as configuration files. ``query.tf`` and ``suppression.tf`` are sample files; you can use the ``query_helper.go`` and ``suppression_helper.go`` files to manage your queries along with those files. 

``query_helper.go`` and ``suppression_helper.go`` must be compiled to binaries in order to be used. They have the following dependencies:
    * 'github.com/hashicorp/hcl'
    * 'github.com/google/go-cmp/cmp'
    * 'github.com/snowflakedb/gosnowflake'

With those dependencies installed, you can compile the binaries with ``go build query_helper.go`` and ``go build suppression_helper.go``. Invoking the binaries with no arguments will print usage instructions. Run ``./query_helper [snowflake username] sample-query.tf`` to insert the sample query spec into your snowalert_queries table.


Testing
-------
After deployment is completed, log into Snowflake without using MFA. This should trigger the test alert which looks for user logins to Snowflake where MFA is not used. If you don't want to wait for the next scheduled run, use AWS's Lambda Test button on the Query Wrapper function.

If you see a new alert created in the alerts table, you have successfully deployed SnowAlert.

Any issues? Reach out to us at snowalert@snowflake.net.
