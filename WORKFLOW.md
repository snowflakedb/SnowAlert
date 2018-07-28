# Setup

In order to use SnowAlert, you need two things: a Snowflake account to store data, and an AWS account to deploy lambda functions. 

## Snowflake Configuration

Snowflake recommends using keypair-based authentication for programmatic access to a Snowflake account. This involves creating a public and private keypair like so:

~~~~
$ openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
$ openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
~~~~

And associating that keypair with a snowflake user using ALTER USER:

`alter user <USER> set rsa_public_key='<PUBLIC KEY>';`

More details can be found at https://docs.snowflake.net/manuals/user-guide/snowsql-start.html#using-key-pair-authentication

The script at the bottom will help you configure your Snowflake environment; choose appropriate user, warehouse, and database names as required.

If you intend to use Snowpipe to automatically ingest data from S3 into Snowflake, then follow the instructions at
    https://docs.snowflake.net/manuals/user-guide/data-load-snowpipe.html
for configuration help.

~~~~
-- change role to ACCOUNTADMIN for user / role steps
use role ACCOUNTADMIN;

-- create role for SnowAlert 
create role if not exists snowalert_role;
grant role snowalert_role to role SYSADMIN;

-- create a user for snowalert 
create user if not exists <USERNAME>;
alter user <USERNAME> set
default_role = snowalert_role
default_warehouse = <WAREHOUSE>;
alter user <USERNAME> set rsa_public_key='<pubkey>'
grant role snowalert_role to user <USERNAME>;

-- change role to SYSADMIN for warehouse / database steps
use role SYSADMIN;

-- create a warehouse for snowalert
create warehouse if not exists <WAREHOUSE>
warehouse_size = xsmall
warehouse_type = standard
auto_suspend = 60
auto_resume = true
initially_suspended = true;

-- grant snowalert access to warehouse
grant all privileges
on warehouse <WAREHOUSE>
to role snowalert_role;

-- create database for snowalert 
create database if not exists <DATABASE>;

-- grant snowalert access to database
grant all privileges
on database <DATABASE>
to role snowalert_role;

-- create table for alerts
create table alerts (
    alert variant
);
grant all privileges on table alerts to role snowalert_role;
~~~~

# Data Sources

SnowAlert can use several different sources of data to generate alerts, including CloudTrail, Jamf, and Osquery. Each data source dumps data into S3, where Snowpipe sees it and automatically ingests it into Snowflake. Some data sources, like CloudTrail, can automatically log data into S3 by default. Other sources, such as Jamf, require a lambda function to make use of an API to get data, which can then be saved in S3.

Once data is saved in S3 and transferred to Snowflake via Snowpipe, it is ready to be queried. Lambda functions can use the Snowflake Connector to connect to Snowflake and run queries on the data; the results of those queries can then be parsed and transformed into alerts, which are then inserted into the SnowAlert alerts table. 

A final lambda function checks the alerts table for alerts which have not been processed, and processes them by creating Jira tickets and associating those tickets with the respective alerts. 

For Jamf, the flow looks like this:

    * collect_jamf.py [Runs once per day, queries the Jamf API and saves json to S3]
    * query_jamf.py [Runs once per day, queries recent Jamf data in Snowflake to generate alerts and insert them into the alerts table]
    * alert_handler.py [Runs once per hour, checks contents of alerts table and creates jira tickets for new alerts]

SnowAlert uses AWS Lambda functions to achieve a serverless architecture. In order to manage the various resources required for such an architecture, we use Terraform to represent our infrastructure as code. Boilerplate Terraform files will be shared in the future. Information on Terraform and how to use it can be found at https://www.terraform.io.

While Terraform manages the resources, it can't handle the creation of deployment packages for lambda functions. Most lambda functions for SnowAlert require interacting with Snowflake in some way; in python, this can be done using the Python Connecter (pip install snowflake-python-connector). Documentation for the python connector can be found at https://docs.snowflake.net/manuals/user-guide/python-connector.html. Instructions for building a deployment package that includes the python connector in Lambda can be found in lambda_build_instructions.txt
