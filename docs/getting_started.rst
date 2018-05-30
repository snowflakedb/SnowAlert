Getting Started with SnowAlert
==============================

Requirements
------------

In order to use SnowAlert, you'll need a Snowflake warehouse for your data and an AWS account for Lambda.

Downloading
-----------
TBD

Setting Up
-----------
Follow the steps below to set up Snowflake and AWS Lambda for SnowAlert.

1. Create Snowflake credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Snowflake recommends using keypair-based authentication for programmatic access to a Snowflake account. This involves creating a public and private keypair like so:

.. code-block:: bash

::
    $ openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
    $ openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

.. code-block:: sql

::
    alter user <USER> set rsa_public_key='<PUBLIC KEY>';

.. highlight:: none

And associating that keypair with a snowflake user using ALTER USER:

`alter user <USER> set rsa_public_key='<PUBLIC KEY';`

More details can be found at https://docs.snowflake.net/manuals/user-guide/snowsql-start.html#using-key-pair-authentication

The script at the bottom will help you configure your Snowflake environment; choose appropriate user, warehouse, and database names as required.

If you intend to use Snowpipe to automatically ingest data from S3 into Snowflake, then follow the instructions at https://docs.snowflake.net/manuals/user-guide/data-load-snowpipe.html for configuration help.



Testing
-------
TODO: test detection on snowflake query history