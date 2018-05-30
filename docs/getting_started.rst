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
.. code-block:: sql
alter user <USER> set rsa_public_key='<PUBLIC KEY>';

Testing
-------
TODO: test detection on snowflake query history