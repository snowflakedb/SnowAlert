SnowAlert WebUI Authentication
==============================

OAuth Setup
-----------
There are a few steps to complete to enable OAuth authentication from the SnowAlert WebUI.

1. Create a Security Integration in the Snowflake account you are connecting to:

.. code::

    CREATE OR REPLACE SECURITY INTEGRATION
          SNOWALERT_INTERNAL
          TYPE = OAUTH
          ENABLED = TRUE
          OAUTH_CLIENT = CUSTOM
          OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
          OAUTH_REDIRECT_URI = '<uri>'
         ;

Change the URI to the location where the Snowalert WebUI is running...for example if it is running at `http://192.168.1.123:8000`
then you should set the <uri> to be `http://192.168.1.123:8000/login`

2. Get the client ID and the Client secret

.. code::

    SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('SNOWALERT_INTERNAL');

This will output a JSON document with the Client ID and 2 Client Secrets (in case you need to rotate them, you only need to choose 1 for the next step

3. Wherever the SnowAlert WebUI is running set 2 environment variables
    1. OAUTH_CLIENT_<account>
    2. OAUTH_SECRET_<account>

Where account is the account name of your Snowflake instance for example:

https://demo.us-east-1.snowflakecomputing.com

would be DEMO (note the upper-case). i.e.:
1. OAUTH_CLIENT_DEMO
2. OAUTH_SECRET_DEMO



