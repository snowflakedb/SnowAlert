..  _authentication:

SnowAlert WebUI Authentication
==============================

Environment Variables
---------------------

The following environment variables are recommended for the SnowAlert WebUI server:

- ``SA_ROLE`` (default "snowalert") specifies the role of your runners
- ``SA_REGION`` (default "us-west-2") specifies the default region used by connectors
- ``SA_KMS_KEY`` (optional) ARN of a key which allows your WebUI to encrypt secrets, e.g. connection passwords
- ``OAUTH_{{type}}_{{account}}`` (see below) allows server to authenticate

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
        OAUTH_REDIRECT_URI = 'http://localhost:8080/login'
        OAUTH_ALLOW_NON_TLS_REDIRECT_URI = TRUE
    ;

Change the URI to the location where the Snowalert WebUI is running: e.g. if it is running at `http://192.168.1.123:8000`
then you should set the <uri> to be `http://192.168.1.123:8000/login`

2. Get the Client ID and the Client Secret

.. code::

    SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('SNOWALERT_INTERNAL');

This will output a JSON document with the Client ID and 2 Client Secrets (two are useful in case you need to rotate them, choose the first for the next step)

3. Wherever the SnowAlert WebUI is running set 2 environment variables

    1. "OAUTH_CLIENT_" + account.toUpperCase()
    2. "OAUTH_SECRET_" + account.toUpperCase()

Where account is the account name of your Snowflake instance for example:

https://mydemo.us-east-1.snowflakecomputing.com

would be DEMO (note the upper-case). i.e.:

    1. OAUTH_CLIENT_MYDEMO
    2. OAUTH_SECRET_MYDEMO

Server-side authentication
--------------------------
Snowflake OAuth is the recommended authentication method for the WebUI. Storing credentials in the following environment variables will enable anyone with web access to manage the rules and data connectors. Please don't use these environment variables in production. While we recommend using OAuth for prod auth, locally or in an otherwise secured environment, you can configure the application to use the same credentials stored in environment variables as the runners do: ``SNOWFLAKE_ACCOUNT``, ``SA_USER``, ``SA_ROLE``, ``SA_DATABASE``, ``SA_WAREHOUSE``, ``PRIVATE_KEY``, ``PRIVATE_KEY_PASSWORD``.
