..  _start:

Getting Started
===============

tl;dr Usage
-----------

The SnowAlert Docker container exposes two commands —

.. code::

    docker run -it snowsec/snowalert ./install

which configures your database, and

.. code::

    docker run -it --env-file snowalert-{account}.envs snowsec/snowalert ./run

which runs the SnowAlert functions, using auth environment variables {``SA_ROLE`` ``SA_USER`` ``PRIVATE_KEY`` and ``PRIVATE_KEY_PASSWORD``} to authenticate and operational envars {``SNOWFLAKE_ACCOUNT`` ``SA_REGION`` ``SA_DATABASE`` ``SA_WAREHOUSE``} to create the alerts.

Finally, we have a SnowAlert Management WebUI, which you can use to edit your rules —

.. code::

    docker run -it -p 8000:8000 --env-file snowalert-{account}.envs snowsec/snowalert-webui

Snowflake OAuth is the recommended authentication method for the WebUI. Storing credentials in the following environment variables will enable anyone with web access to manage the rules and data connectors. Please don't use these environment variables in production. While we recommend using OAuth for prod auth, locally or in an otherwise secured environment, you can configure the application to use the same credentials stored in environment variables as the runners do: ``SNOWFLAKE_ACCOUNT``, ``SA_USER``, ``SA_ROLE``, ``SA_DATABASE``, ``SA_WAREHOUSE``, ``PRIVATE_KEY``, ``PRIVATE_KEY_PASSWORD``. When deploying to prod, please follow the instructions in :ref:`authentication`.

.. note::

The WebUI can use OAuth with Snowflake serving as an "OAuth provider", which is the same protocol, but a different authenticity delegation than the SSO OAuth in the Snowflake product. There, a provider like Okta can provide authentication services to Snowflake instead of a username and password. The two OAuth implementations can be used together to chain authentication from Okta, through Snowflake, and into SnowAlert.

Requirements
------------

To use SnowAlert, you will need

1. Administrator access to a Snowflake account
    - the SnowAlert installer needs Snowflake account administrator (``ACCOUNTADMIN``) privileges.
    - after installation is complete, SnowAlert accesses Snowflake using a dedicated, non-admin user.

2. Docker container orchestration in the cloud, e.g. —
    - SnowAlert runners are published in a Docker container to run on AWS Fargate, Azure Container, or Kubernetes,
    - SnowAlert WebUI is published in a separate Docker container hosting a web application which needs to be network accessible by SnowAlert administrators.

Additionally, if you would like to use KMS as an extra layer of encryption and audit, please run the installer with AWS credentials that are allowed to manage and/or use KMS keys, e.g. —

.. code::

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "kms:CreateKey",
                    "kms:Encrypt",
                    "kms:Decrypt"
                ],
                "Resource": "*"
            }
        ]
    }


Downloading
-----------

The installer and runner both are distributed via DockerHub. To download it, run:

.. code::

    docker pull snowsec/snowalert

You can also use git to download the code that builds this container by running:

.. code::

    git clone git@github.com:snowflakedb/SnowAlert.git
    cd SnowAlert
    docker build -t snowsec/snowalert .


Installing
----------

Snowflake provides an installer script in the home dir of the docker container, as well as the git repository, which will configure your Snowflake workspace and use KMS to encrypt secrets in the runner's environment:

If you have ``~/.aws`` and ``~/.snowsql`` credentials in your environment, you can run it with:

.. code::

  docker run -it \
    --mount type=bind,source="$HOME/.aws",target=/root/.aws \
    --mount type=bind,source="$HOME/.snowsql",target=/root/.snowsql \
      snowsec/snowalert ./install

For a simpler installation which skips the KMS encryption layer, you can run:

.. code::

    $ docker run -it snowsec/snowalert ./install


Results
-------

During the installation, the installer will create:

#. A warehouse, user, role, database, and schema in your Snowflake instance that SA will use to manage alerts.
#. A private key with an optional passphrase for authenticating to Snowflake as the SA user. This passphrase can be stored as an environment variable and can be optionally encrypted via Amazon KMS.

Finally, the installer will provide you with a list of environment variables which must be given to the Docker container in order to run SnowAlert functions. The functions should be run regularly (we recommend at least once an hour). If you need help setting up a framework for scheduling the functions, email snowalert@snowflake.com and we will help you get the automation set up properly.

The installer will also provide you with commands that will let you run a sample alert and violation definition SnowAlert immediately. Since the SnowAlert user authenticated to Snowflake during installation and does not have MFA configured, it will result in an alert appearing in your alerts table. If Jira is configured, then the Jira alert handler will run, creating a ticket in the Jira project for the alert.


Feedback
--------

Any issues? Please reach out to us at snowalert@snowflake.com.
