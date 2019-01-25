Getting Started
===============

tl;dr Usage
-----------

The SnowAlert Docker container exposes two commands —

.. code::

    docker run -it -v$HOME/.aws:/root/.aws -v$HOME/.snowsql:/root/.snowsql snowsec/snowalert ./install

which configures your database, and

.. code::

    docker run --env-file snowalert-{account}.envs snowsec/snowalert ./run

which runs the SnowAlert functions.

Finally, we have a (work in progress!) SnowAlert Management UI, which you can use to edit your rules —

.. code::

    docker run -it -d -p 8000:8000 --env-file snowalert-{account}.envs snowsec/samui

This is a work in progress, which supports some features not yet in General Availability like OAuth, so don't hesitate
to ask your account manager or reach out to us with questions at snowalert@snowflake.com.

Requirements
------------

To use SnowAlert, you will need

1. Administrator access to a Snowflake account
    - to create a snowalert user that reads alert definitions and manages security events

2. A way to run alert creation and processing functions, e.g. —
    - a server on which the SnowAlert docker container can be scheduled to run

3. A way to handle alerts, e.g. —
    - Jira repository and user with permission to create tickets
    - Sigma Computing account to create and manage dashboards

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

If you have `~/.aws` and `~/.showcli` credentials in your environment, you can run it with:

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
