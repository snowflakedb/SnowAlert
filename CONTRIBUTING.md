# Introduction

SnowAlert is an open source project with contributions from Snowflake's security team as well as collaborating security teams. Please reach out to us if you're interested in making SnowAlert better, we'd love to be in touch.

# Technical

## Building docker containers

~~~
docker build -t snowsec/snowalert -f Dockerfile.snowalert .
docker build -t snowsec/snowalert-webui -f Dockerfile.webui .
~~~

## Developing

### Environment Variables

The installer sets up SnowAlert and gives you authentication variables for the runners and WebUI. You can run it with:

~~~
cd src/
python scripts/install.py
~~~

Once you finish, the installer will give you a command that will save env variables to a file on your local system. Since the installer is usually run from a Docker container, we opt for the explicit copy-paste so users understand exactly what they are doing. This file contains SnowAlert settings and authentication credentials and is usually referred to as an "envs file". In order to run these following commands, we assume these env vars are in the `snowalert-$SNOWFLAKE_ACCOUNT.envs` file.

### Build, Test, and Start Runners

~~~
# to build runners
cd src/
python -m venv .venv
source .venv/bin/activate
pip install -e .

# to start runners
export $(cat snowalert-$SNOWFLAKE_ACCOUNT.envs | xargs)
python runners/run.py all
~~~

If you'd like to run the test suite, please create a separate ENVS file that contains these additional variables used to test the installer:

~~~
export SA_ENV=test
export SA_ADMIN_USER=<your user>
export SA_ADMIN_USER=<your password>
~~~

If you save this to `snowalert-$SNOWFLAKE_ACCOUNT.testing.envs`, you can run the tests with:

~~~
# test runners
export $(cat snowalert-$SNOWFLAKE_ACCOUNT.testing.envs | xargs)
pytest -vv
~~~

### Build and Start WebUI Backend

The WebUI has two ways of authenticating. The quicker is server-side authentication accomplished by re-using the ENVS file used for runner credentials:

~~~
# where runners left off
cd src/
source .venv/bin/activate

# build backend
cd webui/backend
pip install -e .

# start backend
export $(cat snowalert-$SNOWFLAKE_ACCOUNT.envs | xargs)
python webui/app.py
~~~

### Building WebUI Frontend

~~~
cd src/webui/frontend
yarn install
yarn build
yarn start
~~~

If you'd help authenticating the WebUI using Snowflake OAuth, please do not hesitate to reach out to us at
snowalert@snowflake.com.


# Developing Plugins

## Developing a new Alert Handler

A handler is a Python module that has a `handle` function, which takes an alert and extra data to describe how it should be handled. This function uses its argument names to declare what subest of the following data it requires —

  `alert` - dict representation of the VARIANT in the field of the `results.alerts:ALERT` column
  `correlation_id` - the VARCHAR value in the field of the `results.alerts:CORRELATION_ID` column
  `alert_count` - the NUMBER value in the field of the `results.alerts:COUNTER` column
  `...alert['HANDLERS'][n]` - the keys and values of the alert handler being called, passed in as keyword arguments

The `handle(...)` function performs whatever is necessary for the alert to be handled. It succeeds if it returns a value and does not succeed if calling it throws an exception. Regardless, the results will be recorded in an array in the `results.alerts:HANDLED` column that is parallel to the `results.alerts:ALERT.HANDLERS` array.

For example:

- the Jira handler creates a Jira ticket or updates a correlated alert's Jira ticket. To run, it uses the `JIRA_PROJECT` env variable to know what project to use and `correlation_id` from the dispatcher. Its function declaration is thus `handle(alert: dict, correlation_id: string)`,
- the Slack handler uses the `SLACK_API` env var and an optional `channel` key/value pair from elements in the alert's `HANDLERS`, along with other variables which may be defined in Alert Queries, so its declaration starts with `handle(alert, channel=None, recipient_email=None, ...`.

Note that in the second case, the "channel" value is created dynamically by Alert Queries that use the Slack handler, and thus should have a default value in order for alert queries without that value create non-failing alerts. Failing alerts should be avoided and may be automatically retried in the future.

New handlers should be placed in the `./src/runners/handlers` directory; you should ensure that the name of the handler module is its type -- the string you will refer to it in the HANDLERS field in Alert Queries.
