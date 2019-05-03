# Introduction

SnowAlert is an open source project with contributions from Snowflake's security team as well as collaborating
security teams. Please reach out to us if you're interested in making SnowAlert better, we'd love to be in touch.

# Technical

## Building docker containers

~~~
docker build -t snowsec/snowalert -f Dockerfile.snowalert .
docker build -t snowsec/webui -f Dockerfile.webui .
~~~

## Developing


### Building WebUI Frontend

~~~
cd src/webui/frontend
yarn install
yarn build
~~~

### Build, Test, and Start Runners

~~~
# build runners
cd src/
python -m venv .venv
source .venv/bin/activate
pip install -e .

# test runners
pytest -vv

# start runners
python runners/run.py all
~~~

### Build and Start WebUI Backend

~~~
# build backend
cd webui/backend
pip install -e .

# start backend
python webui/app.py
~~~

### Building WebUI

~~~
cd src/webui/frontend
yarn install
yarn build
yarn start
~~~

# Extensions

## Developing a new Alert Handler

An alert is a module that has a `handle` function, which takes an alert and extra data to describe how it should be
handled. The handler uses its argument names to declare what subest of the following data it wants —

  `alert` - dict representation of the VARIANT in the fields of the `results.alerts:ALERT` column
  `correlation_id` - the VARCHAR value in the fields of the `results.alerts:CORRELATION_ID` column
  `alert_count` - the NUMBER value in the fields of the `results.alerts:COUNTER` column
  `...alert['HANDLERS'][n]` - all of the keys defined alert handler being called, passed in as keyword arguments

The `handle(...)` function performs whatever requirements necessary for the alert handling. It succeeds if it returns
a value and does not succeed if calling it throws an exception. Regardless, the results will be recorded in an array
in the `results.alerts:HANDLED` column that is parallel to the `results.alerts:ALERT:HANDLERS` array.

For example:

- the Jira handler creates a Jira ticket or updates a correlated alert's Jira ticket in the project it is set up via
ENV vars to run, i.e. `handle(alert, correlation_id, ...)`, and
- the Slack handler reads API keys from an env var and the `channel` from the keys in the alert's `HANDLERS`, i.e.
`handle(alert, channel=None, ...)`.

Note that in the second case, the "channel" value is created dynamically by alerts that define a Slack handler, and
thus needs to have a default value in order for alert queries without that value create non-failing alerts. Failing
alerts should be avoided and may be automatically retried in the future.



A new handler should be placed in the plugins directory; you should ensure that the name of the handler file is
"<service>.py"; i.e. a handler which sends messages to Slack would be called slack.py.
