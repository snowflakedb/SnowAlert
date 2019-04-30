# Introduction

SnowAlert is an open source project with contributions from Snowflake's security team as well as collaborating
security teams. Please reach out to us if you're interested in making SnowAlert better, we'd love to be in touch.

# Technical

## Building docker container

~~~
docker build -t snowsec/snowalert .
~~~

## Developing SAMUI

~~~
docker build -t snowalert .
~~~

~~~
cd src/samui/frontend
yarn install
yarn build
~~~

~~~
cd src/
python -m venv .venv
source .venv/bin/activate
pip install -e .
cd samui/backend
pip install -e .
python samui/app.py
~~~

## Building SAMUI

~~~
cd src/samui/frontend
yarn install
yarn build
~~~

~~~
./build samui
~~~

# Extensions

## Developing a new Alert Handler

An alert handler needs to expose only one part of its interface: a `handle()` function, which takes an alert and
optionally additional metadata from the handler to describe how this alert should be handled.

The `handle()` function should ultimately perform whatever is required for an alert to be considered handled; in the
case of a jira handler, this would involve creating a jira ticket in the specified project. Additionally, the
`handle()` function must handle bookkeeping on the Snowflake side by setting the HANDLED column of the alerts table for the specified alert to reflect the fact that the alert has been processed; you can record whatever data is significant, but this column must not be left unset, or the alert will be handled again on the next run.

A new handler should be placed in the plugins directory; you should ensure that the name of the handler file is
"<service>.py"; i.e. a handler which sends messages to Slack would be called slack.py.
