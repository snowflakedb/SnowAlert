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
