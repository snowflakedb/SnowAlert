# Introduction

#### SnowAlert is a new project, more guidelines coming soon.

# Technical

## Building docker container

~~~
docker build -t snowalert .
~~~

## Building SAMUI

~~~
cd samui/frontend
yarn install
yarn start
~~~

~~~
cd samui/backend
python -m venv .venv
source .venv/bin/activate
pip install -e .
python samui/app.py
~~~
