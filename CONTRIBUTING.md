# Introduction

#### SnowAlert is a new project, more guidelines coming soon.

# Technical

## Building docker container

~~~
docker build -t snowalert .
~~~

## Building SAMUI

~~~
cd src/samui/frontend
yarn install
yarn start
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
