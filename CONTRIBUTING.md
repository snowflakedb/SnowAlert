# Introduction

#### SnowAlert is a new project, more guidelines coming soon.

# Technical

## Building docker container

~~~
./build snowalert
~~~

## Developing SAMUI

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
