FROM python:3.7-slim-stretch

RUN apt-get update -y \
 && apt-get install -y git build-essential r-base

RUN pip install --upgrade pip virtualenv pyflakes pytest pytest-ordering mypy mypy-extensions
