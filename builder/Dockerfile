FROM python:3.7-slim-stretch

# https://github.com/geerlingguy/ansible-role-java/issues/64#issuecomment-393299088
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update -y \
 && apt-get install -y curl git build-essential r-base default-jre

RUN pip install --upgrade pip virtualenv pyflakes pytest pytest-ordering mypy mypy-extensions
