FROM python:3.7-slim-stretch

WORKDIR /var/task

RUN pip install --upgrade pip virtualenv pyflakes

RUN mkdir -p ./snowalert
RUN virtualenv ./snowalert/venv
COPY ./snowalert/requirements.txt ./snowalert/requirements.txt
RUN PYTHONPATH='' ./snowalert/venv/bin/pip install -r ./snowalert/requirements.txt

COPY ../snowalert/src ./snowalert/src
COPY ./run ./run
COPY ./install ./install

ENV PYTHONPATH="/var/task/snowalert/src:${PYTHONPATH}"
ENV PATH="/var/task/snowalert/venv/bin:${PATH}"

CMD ./run
