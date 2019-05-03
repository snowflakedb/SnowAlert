FROM python:3.7-slim-stretch

WORKDIR /var/task

RUN pip install --upgrade pip virtualenv pyflakes

RUN mkdir -p ./snowalert
RUN virtualenv ./snowalert/venv
ENV PATH="/var/task/snowalert/venv/bin:${PATH}"

COPY ./src ./snowalert/src

# backend
RUN apt-get update \
 && apt-get install -y gcc linux-libc-dev \
 && rm -rf /var/lib/apt/lists/* \
 && PYTHONPATH='' pip install ./snowalert/src/ ./snowalert/src/webui/backend/ \
 && apt-get purge -y --auto-remove gcc linux-libc-dev

# frontend
RUN apt-get update \
 && apt-get install -y curl gnupg2 apt-transport-https \
 && curl -sL https://deb.nodesource.com/setup_11.x | bash - \
 && apt-get install -y nodejs \
 && curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
 && echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list \
 && apt-get update \
 && apt-get install -y yarn \
 && cd ./snowalert/src/webui/frontend && yarn install && yarn build \
 && rm -fr node_modules \
 && apt-get purge -y --auto-remove curl gnupg2 apt-transport-https nodejs yarn

# link frontend build into backend venv
RUN ln -s $PWD/snowalert/src/webui/frontend ./snowalert/venv/lib/python3.7/

CMD python ./snowalert/src/webui/backend/webui/app.py
