FROM python:3.8-slim-buster

WORKDIR /app
ENV PYTHONUNBUFFERED True
WORKDIR $APP_HOME
COPY . ./

RUN set -eux && \
    export DEBIAN_FRONTEND=noninteractive && \
    apt-get update && \
    apt-get install -y default-libmysqlclient-dev build-essential gnutls-bin && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]
