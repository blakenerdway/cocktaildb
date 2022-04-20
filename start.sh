#!/bin/bash

docker-compose -f airflow/docker-compose.yml up -d
docker-compose -f mysql/docker-compose.yml up -d
docker-compose -f apis/docker-compose.yml up