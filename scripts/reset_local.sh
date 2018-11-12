#!/usr/bin/env bash

SCRIPT_PATH=$(dirname "$0")
DATA_DIR=${SCRIPT_PATH}/../data

docker-compose stop -t 120

rm -r /tmp/kafka-streams/userApp1 ${DATA_DIR}
mkdir -p ${DATA_DIR}/{zk-data,zk-log,kafka}

docker-compose up -d


