#!/usr/bin/env bash

BROKER_CONTAINER_ID=$(docker ps -q -f "name=broker")

docker exec ${BROKER_CONTAINER_ID} kafka-run-class \
  kafka.tools.GetOffsetShell --offsets 20 --broker-list broker:9092 --topic $@
