#!/usr/bin/env bash

KAFKA_CONTAINER_ID=$(docker ps -q -f "name=broker")

docker exec ${KAFKA_CONTAINER_ID} kafka-console-consumer \
  --bootstrap-server broker:9092 \
  --property print.key=true \
  --topic $@
