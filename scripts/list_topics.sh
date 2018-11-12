#!/usr/bin/env bash

BROKER_CONTAINER_ID=$(docker ps -q -f "name=broker")

docker exec ${BROKER_CONTAINER_ID} kafka-topics --zookeeper zookeeper:2181 --list
