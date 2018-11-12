#!/usr/bin/env bash

SCHEMA_REGISTRY_CONTAINER_ID=$(docker ps -q -f "name=schema_registry")

docker exec ${SCHEMA_REGISTRY_CONTAINER_ID} kafka-avro-console-consumer \
  --bootstrap-server broker:9092 \
  --property schema.registry.url=http://localhost:8081 \
  --property print.key=true \
  --topic $@
