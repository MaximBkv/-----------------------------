#!/bin/bash

echo "Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:29092 1 30

echo "Creating Kafka topics..."

kafka-topics --create \
  --topic browser_events \
  --bootstrap-server kafka:29092 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --if-not-exists

kafka-topics --create \
  --topic device_events \
  --bootstrap-server kafka:29092 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --if-not-exists

kafka-topics --create \
  --topic geo_events \
  --bootstrap-server kafka:29092 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --if-not-exists

kafka-topics --create \
  --topic location_events \
  --bootstrap-server kafka:29092 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --if-not-exists

echo "Kafka topics created successfully"
kafka-topics --list --bootstrap-server kafka:29092
