#!/bin/bash
# Creates Kafka topics required by minibank.
# Runs as a one-shot init container that exits 0 on success.

KAFKA_BROKER="kafka:9092"

echo "Waiting for Kafka to be ready at $KAFKA_BROKER..."
until kafka-topics --bootstrap-server "$KAFKA_BROKER" --list > /dev/null 2>&1; do
  echo "  Kafka not ready yet, retrying in 2s..."
  sleep 2
done
echo "Kafka is ready."

echo "Creating topics..."

kafka-topics --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic transfer.events \
  --partitions 1 \
  --replication-factor 1
echo "  transfer.events: OK"

kafka-topics --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic account.events \
  --partitions 1 \
  --replication-factor 1
echo "  account.events: OK"

echo "All topics created."
