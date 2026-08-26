#!/bin/bash
# ============================================================
# Kafka Topic Initialization Script
# Smart Agriculture Irrigation System
# ============================================================
# Creates all required topics on startup.
# ============================================================

set -e

BOOTSTRAP=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}

echo ">>> Waiting for Kafka broker..."
cub kafka-ready -b $BOOTSTRAP 1 30

echo ">>> Creating topics..."

# Sensor data - IoT readings from soil/air sensors
kafka-topics --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic sensor-data \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

# Weather data - OpenWeatherMap API responses
kafka-topics --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic weather-data \
  --partitions 2 \
  --replication-factor 1 \
  --config retention.ms=2592000000

# Irrigation alerts - threshold breach notifications
kafka-topics --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic irrigation-alerts \
  --partitions 2 \
  --replication-factor 1 \
  --config retention.ms=2592000000

# ML predictions - irrigation recommendations
kafka-topics --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic ml-predictions \
  --partitions 2 \
  --replication-factor 1 \
  --config retention.ms=2592000000

echo ">>> All topics created successfully!"
kafka-topics --list --bootstrap-server $BOOTSTRAP
