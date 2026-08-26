import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_SENSORS = os.getenv("KAFKA_TOPIC_SENSORS", "agri.sensors.raw")
KAFKA_TOPIC_WEATHER = os.getenv("KAFKA_TOPIC_WEATHER", "agri.weather.raw")
KAFKA_TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "agri.alerts")
KAFKA_TOPIC_COMMANDS = os.getenv("KAFKA_TOPIC_COMMANDS", "agri.irrigation.commands")

# Backwards-compatible alias. Consumers in the streaming pipelines use SENSOR_TOPIC.
SENSOR_TOPIC = KAFKA_TOPIC_SENSORS
