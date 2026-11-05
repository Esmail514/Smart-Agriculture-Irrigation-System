import json
import logging

from kafka import KafkaProducer

try:
    from .config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME
except ImportError:
    from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME

logger = logging.getLogger("producer")


class SensorProducer:

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: str(key).encode("utf-8"),
            acks="all",        
            retries=5,
            linger_ms=50,
            max_in_flight_requests_per_connection=1,
            compression_type="gzip",
        )
        self.pending = []

    def send(self, data):
        key = data.get("sensor_id")

        future = self.producer.send(TOPIC_NAME, key=key, value=data)
        future.add_errback(self._on_send_error, sensor_id=key)
        self.pending.append(future)

    def _on_send_error(self, exception, sensor_id):
        logger.error("kafka_send_failed sensor_id=%s error=%s", sensor_id, exception)

    def flush(self):
        self.producer.flush(timeout=30)
        # Surface errors to the process supervisor; logging alone loses data.
        pending, self.pending = self.pending, []
        for future in pending:
            future.get(timeout=30)

    def close(self):
        self.producer.flush()
        self.producer.close(timeout=10)
