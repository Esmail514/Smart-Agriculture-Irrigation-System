import json
import logging

from kafka import KafkaProducer

try:
    from .config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME
except ImportError:
    from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME

logger = logging.getLogger("weather_producer")


class WeatherProducer:

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
        key = data.get("farm_id")
        future = self.producer.send(TOPIC_NAME, key=key, value=data)
        future.add_errback(self._on_send_error, farm_id=key)
        self.pending.append(future)

    def _on_send_error(self, exception, farm_id):
        logger.error("kafka_weather_send_failed farm_id=%s error=%s", farm_id, exception)

    def flush(self):
        self.producer.flush(timeout=30)
        pending, self.pending = self.pending, []
        for future in pending:
            future.get(timeout=30)

    def close(self):
        self.flush()
        self.producer.close(timeout=10)
        logger.info("weather_producer_closed")
