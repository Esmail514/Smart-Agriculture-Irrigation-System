import os


def _require_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            {name}
        )
    return value



MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "esmail")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "Esmail_Ali_123")


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SENSOR_TOPIC = os.getenv("SENSOR_TOPIC", "sensor-data")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "smart_agriculture")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "root")

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

LAKEHOUSE_BUCKET = os.getenv("LAKEHOUSE_BUCKET", "agri-lakehouse")

BRONZE_SENSOR_PATH = f"s3a://{LAKEHOUSE_BUCKET}/bronze/sensor_data"
BRONZE_SENSOR_QUARANTINE_PATH = f"s3a://{LAKEHOUSE_BUCKET}/bronze/sensor_data_quarantine"
CHECKPOINT_BRONZE_SENSOR = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/bronze_sensor_data"
CHECKPOINT_BRONZE_QUARANTINE = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/bronze_sensor_quarantine"

SILVER_SENSOR_PATH = f"s3a://{LAKEHOUSE_BUCKET}/silver/sensor_data"
SILVER_REJECTED_PATH = f"s3a://{LAKEHOUSE_BUCKET}/silver/sensor_data_rejected"
CHECKPOINT_SILVER_SENSOR = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/silver_sensor_data"
CHECKPOINT_SILVER_REJECTED = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/silver_sensor_rejected"

GOLD_ZONE_HISTORY_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/zone_summary_history"
GOLD_ZONE_LATEST_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/zone_summary_latest"
GOLD_FARM_HISTORY_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/farm_summary_history"
GOLD_FARM_LATEST_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/farm_summary_latest"
GOLD_ALERTS_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/irrigation_alerts"

CHECKPOINT_GOLD_ZONE = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/gold_zone"
CHECKPOINT_GOLD_FARM = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/gold_farm"
CHECKPOINT_GOLD_ALERTS = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/gold_alerts"

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"

STREAM_TRIGGER_INTERVAL = os.getenv("STREAM_TRIGGER_INTERVAL", "1 minute")