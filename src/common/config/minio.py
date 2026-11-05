import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", ""))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", ""))

LAKEHOUSE_BUCKET = os.getenv("LAKEHOUSE_BUCKET", "agri-lakehouse")

BRONZE_SENSOR_PATH = f"s3a://{LAKEHOUSE_BUCKET}/bronze/sensor_data_v2"
BRONZE_SENSOR_QUARANTINE_PATH = f"s3a://{LAKEHOUSE_BUCKET}/quarantine/sensor_ingestion"
CHECKPOINT_BRONZE_SENSOR = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/bronze_sensor_data_v2"
CHECKPOINT_BRONZE_QUARANTINE = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/bronze_sensor_quarantine_v2"

SILVER_SENSOR_PATH = f"s3a://{LAKEHOUSE_BUCKET}/silver/sensor_data_v2"
SILVER_REJECTED_PATH = f"s3a://{LAKEHOUSE_BUCKET}/quarantine/sensor_enrichment"
CHECKPOINT_SILVER_SENSOR = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/silver_sensor_data_v2"
CHECKPOINT_SILVER_REJECTED = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/silver_sensor_rejected_v2"

GOLD_ZONE_HISTORY_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/zone_summary_history_v2"
GOLD_ZONE_LATEST_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/zone_summary_latest_v2"
GOLD_FARM_HISTORY_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/farm_summary_history_v2"
GOLD_FARM_LATEST_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/farm_summary_latest_v2"
GOLD_ALERTS_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/irrigation_alerts_v2"

CHECKPOINT_GOLD_ZONE = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/gold_zone"
CHECKPOINT_GOLD_FARM = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/gold_farm"
CHECKPOINT_GOLD_ALERTS = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/gold_alerts"
