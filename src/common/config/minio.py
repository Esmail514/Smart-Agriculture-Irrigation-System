import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", ""))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", ""))

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
