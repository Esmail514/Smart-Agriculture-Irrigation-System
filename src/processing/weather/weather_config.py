import os

from common.config.postgres import POSTGRES_CONFIG
from common.config.clickhouse import CLICKHOUSE_JDBC_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
from common.config.minio import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, LAKEHOUSE_BUCKET,
)
from common.config.weather import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL
from common.session import create_spark


BRONZE_WEATHER_PATH = f"s3a://{LAKEHOUSE_BUCKET}/bronze/weather_data"
SILVER_WEATHER_PATH = f"s3a://{LAKEHOUSE_BUCKET}/silver/weather_data"
SILVER_WEATHER_REJECTED_PATH = f"s3a://{LAKEHOUSE_BUCKET}/silver/weather_data_rejected"
GOLD_WEATHER_DAILY_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/farm_weather_daily"
GOLD_WEATHER_ALERTS_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/weather_irrigation_alerts"

CHECKPOINT_WEATHER_BRONZE = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/weather_bronze"
CHECKPOINT_WEATHER_SILVER = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/weather_silver"
CHECKPOINT_WEATHER_GOLD_DAILY = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/weather_gold_daily"
CHECKPOINT_WEATHER_GOLD_ALERTS = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/weather_gold_alerts"

STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
}

VALID_TEMP_RANGE = (-20, 55)
VALID_HUMIDITY_RANGE = (0, 100)
VALID_PRESSURE_RANGE = (850, 1100)
VALID_WIND_SPEED_RANGE = (0, 60)

RAIN_ALERT_THRESHOLD_MM = 2.0

FARM_COORDINATES = {
    "Al-Jawf Farm - Al Hazm": {"lat": 16.1591, "lon": 44.7825},
    "Al-Jawf Farm - Al Matammah": {"lat": 15.9333, "lon": 45.3833},
    "Al-Jawf Farm - Khab wa Ash Shaaf": {"lat": 16.7500, "lon": 45.7000},
    "Tihama Farm - Bayt Al Faqih": {"lat": 14.5160, "lon": 43.3240},
    "Tihama Farm - Al Marawi'ah": {"lat": 14.8300, "lon": 43.3830},
    "Tihama Farm - Al Mansuriyah": {"lat": 14.6900, "lon": 43.2860},
}
