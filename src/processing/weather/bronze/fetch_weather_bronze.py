"""Fetch validated current-weather observations; malformed payloads go to quarantine."""
import json
from datetime import datetime, timezone
import psycopg2
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from common.logger import setup_logging, get_logger
from weather.contracts import flatten_record, bronze_schema
from weather.weather_config import (BRONZE_WEATHER_PATH, SILVER_WEATHER_REJECTED_PATH,
    OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL, POSTGRES_CONFIG)

setup_logging()
logger = get_logger("fetch_weather_bronze")


def load_farms():
    connection = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT farm_id, farm_name, latitude, longitude FROM farms ORDER BY farm_id")
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        connection.close()


def fetch_weather(lat, lon):
    if not OPENWEATHER_API_KEY:
        raise ValueError("OPENWEATHER_API_KEY is required")
    with requests.Session() as session:
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"], respect_retry_after_header=False)
        session.mount("https://", HTTPAdapter(max_retries=retry))
        response = session.get(OPENWEATHER_BASE_URL, params={"lat": lat, "lon": lon,
            "appid": OPENWEATHER_API_KEY, "units": "metric"}, timeout=(5, 15))
        # Avoid including the request URL, which contains the secret API key.
        if response.status_code != 200:
            raise RuntimeError(f"Weather API HTTP status {response.status_code}")
        return response.json()


def main():
    from common.session import create_spark
    from pyspark.sql import functions as f
    farms = load_farms()
    if not farms:
        raise RuntimeError("No farms available for weather collection")
    fetched_at = datetime.now(timezone.utc)
    records, rejects, failed = [], [], []
    for farm in farms:
        data = None
        try:
            data = fetch_weather(farm["latitude"], farm["longitude"])
            # Each response has its own fetch instant. A slow multi-farm run
            # must not compare later observations with the batch's start time.
            fetched_at = datetime.now(timezone.utc)
            records.append(flatten_record(farm, data, fetched_at))
        except Exception as error:
            logger.error("Weather fetch or validation failed for farm %s (%s)", farm["farm_id"], type(error).__name__)
            failed.append(farm["farm_id"])
            reason = str(error) if data is not None and isinstance(error, ValueError) else type(error).__name__
            rejects.append((str(farm["farm_id"]), fetched_at.isoformat(), type(error).__name__, reason, json.dumps(data, ensure_ascii=False)))
    spark = create_spark("Weather Bronze - Fetch")
    try:
        if rejects:
            spark.createDataFrame(rejects, "farm_id string, rejected_at string, error_type string, rejection_reason string, raw_response string").write.mode("append").parquet(SILVER_WEATHER_REJECTED_PATH + "/ingestion")
        if records:
            frame = spark.createDataFrame(records, bronze_schema())
            for column, expression in [("year", f.year("fetched_at")), ("month", f.month("fetched_at")), ("day", f.dayofmonth("fetched_at"))]:
                frame = frame.withColumn(column, expression)
            # Standard partition root: Spark can discover every observation without
            # recursively traversing nested per-request parquet directories.
            frame.write.mode("append").partitionBy("year", "month", "day").parquet(BRONZE_WEATHER_PATH)
            logger.info("Weather bronze accepted=%d rejected=%d", len(records), len(rejects))
    finally:
        spark.stop()
    if failed:
        raise RuntimeError(f"Weather collection incomplete for farms: {failed}")


if __name__ == "__main__":
    main()
