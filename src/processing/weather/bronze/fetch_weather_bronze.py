import json
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import requests

from common.logger import setup_logging, get_logger
from common.session import create_spark
from weather.weather_config import (
    BRONZE_WEATHER_PATH,
    OPENWEATHER_API_KEY,
    OPENWEATHER_BASE_URL,
    POSTGRES_CONFIG,
)

setup_logging()
logger = get_logger("fetch_weather_bronze")


def load_farms():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT farm_id, farm_name, latitude, longitude FROM farms ORDER BY farm_id;")
        columns = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def fetch_weather(lat, lon):
    response = requests.get(
        OPENWEATHER_BASE_URL,
        params={"lat": lat, "lon": lon, "appid": OPENWEATHER_API_KEY, "units": "metric"},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def flatten_record(farm, data, fetched_at):
    weather = (data.get("weather") or [{}])[0]
    main = data.get("main", {})
    wind = data.get("wind", {})
    clouds = data.get("clouds", {})
    rain = data.get("rain", {})
    dt = data.get("dt")

    return {
        "weather_reading_id": f"{farm['farm_id']}-{dt}",
        "farm_id": farm["farm_id"],
        "farm_name": farm["farm_name"],
        "latitude": float(farm["latitude"]),
        "longitude": float(farm["longitude"]),
        "fetched_at": fetched_at.isoformat(),
        "observation_time_utc": (
            datetime.fromtimestamp(dt, tz=timezone.utc).isoformat() if dt else None
        ),
        "weather_condition": weather.get("main"),
        "weather_description": weather.get("description"),
        "temperature": main.get("temp"),
        "feels_like": main.get("feels_like"),
        "humidity": main.get("humidity"),
        "pressure": main.get("pressure"),
        "wind_speed": wind.get("speed"),
        "cloud_cover": clouds.get("all"),
        "rain_1h_mm": rain.get("1h", 0.0),
        "raw_response": json.dumps(data, ensure_ascii=False),
    }


def main():
    farms = load_farms()
    if not farms:
        logger.error("لا يوجد أي مزرعة بجدول farms - تأكد قاعدة البيانات")
        raise SystemExit(1)

    fetched_at = datetime.now(timezone.utc)
    records = []
    failed_farms = []

    for farm in farms:
        try:
            data = fetch_weather(farm["latitude"], farm["longitude"])
            records.append(flatten_record(farm, data, fetched_at))
        except Exception as exc:
            logger.error("فشل جلب الطقس لمزرعة %s: %s", farm["farm_name"], exc)
            failed_farms.append(farm["farm_name"])

    if not records:
        raise RuntimeError(
            "فشل جلب الطقس لكل المزارع بدون استثناء - راجع OPENWEATHER_API_KEY أو الاتصال بالشبكة"
        )

    df = pd.DataFrame(records)
    df["year"] = fetched_at.year
    df["month"] = fetched_at.month
    df["day"] = fetched_at.day

    output_path = (
        f"{BRONZE_WEATHER_PATH}/year={fetched_at.year}/month={fetched_at.month}/"
        f"day={fetched_at.day}/weather_{fetched_at.strftime('%Y%m%dT%H%M%S')}.parquet"
    )

    # Use PySpark (which has S3A configured) instead of pandas.to_parquet
    # with s3fs — avoids the heavy aiobotocore dependency chain.
    spark = create_spark("Weather Bronze - Fetch")
    spark.sparkContext.setLogLevel("WARN")
    spark_df = spark.createDataFrame(df)
    spark_df.write.mode("append").parquet(output_path)
    spark.stop()

    logger.info(
        "bronze_weather_written rows=%d failed_farms=%s path=%s",
        len(records), failed_farms, output_path,
    )

    if failed_farms:
        raise RuntimeError(f"فشل جلب الطقس للمزارع التالية: {failed_farms}")


if __name__ == "__main__":
    main()
