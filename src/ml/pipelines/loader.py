import pandas as pd
from ml.pipelines.clickhouse_client import get_client, query_to_dataframe
from ml.pipelines.settings import TABLE_ZONE_HISTORY, TABLE_FARM_WEATHER_DAILY
from ml.pipelines.logger import get_logger

logger = get_logger("data_loader")


def load_training_data() -> pd.DataFrame:
    client = get_client()

    zone_query = f"""
    SELECT zone_id, zone_name, farm_id, farm_name, crop_id, crop_name,
           sensor_count, avg_soil_moisture, avg_soil_temperature,
           avg_air_temperature, avg_air_humidity, moisture_min, moisture_max,
           recommended_water_mm, last_update, water_status
    FROM {TABLE_ZONE_HISTORY}
    WHERE last_update >= today() - INTERVAL 90 DAY
    """

    weather_query = f"""
    SELECT farm_id, weather_date, avg_temperature, avg_humidity,
           total_rain, avg_wind_speed
    FROM {TABLE_FARM_WEATHER_DAILY}
    WHERE weather_date >= today() - INTERVAL 90 DAY
    """

    logger.info("Loading zone history from ClickHouse...")
    zone_df = query_to_dataframe(client, zone_query)
    logger.info("Loaded %d zone records", len(zone_df))

    logger.info("Loading weather data from ClickHouse...")
    weather_df = query_to_dataframe(client, weather_query)
    logger.info("Loaded %d weather records", len(weather_df))

    if zone_df.empty:
        logger.warning("No zone data found")
        return pd.DataFrame()

    zone_df["last_update"] = pd.to_datetime(zone_df["last_update"])
    zone_df["date"] = zone_df["last_update"].dt.date

    if not weather_df.empty:
        weather_df["weather_date"] = pd.to_datetime(weather_df["weather_date"]).dt.date
        merged = zone_df.merge(
            weather_df,
            left_on=["farm_id", "date"],
            right_on=["farm_id", "weather_date"],
            how="left",
        )
    else:
        merged = zone_df
        for col in ["avg_temperature", "avg_humidity", "total_rain", "avg_wind_speed"]:
            merged[col] = 0.0

    return merged
