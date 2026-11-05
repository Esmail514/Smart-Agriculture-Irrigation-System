"""Read history and merge one weather row per farm/day with explicit availability."""
import pandas as pd
from ml.feature_engineering import merge_weather
from ml.pipelines.clickhouse_client import get_client, query_to_dataframe
from ml.pipelines.settings import TABLE_ZONE_HISTORY, TABLE_FARM_WEATHER_DAILY


def load_training_data():
    client = get_client()
    try:
        zones = query_to_dataframe(client, f"""SELECT zone_id, zone_name, farm_id, farm_name, crop_id,
            crop_name, sensor_count, avg_soil_moisture, avg_soil_temperature, avg_air_temperature,
            avg_air_humidity, moisture_min, moisture_max, recommended_water_mm, last_update,
            water_status, snapshot_id FROM {TABLE_ZONE_HISTORY} FINAL WHERE last_update >= today() - INTERVAL 90 DAY""")
        if zones.empty:
            return pd.DataFrame()
        weather = query_to_dataframe(client, f"""SELECT farm_id, weather_date, avg_temperature,
            avg_humidity, total_rain, avg_wind_speed, last_update FROM {TABLE_FARM_WEATHER_DAILY}
            WHERE weather_date >= today() - INTERVAL 90 DAY""")
        return merge_weather(zones, weather)
    finally:
        client.close()
