"""Daily observed-weather estimates at one farm/day grain (UTC)."""
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from common.logger import setup_logging, get_logger
from weather.contracts import silver_schema, daily_schema, alerts_schema, enforce_columns
from weather.weather_config import (GOLD_WEATHER_ALERTS_PATH, GOLD_WEATHER_DAILY_PATH,
    RAIN_ALERT_THRESHOLD_MM, SILVER_WEATHER_PATH, create_spark)

setup_logging()
logger = get_logger("aggregate_weather_gold")


def aggregate(df):
    df = df.withColumn("weather_date", f.to_date("observation_time_utc"))
    # The current-weather API reports rainfall over the previous rolling hour.
    # Choose one latest observation per UTC hour; polling twice an hour must
    # not double reported rainfall. This is an estimate, not a rain gauge total.
    hourly_window = Window.partitionBy("farm_id", f.date_trunc("hour", f.col("observation_time_utc"))).orderBy(f.col("observation_time_utc").desc(), f.col("fetched_at").desc())
    hourly = df.withColumn("rn", f.row_number().over(hourly_window)).filter("rn = 1").drop("rn")
    daily = hourly.groupBy("farm_id", "weather_date").agg(
        f.avg("temperature").alias("avg_temperature"), f.avg("humidity").alias("avg_humidity"),
        f.sum("rain_1h_mm").alias("total_rain"), f.avg("wind_speed").alias("avg_wind_speed"),
        f.avg("cloud_cover").alias("avg_clouds"), f.max("observation_time_utc").alias("last_update"))
    last_window = Window.partitionBy("farm_id", "weather_date").orderBy(f.col("observation_time_utc").desc(), f.col("fetched_at").desc())
    conditions = df.withColumn("rn", f.row_number().over(last_window)).filter("rn = 1").select("farm_id", "weather_date", "farm_name", "weather_condition")
    daily = daily.join(conditions, ["farm_id", "weather_date"], "left")
    alerts = df.filter(f.col("rain_1h_mm") > RAIN_ALERT_THRESHOLD_MM).select(
        "farm_id", "farm_name", "observation_time_utc", "rain_1h_mm", "weather_condition").withColumn(
        "alert_message", f.lit("Observed rainfall: review planned irrigation"))
    return enforce_columns(daily, daily_schema()), enforce_columns(alerts, alerts_schema())


def main():
    spark = create_spark("Weather Gold - Batch")
    try:
        daily, alerts = aggregate(spark.read.schema(silver_schema()).parquet(SILVER_WEATHER_PATH))
        daily.write.mode("overwrite").partitionBy("weather_date").parquet(GOLD_WEATHER_DAILY_PATH)
        alerts.write.mode("overwrite").parquet(GOLD_WEATHER_ALERTS_PATH)
        logger.info("Weather gold daily=%d alerts=%d", daily.count(), alerts.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
