"""Publish fresh zone alerts with distinct observed rainfall and forecast fields."""
from pyspark.sql import functions as f
from pyspark.sql.types import StringType
from common.ch_sync import atomic_replace
from common.config import (CLICKHOUSE_JDBC_DRIVER, CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_USER)
from common.logger import get_logger, setup_logging
from common.session import create_spark
from weather.contracts import gold_alert_schema, enforce_columns
from weather.decisions import observed_weather_decision

setup_logging()
logger = get_logger("sync_weather_alerts_clickhouse")
JDBC_OPTS = {"driver": CLICKHOUSE_JDBC_DRIVER, "url": CLICKHOUSE_JDBC_URL,
             "user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}
TABLE_ALERTS = "agri_gold.weather_irrigation_alerts"
ZONE_QUERY = "SELECT * FROM agri_gold.zone_summary_latest WHERE last_update >= now() - INTERVAL 10 MINUTE"
WEATHER_QUERY = """
SELECT farm_id, weather_date, total_rain, weather_condition, last_update AS weather_observation_time
FROM (
    SELECT *, row_number() OVER (PARTITION BY farm_id ORDER BY weather_date DESC) AS rn
    FROM agri_gold.farm_weather_daily
) t
WHERE rn = 1 AND weather_date = today() AND last_update >= now() - INTERVAL 6 HOUR
"""
_decision_udf = f.udf(observed_weather_decision, StringType())


def transform_alerts(zones_df, weather_df):
    alerts = (zones_df.join(weather_df, "farm_id", "left")
        .withColumn("soil_moisture", f.col("avg_soil_moisture"))
        .withColumn("expected_rain", f.lit(None).cast("double"))
        .withColumn("observed_rain_mm", f.col("total_rain").cast("double"))
        .withColumn("weather_available", f.when(f.col("weather_observation_time").isNotNull()
            & f.col("observed_rain_mm").isNotNull(), f.lit(1)).otherwise(f.lit(0)))
        .withColumn("irrigation_decision", _decision_udf(f.col("water_status"),
            f.col("observed_rain_mm"), f.col("avg_soil_moisture")))
        .withColumn("weather_condition", f.coalesce(f.col("weather_condition"), f.lit("Unknown")))
        .withColumn("event_time", f.to_timestamp("last_update"))
        .withColumn("processing_time", f.current_timestamp()))
    return enforce_columns(alerts, gold_alert_schema())


def build_alerts(spark):
    zones = spark.read.format("jdbc").options(**JDBC_OPTS, dbtable=f"({ZONE_QUERY}) z").load()
    weather = spark.read.format("jdbc").options(**JDBC_OPTS, dbtable=f"({WEATHER_QUERY}) w").load()
    return transform_alerts(zones, weather)


def main():
    spark = create_spark("Weather Irrigation Alerts -> ClickHouse")
    spark.sparkContext.setLogLevel("WARN")
    try:
        alerts = build_alerts(spark)
        # An empty set is a valid current snapshot; old alerts must disappear.
        atomic_replace(spark, alerts, TABLE_ALERTS, JDBC_OPTS)
        logger.info("weather_alerts_written_to_clickhouse rows=%d", alerts.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
