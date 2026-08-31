from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from common.ch_sync import atomic_replace
from common.config import (
    CLICKHOUSE_JDBC_DRIVER,
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
)
from common.irrigation_decisions import irrigation_decision
from common.logger import get_logger, setup_logging
from common.session import create_spark

setup_logging()
logger = get_logger("sync_weather_alerts_clickhouse")

JDBC_OPTS = {
    "driver": CLICKHOUSE_JDBC_DRIVER,
    "url": CLICKHOUSE_JDBC_URL,
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASSWORD,
}

TABLE_ALERTS = "agri_gold.weather_irrigation_alerts"

ZONE_QUERY = "SELECT * FROM agri_gold.zone_summary_latest"

WEATHER_QUERY = """
SELECT farm_id, weather_date, total_rain, weather_condition
FROM (
    SELECT *, row_number() OVER (PARTITION BY farm_id ORDER BY weather_date DESC) AS rn
    FROM agri_gold.farm_weather_daily
) t
WHERE rn = 1
"""

_decision_udf = f.udf(irrigation_decision, StringType())


def build_alerts(spark):
    zones_df = (
        spark.read.format("jdbc")
        .options(**JDBC_OPTS, dbtable=f"({ZONE_QUERY}) z")
        .load()
    )
    weather_df = (
        spark.read.format("jdbc")
        .options(**JDBC_OPTS, dbtable=f"({WEATHER_QUERY}) w")
        .load()
    )

    alerts_df = (
        zones_df.join(weather_df, "farm_id", "left")
        .withColumn("soil_moisture", f.col("avg_soil_moisture"))
        .withColumn("expected_rain", f.coalesce(f.col("total_rain"), f.lit(0.0)))
        .withColumn(
            "irrigation_decision",
            _decision_udf(
                f.col("water_status"),
                f.col("expected_rain"),
                f.col("avg_soil_moisture"),
            ),
        )
        .withColumn(
            "weather_condition", f.coalesce(f.col("weather_condition"), f.lit(""))
        )
        .withColumn(
            "event_time",
            f.coalesce(f.to_timestamp(f.col("weather_date")), f.current_timestamp()),
        )
        .withColumn("processing_time", f.current_timestamp())
        .select(
            "farm_id",
            "farm_name",
            "zone_id",
            "zone_name",
            "crop_id",
            "crop_name",
            "soil_moisture",
            "expected_rain",
            "recommended_water_mm",
            "irrigation_decision",
            "weather_condition",
            "event_time",
            "processing_time",
        )
    )

    return alerts_df


def main():
    spark = create_spark("Weather Irrigation Alerts -> ClickHouse")
    spark.sparkContext.setLogLevel("WARN")

    try:
        alerts_df = build_alerts(spark)
        alerts_count = alerts_df.count()
        if alerts_count > 0:
            atomic_replace(spark, alerts_df, TABLE_ALERTS, JDBC_OPTS)
            logger.info("weather_alerts_written_to_clickhouse rows=%d", alerts_count)
        else:
            logger.info("weather_alerts_empty - nothing to sync")
    finally:
        spark.stop()
    logger.info("weather_alerts_clickhouse_sync_done")


if __name__ == "__main__":
    main()
