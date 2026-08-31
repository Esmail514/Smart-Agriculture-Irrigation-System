from weather.weather_config import GOLD_WEATHER_DAILY_PATH

from common.ch_sync import atomic_replace
from common.config import (
    CLICKHOUSE_JDBC_DRIVER,
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
)
from common.logger import get_logger, setup_logging
from common.session import create_spark

setup_logging()
logger = get_logger("sync_weather_clickhouse")

WRITE_OPTIONS = {
    "driver": CLICKHOUSE_JDBC_DRIVER,
    "url": CLICKHOUSE_JDBC_URL,
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASSWORD,
}


def main():
    spark = create_spark("Weather Gold -> ClickHouse")
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Aggregate_weather_gold rewrites the FULL history each run, so the
        # ClickHouse mirror must be replaced wholesale (idempotent).  Appending
        # would duplicate every farm/day row on every 30-minute DAG run.
        daily_df = spark.read.parquet(GOLD_WEATHER_DAILY_PATH)
        daily_count = daily_df.count()
        if daily_count > 0:
            atomic_replace(spark, daily_df, "agri_gold.farm_weather_daily", WRITE_OPTIONS)
            logger.info("weather_daily_written_to_clickhouse rows=%d (full mirror)", daily_count)
        else:
            logger.info("weather_daily_empty - nothing to sync")
    finally:
        spark.stop()
    logger.info("weather_clickhouse_sync_done")


if __name__ == "__main__":
    main()