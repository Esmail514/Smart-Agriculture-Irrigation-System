from common.logger import setup_logging, get_logger

from common.config import (
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_JDBC_DRIVER,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
)
from common.session import create_spark
from weather.weather_config import GOLD_WEATHER_DAILY_PATH

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

    daily_df = spark.read.parquet(GOLD_WEATHER_DAILY_PATH)
    daily_count = daily_df.count()
    if daily_count > 0:
        (
            daily_df.write
            .format("jdbc")
            .mode("append")
            .options(**WRITE_OPTIONS)
            .option("dbtable", "agri_gold.farm_weather_daily")
            .save()
        )
        logger.info("weather_daily_written_to_clickhouse rows=%d", daily_count)
    else:
        logger.info("weather_daily_empty - nothing to sync")

    spark.stop()
    logger.info("weather_clickhouse_sync_done")


if __name__ == "__main__":
    main()

