from common.logger import setup_logging, get_logger

from pyspark.sql.functions import col

from weather.weather_config import (
    BRONZE_WEATHER_PATH,
    SILVER_WEATHER_PATH,
    SILVER_WEATHER_REJECTED_PATH,
    VALID_HUMIDITY_RANGE,
    VALID_PRESSURE_RANGE,
    VALID_TEMP_RANGE,
    VALID_WIND_SPEED_RANGE,
    create_spark,
)

setup_logging()
logger = get_logger("transform_weather_silver")


def main():
    spark = create_spark("Weather Silver - Batch")
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(BRONZE_WEATHER_PATH)

    before_count = df.count()
    df = df.dropDuplicates(["weather_reading_id"])
    logger.info("dedup: %d -> %d صف (استدعاءات API مكررة لنفس لحظة الرصد)", before_count, df.count())

    condition = (
        col("temperature").between(*VALID_TEMP_RANGE)
        & col("humidity").between(*VALID_HUMIDITY_RANGE)
        & col("pressure").between(*VALID_PRESSURE_RANGE)
        & col("wind_speed").between(*VALID_WIND_SPEED_RANGE)
        & col("farm_id").isNotNull()
        & col("observation_time_utc").isNotNull()
    )

    valid_df = df.filter(condition).drop("raw_response")
    rejected_df = df.filter(~condition)

    valid_count = valid_df.count()
    rejected_count = rejected_df.count()

    valid_df.write.format("parquet").mode("overwrite").save(SILVER_WEATHER_PATH)

    if rejected_count > 0:
        rejected_df.write.format("parquet").mode("overwrite").save(SILVER_WEATHER_REJECTED_PATH)

    logger.info(
        "silver_weather_done valid=%d rejected=%d",
        valid_count, rejected_count,
    )

    spark.stop()


if __name__ == "__main__":
    main()
