"""Normalize and validate typed weather observations before publishing Silver."""
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from common.logger import setup_logging, get_logger
from weather.contracts import NUMERIC_RANGES, bronze_schema, silver_schema, enforce_columns, weather_record_error
from weather.weather_config import (BRONZE_WEATHER_PATH, SILVER_WEATHER_PATH,
    SILVER_WEATHER_REJECTED_PATH, create_spark)

setup_logging()
logger = get_logger("transform_weather_silver")


def transform(df):
    for name in list(NUMERIC_RANGES) + ["latitude", "longitude", "feels_like"]:
        df = df.withColumn(name, f.col(name).cast("double"))
    for name in ["observation_time_utc", "fetched_at"]:
        df = df.withColumn(name, f.to_timestamp(name))
    df = df.withColumn("farm_id", f.col("farm_id").cast("long"))
    # Never relabel an unsupported version before checking it. Sharing the
    # admission validator keeps Silver's temporal/numeric checks in agreement
    # with Bronze, and returns a reason for every rejected row (including NULL).
    fields = [name for name in bronze_schema().fieldNames() if name != "raw_response"]
    df = df.withColumn("contract_error", f.udf(weather_record_error, StringType())(f.struct(*fields)))
    rejected = df.filter(f.col("contract_error").isNotNull()).withColumn("rejection_reason", f.col("contract_error")).drop("contract_error")
    accepted = df.filter(f.col("contract_error").isNull()).drop("contract_error", "raw_response", "year", "month", "day")
    window = Window.partitionBy("weather_reading_id").orderBy(f.col("fetched_at").desc())
    accepted = accepted.withColumn("rn", f.row_number().over(window)).filter("rn = 1").drop("rn")
    accepted = accepted.withColumn("observation_date", f.to_date("observation_time_utc"))
    return enforce_columns(accepted, silver_schema()), rejected


def main():
    spark = create_spark("Weather Silver - Batch")
    try:
        accepted, rejected = transform(spark.read.schema(bronze_schema()).parquet(BRONZE_WEATHER_PATH))
        accepted.write.mode("overwrite").partitionBy("observation_date").parquet(SILVER_WEATHER_PATH)
        # Replace even an empty reject snapshot so resolved rejects do not persist.
        rejected.write.mode("overwrite").parquet(SILVER_WEATHER_REJECTED_PATH + "/silver")
        logger.info("Weather silver accepted=%d rejected=%d", accepted.count(), rejected.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
