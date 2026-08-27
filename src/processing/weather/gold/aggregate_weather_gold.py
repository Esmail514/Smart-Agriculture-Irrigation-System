from common.logger import setup_logging, get_logger

from pyspark.sql.functions import avg
from pyspark.sql.functions import col
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_date
from pyspark.sql.functions import first

from weather.weather_config import (
    GOLD_WEATHER_ALERTS_PATH,
    GOLD_WEATHER_DAILY_PATH,
    RAIN_ALERT_THRESHOLD_MM,
    SILVER_WEATHER_PATH,
    create_spark,
)

setup_logging()
logger = get_logger("aggregate_weather_gold")


def main():
    spark = create_spark("Weather Gold - Batch")
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(SILVER_WEATHER_PATH)
    df = df.withColumn("weather_date", to_date(col("observation_time_utc")))

    daily_df = (
        df.groupBy("farm_id", "farm_name", "weather_date")
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
            spark_sum("rain_1h_mm").alias("total_rain"),
            avg("wind_speed").alias("avg_wind_speed"),
            avg("cloud_cover").alias("avg_clouds"),
            first("weather_condition").alias("weather_condition"),
            countDistinct("weather_reading_id").alias("observation_count"),
        )
        .withColumn("last_update", current_timestamp())
        .drop("observation_count")
    )

    daily_df.write.format("parquet").mode("overwrite").save(GOLD_WEATHER_DAILY_PATH)


    alerts_df = (
        df.filter(col("rain_1h_mm") > RAIN_ALERT_THRESHOLD_MM)
        .select("farm_id", "farm_name", "observation_time_utc", "rain_1h_mm", "weather_condition")
        .withColumn("alert_message", lit("هطول أمطار ملحوظ - فكّر بتقليل/تأجيل الري المجدول"))
    )

    alerts_count = alerts_df.count()
    if alerts_count > 0:
        alerts_df.write.format("parquet").mode("overwrite").save(GOLD_WEATHER_ALERTS_PATH)

    logger.info(
        "gold_weather_done daily_rows=%d alert_rows=%d",
        daily_df.count(), alerts_count,
    )

    spark.stop()


if __name__ == "__main__":
    main()
