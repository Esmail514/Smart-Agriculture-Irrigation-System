from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    max as spark_max,
    row_number,
    sum as spark_sum,
    when,
)

from common.transformations import compute_water_status


def create_zone_summary(df):
    return (
        df
        .groupBy("zone_id", "zone_name", "farm_id", "farm_name", "crop_id", "crop_name")
        .agg(
            countDistinct("sensor_id").alias("sensor_count"),
            avg("soil_moisture").alias("avg_soil_moisture"),
            avg("soil_temperature").alias("avg_soil_temperature"),
            avg("air_temperature").alias("avg_air_temperature"),
            avg("air_humidity").alias("avg_air_humidity"),
            avg("moisture_min").alias("moisture_min"),
            avg("moisture_max").alias("moisture_max"),
            avg("recommended_water_mm").alias("recommended_water_mm"),
            spark_max("processing_time").alias("last_update"),
        )
        .withColumn(
            "water_status",
            compute_water_status(source_col="avg_soil_moisture"),
        )
    )


def create_farm_summary(df):
    return (
        df
        .groupBy("farm_id", "farm_name")
        .agg(
            countDistinct("zone_id").alias("zones"),
            countDistinct("sensor_id").alias("sensors"),
            avg("soil_moisture").alias("avg_soil_moisture"),
            avg("air_temperature").alias("avg_air_temperature"),
            avg("air_humidity").alias("avg_air_humidity"),
            spark_sum(when(col("water_status") == "Dry", 1).otherwise(0)).alias("dry_readings"),
            spark_sum(when(col("water_status") == "Optimal", 1).otherwise(0)).alias("healthy_readings"),
            spark_sum(when(col("water_status") == "Overwatered", 1).otherwise(0)).alias("overwatered_readings"),
            spark_max("processing_time").alias("last_update"),
        )
    )


def create_irrigation_alerts(df):
    return (
        df
        .filter(col("water_status") != "Optimal")
        .select(
            "reading_id",       
            "farm_name",
            "zone_name",
            "crop_name",
            "soil_moisture",
            "water_status",
            "recommended_water_mm",
            "event_time",
            "processing_time",
        )
    )


def upsert_latest(spark, new_summary_df, path, key_column, timestamp_column="last_update"):
    try:
        existing_df = spark.read.parquet(path)
        combined_df = existing_df.unionByName(new_summary_df, allowMissingColumns=True)
    except Exception:
        combined_df = new_summary_df

    window = Window.partitionBy(key_column).orderBy(col(timestamp_column).desc())

    deduped_df = (
        combined_df
        .withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    (
        deduped_df.write
        .format("parquet")
        .mode("overwrite")
        .save(path)
    )