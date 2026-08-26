from pyspark.sql.functions import broadcast, col, current_timestamp, to_timestamp, when

from common.config import TIMESTAMP_FORMAT

VALID_SOIL_MOISTURE_RANGE = (0, 100)
VALID_TEMPERATURE_RANGE = (-10, 70)
VALID_HUMIDITY_RANGE = (0, 100)


def split_valid_and_rejected(df):
    condition = (
        col("sensor_id").isNotNull()
        & col("timestamp").isNotNull()
        & col("soil_moisture").between(*VALID_SOIL_MOISTURE_RANGE)
        & col("soil_temperature").between(*VALID_TEMPERATURE_RANGE)
        & col("air_temperature").between(*VALID_TEMPERATURE_RANGE)
        & col("air_humidity").between(*VALID_HUMIDITY_RANGE)
    )

    valid_df = df.filter(condition)
    rejected_df = df.filter(~condition).withColumn("rejected_at", current_timestamp())

    return valid_df, rejected_df


def enrich_with_rules(sensor_df, rules_df):
    return sensor_df.join(broadcast(rules_df), on="crop_id", how="left")


def add_event_time(df):
    return df.withColumn("event_time", to_timestamp(col("timestamp"), TIMESTAMP_FORMAT))


def add_processing_time(df):
    return df.withColumn("processing_time", current_timestamp())


def add_water_status(df):
    return df.withColumn(
        "water_status",
        compute_water_status(source_col="soil_moisture"),
    )


def compute_water_status(source_col="soil_moisture"):
    """Return a Column computing water_status from moisture_min / moisture_max thresholds.

    Call as: df.withColumn("water_status", compute_water_status(source_col="avg_soil_moisture"))
    """
    return (
        when(col("moisture_min").isNull() | col("moisture_max").isNull(), "Unknown_NoRule")
        .when(col(source_col) < col("moisture_min"), "Dry")
        .when(col(source_col) > col("moisture_max"), "Overwatered")
        .otherwise("Optimal")
    )

    
