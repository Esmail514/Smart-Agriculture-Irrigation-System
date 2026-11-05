from pyspark.sql.functions import broadcast, col, coalesce, current_timestamp, lit, to_timestamp, trim, when

from common.config import TIMESTAMP_FORMAT
from common.sensor_contract import RANGES

VALID_SOIL_MOISTURE_RANGE = RANGES["soil_moisture"]
VALID_TEMPERATURE_RANGE = RANGES["air_temperature"]
VALID_HUMIDITY_RANGE = RANGES["air_humidity"]


def sensor_validity_condition():
    """Fail closed: SQL NULL must never silently disappear from both outputs."""
    condition = (
        (col("sensor_id") > 0)
        & (col("zone_id") > 0)
        & (col("farm_id") > 0)
        & (col("crop_id") > 0)
        & (trim(col("reading_id")) != "")
        & to_timestamp(col("timestamp"), TIMESTAMP_FORMAT).isNotNull()
        & col("soil_moisture").between(*VALID_SOIL_MOISTURE_RANGE)
        & col("soil_temperature").between(*VALID_TEMPERATURE_RANGE)
        & col("air_temperature").between(*VALID_TEMPERATURE_RANGE)
        & col("air_humidity").between(*VALID_HUMIDITY_RANGE)
    )

    for field in ("sensor_code", "zone_name", "farm_name", "crop_name"):
        condition = condition & (trim(col(field)) != "")
    return coalesce(condition, lit(False))


def split_valid_and_rejected(df):
    condition = sensor_validity_condition()
    valid_df = df.filter(condition)
    rejected_df = (df.filter(~condition)
                   .withColumn("rejection_reason", lit("invalid_sensor_contract"))
                   .withColumn("rejected_at", current_timestamp()))

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

    
