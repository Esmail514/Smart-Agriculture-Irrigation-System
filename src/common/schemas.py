from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sensor_schema = StructType([
    StructField("schema_version", IntegerType()),
    StructField("sensor_id", LongType()),
    StructField("sensor_code", StringType()),
    StructField("zone_id", LongType()),
    StructField("zone_name", StringType()),
    StructField("farm_id", LongType()),
    StructField("farm_name", StringType()),
    StructField("crop_id", LongType()),
    StructField("crop_name", StringType()),
    StructField("soil_moisture", DoubleType()),
    StructField("soil_temperature", DoubleType()),
    StructField("air_temperature", DoubleType()),
    StructField("air_humidity", DoubleType()),
    StructField("reading_id", StringType()),
    StructField("timestamp", StringType()),
])

bronze_schema = StructType(sensor_schema.fields + [
    StructField("raw_value", StringType()),
    StructField("kafka_timestamp", TimestampType()),
    StructField("kafka_topic", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("event_time", TimestampType()),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
])

silver_schema = StructType(sensor_schema.fields + [
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("day", IntegerType()),
    StructField("moisture_min", DoubleType()),
    StructField("moisture_max", DoubleType()),
    StructField("recommended_water_mm", DoubleType()),
    StructField("event_time", TimestampType()),
    StructField("processing_time", TimestampType()),
    StructField("water_status", StringType()),
])

# Explicit contracts: startup never depends on an existing Parquet file.
zone_summary_schema = StructType([
    StructField(name, LongType() if name in {"zone_id", "farm_id", "crop_id", "sensor_count", "snapshot_id"}
                else TimestampType() if name == "last_update"
                else StringType() if name in {"zone_name", "farm_name", "crop_name", "water_status"}
                else DoubleType())
    for name in ["zone_id", "zone_name", "farm_id", "farm_name", "crop_id", "crop_name",
                 "sensor_count", "avg_soil_moisture", "avg_soil_temperature", "avg_air_temperature",
                 "avg_air_humidity", "moisture_min", "moisture_max", "recommended_water_mm", "last_update", "water_status", "snapshot_id"]
])
farm_summary_schema = StructType([
    StructField(name, StringType() if name == "farm_name" else TimestampType() if name == "last_update"
                else DoubleType() if name.startswith("avg_") else LongType())
    for name in ["farm_id", "farm_name", "zones", "sensors", "avg_soil_moisture", "avg_air_temperature",
                 "avg_air_humidity", "dry_readings", "healthy_readings", "overwatered_readings", "last_update", "snapshot_id"]
])
alerts_schema = StructType([silver_schema[name] for name in [
    "reading_id", "farm_name", "zone_name", "crop_name", "soil_moisture", "water_status",
    "recommended_water_mm", "event_time", "processing_time"
]])
