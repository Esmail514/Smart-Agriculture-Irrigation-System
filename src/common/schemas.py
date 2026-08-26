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
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
])

silver_schema = StructType(bronze_schema.fields + [
    StructField("moisture_min", DoubleType()),
    StructField("moisture_max", DoubleType()),
    StructField("recommended_water_mm", DoubleType()),
    StructField("event_time", TimestampType()),
    StructField("processing_time", TimestampType()),
    StructField("water_status", StringType()),
])