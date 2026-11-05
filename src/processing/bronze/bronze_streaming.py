from common.logger import setup_logging, get_logger

from pyspark.sql.functions import col, current_timestamp, dayofmonth, from_json, month, to_timestamp, year, udf, coalesce, lit
from pyspark.sql.types import StringType

from common.config import (
    BRONZE_SENSOR_PATH,
    BRONZE_SENSOR_QUARANTINE_PATH,
    CHECKPOINT_BRONZE_QUARANTINE,
    CHECKPOINT_BRONZE_SENSOR,
    KAFKA_BOOTSTRAP_SERVERS,
    SENSOR_TOPIC,
    TIMESTAMP_FORMAT,
    STREAM_TRIGGER_INTERVAL,
)
from common.schemas import sensor_schema
from common.transformations import split_valid_and_rejected
from common.sensor_contract import sensor_ingress_error
from common.sensor_registry import load_sensor_registry, validate_sensor_registry
from common.session import create_spark

setup_logging()
logger = get_logger("bronze_streaming")
ingress_error_udf = udf(sensor_ingress_error, StringType())


def route_sensor_rows(parsed_df, registry):
    """Partition every parsed input into exactly one admission output."""
    parsed_df = validate_sensor_registry(parsed_df, registry)
    structurally_valid, rejected = split_valid_and_rejected(parsed_df)
    valid_df = structurally_valid.filter(col("contract_error").isNull() & col("registry_valid")).drop("contract_error", "registry_valid")
    quarantine_df = (parsed_df.filter(col("contract_error").isNotNull())
                     .withColumn("rejection_reason", col("contract_error")))
    quarantine_df = quarantine_df.unionByName(
        rejected.filter(col("contract_error").isNull()).drop("rejected_at"), allowMissingColumns=True)
    quarantine_df = quarantine_df.unionByName(
        structurally_valid.filter(col("contract_error").isNull() & ~col("registry_valid"))
        .withColumn("rejection_reason", lit("unknown_sensor_or_relationship")), allowMissingColumns=True)
    return valid_df, quarantine_df


def main():
    spark = create_spark("Bronze Layer - Sensor Data")
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", SENSOR_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS raw_value", "timestamp AS kafka_timestamp",
                    "topic AS kafka_topic", "partition AS kafka_partition", "offset AS kafka_offset")
        # Both sinks evaluate this plan independently. Wall-clock validation
        # could put a near-future event in neither output; broker append time
        # is immutable and gives the same decision on both queries and replay.
        .withColumn("contract_error", ingress_error_udf("raw_value", "kafka_timestamp"))
        .withColumn("sensor", from_json(col("raw_value"), sensor_schema))
        .select("sensor.*", "raw_value", "kafka_timestamp", "kafka_topic", "kafka_partition", "kafka_offset", "contract_error")
    )


    valid_df, quarantine_df = route_sensor_rows(parsed_df, load_sensor_registry(spark))

    bronze_df = (
        valid_df
        .withColumn("schema_version", coalesce(col("schema_version"), lit(1)))
        .withColumn("event_time", to_timestamp(col("timestamp"), TIMESTAMP_FORMAT))
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
    )

    quarantine_out_df = (
        quarantine_df
        .select("raw_value", "kafka_timestamp", "kafka_topic", "kafka_partition", "kafka_offset", "rejection_reason")
        .withColumn("quarantined_at", current_timestamp())
    )

    bronze_query = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .trigger(processingTime=STREAM_TRIGGER_INTERVAL)
        .option("path", BRONZE_SENSOR_PATH)
        .option("checkpointLocation", CHECKPOINT_BRONZE_SENSOR)
        .partitionBy("year", "month", "day")
        .start()
    )

    quarantine_query = (
        quarantine_out_df.writeStream
        .format("parquet")
        .outputMode("append")
        .trigger(processingTime=STREAM_TRIGGER_INTERVAL)
        .option("path", BRONZE_SENSOR_QUARANTINE_PATH)
        .option("checkpointLocation", CHECKPOINT_BRONZE_QUARANTINE)
        .start()
    )

    logger.info(
        "bronze_streams_started topic=%s bronze_path=%s quarantine_path=%s",
        SENSOR_TOPIC, BRONZE_SENSOR_PATH, BRONZE_SENSOR_QUARANTINE_PATH,
    )

    try:
        spark.streams.awaitAnyTermination()
    except Exception:
        logger.exception("bronze_stream_failed")
        raise
    finally:
        for query, label in [(bronze_query, "bronze"), (quarantine_query, "quarantine")]:
            logger.info(
                "query_status label=%s active=%s last_progress=%s",
                label, query.isActive, query.lastProgress,
            )
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
