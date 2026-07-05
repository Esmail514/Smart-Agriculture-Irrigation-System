import logging

from pyspark.sql.functions import col, current_timestamp, dayofmonth, from_json, month, to_timestamp, year

from utils.config import (
    BRONZE_SENSOR_PATH,
    BRONZE_SENSOR_QUARANTINE_PATH,
    CHECKPOINT_BRONZE_QUARANTINE,
    CHECKPOINT_BRONZE_SENSOR,
    KAFKA_BOOTSTRAP_SERVERS,
    SENSOR_TOPIC,
    TIMESTAMP_FORMAT,
)
from utils.schemas import sensor_schema
from utils.session import create_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("bronze_writer")


def main():
    spark = create_spark("Bronze Layer - Sensor Data")
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", SENSOR_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS raw_value", "timestamp AS kafka_timestamp")
        .withColumn("sensor", from_json(col("raw_value"), sensor_schema))
    )


    valid_df = parsed_df.filter(
        col("sensor").isNotNull() & col("sensor.sensor_id").isNotNull()
    )
    quarantine_df = parsed_df.filter(
        col("sensor").isNull() | col("sensor.sensor_id").isNull()
    )

    bronze_df = (
        valid_df
        .select("sensor.*", "raw_value", "kafka_timestamp")
        .withColumn("event_time", to_timestamp(col("timestamp"), TIMESTAMP_FORMAT))
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
    )

    quarantine_out_df = (
        quarantine_df
        .select("raw_value", "kafka_timestamp")
        .withColumn("quarantined_at", current_timestamp())
    )

    bronze_query = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_SENSOR_PATH)
        .option("checkpointLocation", CHECKPOINT_BRONZE_SENSOR)
        .partitionBy("year", "month", "day")
        .start()
    )

    quarantine_query = (
        quarantine_out_df.writeStream
        .format("parquet")
        .outputMode("append")
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


if __name__ == "__main__":
    main()