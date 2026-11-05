from common.logger import setup_logging, get_logger

from common.session import create_spark, ensure_stream_paths
from common.schemas import bronze_schema, silver_schema
from common.rules import load_irrigation_rules
from common.transformations import (
    split_valid_and_rejected,
    enrich_with_rules,
    add_event_time,
    add_processing_time,
    add_water_status,
)
from common.config import (
    BRONZE_SENSOR_PATH,
    SILVER_SENSOR_PATH,
    SILVER_REJECTED_PATH,
    CHECKPOINT_SILVER_SENSOR,
    CHECKPOINT_SILVER_REJECTED,
    STREAM_TRIGGER_INTERVAL,
)

setup_logging()
logger = get_logger("silver_streaming")


def main():
    spark = create_spark("Silver Layer - Sensor Data")
    spark.sparkContext.setLogLevel("WARN")

    rules_df = load_irrigation_rules(spark)
    ensure_stream_paths(spark, BRONZE_SENSOR_PATH)

    bronze_df = (
        spark.readStream
        .schema(bronze_schema)
        .parquet(BRONZE_SENSOR_PATH)
    )

    valid_df, rejected_df = split_valid_and_rejected(bronze_df)

    silver_df = enrich_with_rules(valid_df, rules_df)
    silver_df = add_event_time(silver_df)

    silver_df = silver_df.withWatermark("event_time", "10 minutes")

    silver_df = silver_df.dropDuplicates(["reading_id", "event_time"])

    silver_df = add_processing_time(silver_df)
    silver_df = add_water_status(silver_df)
    silver_df = silver_df.select(*silver_schema.fieldNames())

    silver_query = (
        silver_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_SENSOR_PATH)
        .option("checkpointLocation", CHECKPOINT_SILVER_SENSOR)
        .partitionBy("year", "month", "day")
        .trigger(processingTime=STREAM_TRIGGER_INTERVAL)
        .start()
    )

    rejected_query = (
        rejected_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_REJECTED_PATH)
        .option("checkpointLocation", CHECKPOINT_SILVER_REJECTED)
        .trigger(processingTime=STREAM_TRIGGER_INTERVAL)
        .start()
    )

    logger.info(
        "silver_streams_started silver_path=%s rejected_path=%s",
        SILVER_SENSOR_PATH, SILVER_REJECTED_PATH,
    )

    try:
        spark.streams.awaitAnyTermination()
    except Exception:
        logger.exception("silver_stream_failed")
        raise
    finally:
        for query, label in [(silver_query, "silver"), (rejected_query, "rejected")]:
            logger.info(
                "query_status label=%s active=%s last_progress=%s",
                label, query.isActive, query.lastProgress,
            )
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
