import logging

from utils.session import create_spark
from utils.schemas import bronze_schema
from utils.rules import load_irrigation_rules
from utils.transformations import (
    split_valid_and_rejected,
    enrich_with_rules,
    add_event_time,
    add_processing_time,
    add_water_status,
)
from utils.config import (
    BRONZE_SENSOR_PATH,
    SILVER_SENSOR_PATH,
    SILVER_REJECTED_PATH,
    CHECKPOINT_SILVER_SENSOR,
    CHECKPOINT_SILVER_REJECTED,
    STREAM_TRIGGER_INTERVAL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("silver_writer")


def main():
    spark = create_spark("Silver Layer - Sensor Data")
    spark.sparkContext.setLogLevel("WARN")

    rules_df = load_irrigation_rules(spark)

    bronze_df = (
        spark.readStream
        .schema(bronze_schema)
        .parquet(BRONZE_SENSOR_PATH)
    )

    valid_df, rejected_df = split_valid_and_rejected(bronze_df)

    silver_df = enrich_with_rules(valid_df, rules_df)
    silver_df = add_event_time(silver_df)

    silver_df = silver_df.withWatermark("event_time", "10 minutes")

    silver_df = silver_df.dropDuplicates(["reading_id"])

    silver_df = add_processing_time(silver_df)
    silver_df = add_water_status(silver_df)

    silver_query = (
        silver_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_SENSOR_PATH)
        .option("checkpointLocation", CHECKPOINT_SILVER_SENSOR)
        .partitionBy("year", "month", "crop_name")
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


if __name__ == "__main__":
    main()