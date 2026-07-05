import logging

from utils.session import create_spark
from utils.schemas import silver_schema
from utils.gold_transformations import (
    create_zone_summary,
    create_farm_summary,
    create_irrigation_alerts,
    upsert_latest,
)
from utils.config import (
    SILVER_SENSOR_PATH,
    GOLD_ZONE_HISTORY_PATH,
    GOLD_ZONE_LATEST_PATH,
    GOLD_FARM_HISTORY_PATH,
    GOLD_FARM_LATEST_PATH,
    GOLD_ALERTS_PATH,
    CHECKPOINT_GOLD_ZONE,
    CHECKPOINT_GOLD_FARM,
    CHECKPOINT_GOLD_ALERTS,
    STREAM_TRIGGER_INTERVAL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("gold_writer")

spark = create_spark("Gold Layer - Summaries & Alerts")


def write_zone_summary(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    summary_df = create_zone_summary(batch_df).cache()
    try:
        summary_df.write.format("parquet").mode("append").save(GOLD_ZONE_HISTORY_PATH)
        upsert_latest(spark, summary_df, GOLD_ZONE_LATEST_PATH, key_column="zone_id")
        logger.info("zone_summary_written batch_id=%s rows=%d", batch_id, summary_df.count())
    finally:
        summary_df.unpersist()


def write_farm_summary(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    summary_df = create_farm_summary(batch_df).cache()
    try:
        summary_df.write.format("parquet").mode("append").save(GOLD_FARM_HISTORY_PATH)
        upsert_latest(spark, summary_df, GOLD_FARM_LATEST_PATH, key_column="farm_id")
        logger.info("farm_summary_written batch_id=%s rows=%d", batch_id, summary_df.count())
    finally:
        summary_df.unpersist()


def write_alerts(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    alert_df = create_irrigation_alerts(batch_df)
    alert_df.write.format("parquet").mode("append").save(GOLD_ALERTS_PATH)
    logger.info("alerts_written batch_id=%s rows=%d", batch_id, alert_df.count())


def main():
    spark.sparkContext.setLogLevel("WARN")

    silver_df = (
        spark.readStream
        .schema(silver_schema)
        .parquet(SILVER_SENSOR_PATH)
    )

    zone_query = (
        silver_df.writeStream
        .outputMode("append")
        .foreachBatch(write_zone_summary)
        .option("checkpointLocation", CHECKPOINT_GOLD_ZONE)
        .trigger(processingTime=STREAM_TRIGGER_INTERVAL)
        .start()
    )

    farm_query = (
        silver_df.writeStream
        .outputMode("append")
        .foreachBatch(write_farm_summary)
        .option("checkpointLocation", CHECKPOINT_GOLD_FARM)
        .trigger(processingTime=STREAM_TRIGGER_INTERVAL)
        .start()
    )

    alerts_query = (
        silver_df.writeStream
        .outputMode("append")
        .foreachBatch(write_alerts)
        .option("checkpointLocation", CHECKPOINT_GOLD_ALERTS)
        .trigger(processingTime=STREAM_TRIGGER_INTERVAL)
        .start()
    )

    logger.info("gold_streams_started")

    try:
        spark.streams.awaitAnyTermination()
    except Exception:
        logger.exception("gold_stream_failed")
        raise
    finally:
        for query, label in [(zone_query, "zone"), (farm_query, "farm"), (alerts_query, "alerts")]:
            logger.info(
                "query_status label=%s active=%s last_progress=%s",
                label, query.isActive, query.lastProgress,
            )


if __name__ == "__main__":
    main()