"""One checkpoint, sensor-grain state, and replayable Gold batch outputs."""
from datetime import timedelta
from pyspark.sql.functions import col, lit, max as spark_max
from common.logger import setup_logging, get_logger
from common.session import create_spark, ensure_stream_paths
from common.schemas import silver_schema
from common.snapshots import read_latest_snapshot, publish_snapshot
from common.gold_transformations import (
    create_zone_summary, create_farm_summary, create_irrigation_alerts, upsert_latest,
)
from common.config import (
    SILVER_SENSOR_PATH, GOLD_ZONE_HISTORY_PATH, GOLD_ZONE_LATEST_PATH,
    GOLD_FARM_HISTORY_PATH, GOLD_FARM_LATEST_PATH, GOLD_ALERTS_PATH,
    LAKEHOUSE_BUCKET, STREAM_TRIGGER_INTERVAL,
)

setup_logging()
logger = get_logger("gold_streaming")
SENSOR_STATE_PATH = f"s3a://{LAKEHOUSE_BUCKET}/gold/sensor_latest_v2"
CHECKPOINT = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/gold_contract_v2"


def write_gold_batch(spark, batch_df, batch_id):
    if not batch_df.take(1):
        return
    upsert_latest(spark, batch_df, SENSOR_STATE_PATH, "sensor_id", "event_time")
    current = read_latest_snapshot(spark, SENSOR_STATE_PATH, schema=silver_schema)
    if current is None:
        raise RuntimeError("Sensor snapshot was not published")
    current = current.cache()
    try:
        max_row = current.agg(spark_max("event_time")).first()
        newest_event = max_row[0] if max_row else None
        if newest_event is None:
            return
        # Offline sensors retain their last_seen state but cannot keep skewing
        # a live zone average when other sensors continue to report.
        recent = current.filter(col("event_time") >= lit(newest_event - timedelta(minutes=10)))
        outputs = []
        for summary, history, latest in (
            (create_zone_summary(recent), GOLD_ZONE_HISTORY_PATH, GOLD_ZONE_LATEST_PATH),
            (create_farm_summary(recent), GOLD_FARM_HISTORY_PATH, GOLD_FARM_LATEST_PATH),
        ):
            summary = summary.withColumn("snapshot_id", lit(batch_id + 1).cast("long"))
            # These summarize all recent sensors, so publish directly.
            # Upserting by max event time would discard a late sensor update
            # whenever a different sensor already holds the same max time.
            publish_snapshot(spark, summary, latest)
            outputs.append((summary, history))
        # Publish BOTH current snapshot pointers before history becomes visible
        # to ClickHouse's file streams. Otherwise history can trigger a refresh
        # against an absent/previous pointer with no subsequent refresh event.
        for summary, history in outputs:
            # Stable logical batch path on replay; version folders are never
            # placed under a history stream's discovery root.
            summary.write.mode("overwrite").parquet(f"{history}/batch_{batch_id:020d}")
        create_irrigation_alerts(batch_df).write.mode("overwrite").parquet(
            f"{GOLD_ALERTS_PATH}/batch_{batch_id:020d}")
        logger.info("gold_batch_written batch_id=%s", batch_id)
    finally:
        current.unpersist()


def main():
    spark = create_spark("Gold Layer - Sensor State & Summaries")
    query = None
    try:
        ensure_stream_paths(spark, SILVER_SENSOR_PATH)
        query = (spark.readStream.schema(silver_schema).parquet(SILVER_SENSOR_PATH)
                 .writeStream.foreachBatch(lambda df, bid: write_gold_batch(spark, df, bid))
                 .option("checkpointLocation", CHECKPOINT)
                 .trigger(processingTime=STREAM_TRIGGER_INTERVAL).start())
        query.awaitTermination()
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
