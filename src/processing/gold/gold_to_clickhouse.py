from common.logger import setup_logging, get_logger

from common.config import (
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_JDBC_DRIVER,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    GOLD_ZONE_HISTORY_PATH,
    GOLD_ALERTS_PATH,
    LAKEHOUSE_BUCKET,
)
from common.session import create_spark

setup_logging()
logger = get_logger("gold_to_clickhouse")


def write_zone(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df.write
        .format("jdbc")
        .mode("append")
        .option("driver", CLICKHOUSE_JDBC_DRIVER)
        .option("url", CLICKHOUSE_JDBC_URL)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("dbtable", "agri_gold.zone_summary_history")
        .save()
    )
    logger.info("zone_summary_written_to_clickhouse batch_id=%s rows=%d", batch_id, batch_df.count())


def write_alerts(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df.write
        .format("jdbc")
        .mode("append")
        .option("driver", CLICKHOUSE_JDBC_DRIVER)
        .option("url", CLICKHOUSE_JDBC_URL)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("dbtable", "agri_gold.irrigation_alerts")
        .save()
    )
    logger.info("alerts_written_to_clickhouse batch_id=%s rows=%d", batch_id, batch_df.count())


def main():
    spark = create_spark("Gold To ClickHouse")
    spark.sparkContext.setLogLevel("WARN")

    # Spark cannot infer schemas for streaming sources - derive them from a
    # static read of the same (already-populated) Gold directories.
    zone_schema = spark.read.parquet(GOLD_ZONE_HISTORY_PATH).schema
    alerts_schema = spark.read.parquet(GOLD_ALERTS_PATH).schema

    zone_df = spark.readStream.schema(zone_schema).parquet(GOLD_ZONE_HISTORY_PATH)
    alerts_df = spark.readStream.schema(alerts_schema).parquet(GOLD_ALERTS_PATH)

    checkpoints = {
        "zone": f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/ch_zone",
        "alerts": f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/ch_alerts",
    }

    zone_query = (
        zone_df.writeStream
        .foreachBatch(write_zone)
        .outputMode("append")
        .option("checkpointLocation", checkpoints["zone"])
        .start()
    )

    alerts_query = (
        alerts_df.writeStream
        .foreachBatch(write_alerts)
        .outputMode("append")
        .option("checkpointLocation", checkpoints["alerts"])
        .start()
    )

    logger.info("gold_to_clickhouse_streams_started")

    try:
        spark.streams.awaitAnyTermination()
    except Exception:
        logger.exception("gold_to_clickhouse_failed")
        raise
    finally:
        for query, label in [(zone_query, "zone"), (alerts_query, "alerts")]:
            logger.info(
                "query_status label=%s active=%s last_progress=%s",
                label, query.isActive, query.lastProgress,
            )
        spark.stop()


if __name__ == "__main__":
    main()
