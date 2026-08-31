from common.ch_sync import atomic_replace
from common.config import (
    CLICKHOUSE_JDBC_DRIVER,
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
    GOLD_ALERTS_PATH,
    GOLD_FARM_HISTORY_PATH,
    GOLD_FARM_LATEST_PATH,
    GOLD_ZONE_HISTORY_PATH,
    GOLD_ZONE_LATEST_PATH,
    LAKEHOUSE_BUCKET,
)
from common.logger import get_logger, setup_logging
from common.session import create_spark

setup_logging()
logger = get_logger("gold_to_clickhouse")


def _jdbc_options():
    return {
        "driver": CLICKHOUSE_JDBC_DRIVER,
        "url": CLICKHOUSE_JDBC_URL,
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
    }


def _jdbc_append(batch_df, table_name):
    (
        batch_df.write
        .format("jdbc")
        .mode("append")
        .option("driver", CLICKHOUSE_JDBC_DRIVER)
        .option("url", CLICKHOUSE_JDBC_URL)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("dbtable", table_name)
        .save()
    )


def _jdbc_truncate_and_insert(spark, batch_df, table_name):
    atomic_replace(spark, batch_df, table_name, _jdbc_options())


def write_zone_history(spark, batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    _jdbc_append(batch_df, "agri_gold.zone_summary_history")
    logger.info("zone_history_written batch_id=%s rows=%d", batch_id, batch_df.count())


def write_zone_latest(spark, batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    _jdbc_truncate_and_insert(spark, batch_df, "agri_gold.zone_summary_latest")
    logger.info("zone_latest_written batch_id=%s rows=%d", batch_id, batch_df.count())


def write_farm_history(spark, batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    _jdbc_append(batch_df, "agri_gold.farm_summary_history")
    logger.info("farm_history_written batch_id=%s rows=%d", batch_id, batch_df.count())


def write_farm_latest(spark, batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    _jdbc_truncate_and_insert(spark, batch_df, "agri_gold.farm_summary_latest")
    logger.info("farm_latest_written batch_id=%s rows=%d", batch_id, batch_df.count())


def write_alerts(spark, batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    _jdbc_append(batch_df, "agri_gold.irrigation_alerts")
    logger.info("alerts_written batch_id=%s rows=%d", batch_id, batch_df.count())


def main():
    spark = create_spark("Gold To ClickHouse")
    spark.sparkContext.setLogLevel("WARN")

    zone_schema = spark.read.parquet(GOLD_ZONE_HISTORY_PATH).schema
    alerts_schema = spark.read.parquet(GOLD_ALERTS_PATH).schema
    farm_schema = spark.read.parquet(GOLD_FARM_HISTORY_PATH).schema

    zone_hist_df = spark.readStream.schema(zone_schema).parquet(GOLD_ZONE_HISTORY_PATH)
    zone_latest_df = spark.readStream.schema(zone_schema).parquet(GOLD_ZONE_LATEST_PATH)
    farm_hist_df = spark.readStream.schema(farm_schema).parquet(GOLD_FARM_HISTORY_PATH)
    farm_latest_df = spark.readStream.schema(farm_schema).parquet(GOLD_FARM_LATEST_PATH)
    alerts_df = spark.readStream.schema(alerts_schema).parquet(GOLD_ALERTS_PATH)

    cp = lambda name: f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/ch_{name}"

    zone_hist_q = (
        zone_hist_df.writeStream
        .foreachBatch(lambda df, bid: write_zone_history(spark, df, bid))
        .outputMode("append")
        .option("checkpointLocation", cp("zone"))
        .start()
    )

    zone_latest_q = (
        zone_latest_df.writeStream
        .foreachBatch(lambda df, bid: write_zone_latest(spark, df, bid))
        .outputMode("append")
        .option("checkpointLocation", cp("zone_latest"))
        .start()
    )

    farm_hist_q = (
        farm_hist_df.writeStream
        .foreachBatch(lambda df, bid: write_farm_history(spark, df, bid))
        .outputMode("append")
        .option("checkpointLocation", cp("farm_hist"))
        .start()
    )

    farm_latest_q = (
        farm_latest_df.writeStream
        .foreachBatch(lambda df, bid: write_farm_latest(spark, df, bid))
        .outputMode("append")
        .option("checkpointLocation", cp("farm_latest"))
        .start()
    )

    alerts_q = (
        alerts_df.writeStream
        .foreachBatch(lambda df, bid: write_alerts(spark, df, bid))
        .outputMode("append")
        .option("checkpointLocation", cp("alerts"))
        .start()
    )

    queries = [
        (zone_hist_q, "zone_hist"),
        (zone_latest_q, "zone_latest"),
        (farm_hist_q, "farm_hist"),
        (farm_latest_q, "farm_latest"),
        (alerts_q, "alerts"),
    ]
    logger.info("gold_to_clickhouse_streams_started count=%d", len(queries))

    try:
        spark.streams.awaitAnyTermination()
    except Exception:
        logger.exception("gold_to_clickhouse_failed")
        raise
    finally:
        for query, label in queries:
            try:
                if query.isActive:
                    query.stop()
                logger.info(
                    "query_stopped label=%s active=%s last_progress=%s",
                    label, query.isActive, query.lastProgress,
                )
            except Exception:
                logger.exception("query_stop_failed label=%s", label)
        spark.stop()


if __name__ == "__main__":
    main()
