from common.ch_sync import atomic_replace
from common.snapshots import read_latest_snapshot
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
    STREAM_TRIGGER_INTERVAL,
)
from common.logger import get_logger, setup_logging
from common.session import create_spark, ensure_stream_paths
from common.schemas import zone_summary_schema, farm_summary_schema, alerts_schema, silver_schema
from common.config import SILVER_SENSOR_PATH, POSTGRES_JDBC_URL, POSTGRES_JDBC_PROPERTIES


def sync_dimensions(spark):
    for name, columns in {
        "farm": "farm_id, farm_name, city, district, latitude, longitude, total_area_hectare",
        "zone": "zone_id, farm_id, crop_id, zone_name, area_hectare, soil_type",
        "crop": "crop_id, crop_name, optimal_moisture_min, optimal_moisture_max",
        "sensor": "sensor_id, zone_id, sensor_code",
    }.items():
        source = {"farm": "farms", "zone": "zones", "crop": "crops", "sensor": "sensors"}[name]
        df = spark.read.jdbc(POSTGRES_JDBC_URL, f"(SELECT {columns} FROM {source}) AS dims",
                             properties=POSTGRES_JDBC_PROPERTIES)
        atomic_replace(spark, df, f"agri_gold.dim_{name}", _jdbc_options())


def write_sensor_facts(spark, df, batch_id):
    if not df.take(1):
        return
    _jdbc_append(df.select("reading_id", "sensor_id", "zone_id", "farm_id", "crop_id",
                           "soil_moisture", "soil_temperature", "air_temperature", "air_humidity",
                           "event_time", "processing_time", "water_status"), "agri_gold.fact_sensor_readings")

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
    if not batch_df.take(1):
        return
    _jdbc_append(batch_df, "agri_gold.zone_summary_history")
    if batch_id % 20 == 0:
        sync_dimensions(spark)
    # Mutable snapshots are batch-read in full; file streaming would see only
    # newly discovered files and lose farms/sensors on partial microbatches.
    snapshot = read_latest_snapshot(spark, GOLD_ZONE_LATEST_PATH, schema=zone_summary_schema)
    if snapshot is None:
        raise RuntimeError("Zone history exists but no latest snapshot is published")
    _jdbc_truncate_and_insert(spark, snapshot,
                              "agri_gold.zone_summary_latest")
    logger.info("zone_history_written batch_id=%s", batch_id)


def write_zone_latest(spark, batch_df, batch_id):
    if not batch_df.take(1):
        return
    _jdbc_truncate_and_insert(spark, batch_df, "agri_gold.zone_summary_latest")
    logger.info("zone_latest_written batch_id=%s", batch_id)


def write_farm_history(spark, batch_df, batch_id):
    if not batch_df.take(1):
        return
    _jdbc_append(batch_df, "agri_gold.farm_summary_history")
    snapshot = read_latest_snapshot(spark, GOLD_FARM_LATEST_PATH, schema=farm_summary_schema)
    if snapshot is None:
        raise RuntimeError("Farm history exists but no latest snapshot is published")
    _jdbc_truncate_and_insert(spark, snapshot,
                              "agri_gold.farm_summary_latest")
    logger.info("farm_history_written batch_id=%s", batch_id)


def write_farm_latest(spark, batch_df, batch_id):
    if not batch_df.take(1):
        return
    _jdbc_truncate_and_insert(spark, batch_df, "agri_gold.farm_summary_latest")
    logger.info("farm_latest_written batch_id=%s", batch_id)


def write_alerts(spark, batch_df, batch_id):
    if not batch_df.take(1):
        return
    _jdbc_append(batch_df, "agri_gold.irrigation_alerts")
    logger.info("alerts_written batch_id=%s", batch_id)


def main():
    spark = create_spark("Gold To ClickHouse")
    spark.sparkContext.setLogLevel("WARN")
    ensure_stream_paths(spark, GOLD_ZONE_HISTORY_PATH, GOLD_FARM_HISTORY_PATH,
                        GOLD_ALERTS_PATH, SILVER_SENSOR_PATH)

    zone_schema = zone_summary_schema
    farm_schema = farm_summary_schema

    zone_hist_df = spark.readStream.schema(zone_schema).parquet(GOLD_ZONE_HISTORY_PATH)
    farm_hist_df = spark.readStream.schema(farm_schema).parquet(GOLD_FARM_HISTORY_PATH)
    alerts_df = spark.readStream.schema(alerts_schema).parquet(GOLD_ALERTS_PATH)

    def get_checkpoint(name):
        return f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/ch_{name}_v2"

    zone_hist_q = (
        zone_hist_df.writeStream
        .trigger(processingTime=STREAM_TRIGGER_INTERVAL)
        .foreachBatch(lambda df, bid: write_zone_history(spark, df, bid))
        .outputMode("append")
        .option("checkpointLocation", get_checkpoint("zone"))
        .start()
    )

    sensor_q = (
        spark.readStream.schema(silver_schema).parquet(SILVER_SENSOR_PATH)
        .writeStream
        .trigger(processingTime=STREAM_TRIGGER_INTERVAL)
        .foreachBatch(lambda df, bid: write_sensor_facts(spark, df, bid))
        .option("checkpointLocation", get_checkpoint("sensor_facts_v2"))
        .start()
    )

    farm_hist_q = (
        farm_hist_df.writeStream
        .trigger(processingTime=STREAM_TRIGGER_INTERVAL)
        .foreachBatch(lambda df, bid: write_farm_history(spark, df, bid))
        .outputMode("append")
        .option("checkpointLocation", get_checkpoint("farm_hist"))
        .start()
    )

    alerts_q = (
        alerts_df.writeStream
        .trigger(processingTime=STREAM_TRIGGER_INTERVAL)
        .foreachBatch(lambda df, bid: write_alerts(spark, df, bid))
        .outputMode("append")
        .option("checkpointLocation", get_checkpoint("alerts"))
        .start()
    )

    queries = [
        (zone_hist_q, "zone_hist"),
        (sensor_q, "sensor_facts"),
        (farm_hist_q, "farm_hist"),
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
