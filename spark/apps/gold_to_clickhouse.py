from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Gold To ClickHouse")

    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio_admin")
    .config("spark.hadoop.fs.s3a.secret.key", "M1n10_S3cur3_Pass")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/analytics"

CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

zone_df = (
    spark.readStream
    .parquet(
        "s3a://agri-lakehouse/gold/zone_summary"
    )
)

alerts_df = (
    spark.readStream
    .parquet(
        "s3a://agri-lakehouse/gold/irrigation_alerts"
    )
)


def write_zone(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    (
        batch_df.write
        .format("jdbc")
        .mode("append")
        .option("driver", CLICKHOUSE_DRIVER)
        .option("url", CLICKHOUSE_URL)
        .option("dbtable", "zone_metrics")
        .save()
    )


def write_alerts(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    (
        batch_df.write
        .format("jdbc")
        .mode("append")
        .option("driver", CLICKHOUSE_DRIVER)
        .option("url", CLICKHOUSE_URL)
        .option("dbtable", "irrigation_alerts")
        .save()
    )


zone_query = (
    zone_df.writeStream
    .foreachBatch(write_zone)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "s3a://agri-lakehouse/checkpoints/ch_zone"
    )
    .start()
)

alerts_query = (
    alerts_df.writeStream
    .foreachBatch(write_alerts)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "s3a://agri-lakehouse/checkpoints/ch_alerts"
    )
    .start()
)

spark.streams.awaitAnyTermination()