from pyspark.sql import SparkSession

from common.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


def ensure_stream_paths(spark, *paths):
    """Create empty stream inputs; explicit schemas support a clean first run."""
    for path in paths:
        target = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
        fs = target.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        if not fs.exists(target) and not fs.mkdirs(target):
            raise RuntimeError(f"Cannot initialize stream input {path}")


def create_spark(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
