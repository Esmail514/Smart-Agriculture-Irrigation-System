"""Submit bounded job paths; failures must fail the Airflow task."""
import logging

from utils.docker_exec import docker_exec

logger = logging.getLogger(__name__)
SPARK_MASTER_CONTAINER = "agri-spark-master"
STREAM_JOBS = frozenset({"bronze/bronze_streaming.py", "silver/silver_streaming.py",
                         "gold/gold_streaming.py", "gold/gold_to_clickhouse.py"})
BATCH_JOBS = frozenset({"weather/bronze/fetch_weather_bronze.py", "weather/silver/transform_weather_silver.py",
                       "weather/gold/aggregate_weather_gold.py", "weather/gold/sync_weather_clickhouse.py",
                       "weather/gold/sync_weather_alerts_clickhouse.py", "ml/predict_spark.py"})


def launch_stream_job(job_filename, **context):
    if job_filename not in STREAM_JOBS:
        raise ValueError("Unsupported streaming job")
    output = docker_exec(SPARK_MASTER_CONTAINER,
                         ["bash", "/opt/spark/scripts/launch_stream_job.sh", job_filename], timeout=120)
    logger.info("stream_launch job=%s result=%s", job_filename, output[-4000:])


def run_batch_job(job_filename, **context):
    return run_spark_batch_job(job_filename, **context)


def run_spark_batch_job(job_path, **context):
    if job_path not in BATCH_JOBS:
        raise ValueError("Unsupported batch job")
    output = docker_exec(SPARK_MASTER_CONTAINER,
                         ["bash", "/opt/spark/scripts/run_spark_batch_job.sh", job_path], timeout=1200)
    logger.info("batch_finished job=%s result=%s", job_path, output[-4000:])
