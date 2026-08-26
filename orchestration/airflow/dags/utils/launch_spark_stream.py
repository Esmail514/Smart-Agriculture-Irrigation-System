import logging

import docker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("launch_spark_stream")

SPARK_MASTER_CONTAINER = "agri-spark-master"


def launch_stream_job(job_filename, **context):
    """
    يُستدعى من PythonOperator بكل DAG من الثلاثة (bronze/silver/gold).
    يحتاج /var/run/docker.sock مركّب داخل حاوية airflow-worker.
    """
    client = docker.from_env()
    container = client.containers.get(SPARK_MASTER_CONTAINER)

    exit_code, output = container.exec_run(
        cmd=["bash", "/opt/spark/scripts/launch_stream_job.sh", job_filename],
    )
    output_text = output.decode() if output else ""
    logger.info("launch_result job=%s exit_code=%s\n%s", job_filename, exit_code, output_text)

    if exit_code != 0:
        raise RuntimeError(
            f"فشل إطلاق {job_filename} - exit_code={exit_code}\n{output_text}"
        )


def run_batch_job(job_filename, **context):
    """
    For synchronous batch jobs that are NOT Spark-based (e.g. Python scripts
    that don't use spark-submit). Uses run_spark_batch_job.sh which handles
    --py-files packaging automatically.
    """
    return run_spark_batch_job(job_filename, **context)


def run_spark_batch_job(job_path, **context):
    """
    لـ PySpark batch jobs (Silver/Gold الطقس مثلاً) - عبر run_spark_batch_job.sh
    المتزامن (spark-submit + --py-files، بدون nohup/خلفية). exec_run هنا
    ينتظر فعلياً حتى ينتهي spark-submit قبل ما يرجّع النتيجة.
    """
    client = docker.from_env()
    container = client.containers.get(SPARK_MASTER_CONTAINER)

    exit_code, output = container.exec_run(
        cmd=["bash", "/opt/spark/scripts/run_spark_batch_job.sh", job_path],
    )
    output_text = output.decode() if output else ""
    logger.info("spark_batch_result job=%s exit_code=%s\n%s", job_path, exit_code, output_text)

    if exit_code != 0:
        raise RuntimeError(
            f"فشل تشغيل {job_path} - exit_code={exit_code}\n{output_text}"
        )
