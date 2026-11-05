from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.launch_spark_stream import launch_stream_job  # noqa: E402

default_args = {
    "owner": "smart_agriculture",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

STREAMING_JOBS = [
    # ClickHouse sync launches FIRST: standalone-cluster scheduling is FIFO,
    # and launching it last meant bronze/silver/gold consumed every worker
    # slot before it registered (permanent executor starvation).
    "gold/gold_to_clickhouse.py",
    "bronze/bronze_streaming.py",
    "silver/silver_streaming.py",
    "gold/gold_streaming.py",
]


def _start_all_streaming(**context):
    for job in STREAMING_JOBS:
        launch_stream_job(job)


def _check_streaming_health(**context):
    import docker

    client = docker.from_env(timeout=30)
    try:
        container = client.containers.get("agri-spark-master")

        for job in STREAMING_JOBS:
            script_name = job.split("/")[-1]
            log_path = f"/opt/spark/apps/logs/{job.replace('.py', '')}.log"
            # Bracket-escape the first char so pgrep cannot match the *invoking*
            # shell itself (its cmdline contains the pattern text), which made
            # this check always pass even when the job had crashed.
            pgrep_pattern = f"[{script_name[0]}]{script_name[1:]}"
            result = container.exec_run(
                cmd=["bash", "-c", f"pgrep -f '{pgrep_pattern}'"],
            )
            if result.exit_code != 0:
                raise RuntimeError(
                    f"Streaming job {job} is NOT running (exit_code={result.exit_code}). "
                    "The next DAG run will relaunch missing jobs."
                )
            exit_code, output = container.exec_run(
                cmd=["bash", "-c", f"tail -1 {log_path} 2>/dev/null || true"],
            )
            output_text = output.decode(errors="replace") if output else ""
            if "fatal" in output_text.lower():
                raise RuntimeError(
                    f"Streaming job {job} log shows error:\n{output_text}"
                )
    finally:
        client.close()


with DAG(
    dag_id="sensor_pipeline",
    description="Sensor data pipeline: bronze → silver → gold streaming + ClickHouse sync",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Aden"),
    catchup=False,
    max_active_runs=1,
    tags=["sensor", "streaming", "batch"],
) as dag:

    start_streaming = PythonOperator(
        task_id="start_streaming_jobs",
        python_callable=_start_all_streaming,
    )

    check_health = PythonOperator(
        task_id="check_streaming_health",
        python_callable=_check_streaming_health,
    )

    start_streaming >> check_health
