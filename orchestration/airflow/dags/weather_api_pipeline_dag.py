from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.launch_spark_stream import launch_stream_job, run_spark_batch_job  # noqa: E402

default_args = {
    "owner": "smart_agriculture",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_api_pipeline",
    description="جلب طقس OpenWeatherMap (bronze/بايثون) -> تحقق (silver/PySpark) -> تجميع (gold/PySpark) -> ClickHouse",
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Aden"),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "batch", "openweather"],
) as dag:

    fetch_bronze = PythonOperator(
        task_id="fetch_weather_bronze",
        python_callable=run_spark_batch_job,
        op_kwargs={"job_path": "weather/bronze/fetch_weather_bronze.py"},
    )

    transform_silver = PythonOperator(
        task_id="transform_weather_silver",
        python_callable=run_spark_batch_job,
        op_kwargs={"job_path": "weather/silver/transform_weather_silver.py"},
    )

    aggregate_gold = PythonOperator(
        task_id="aggregate_weather_gold",
        python_callable=run_spark_batch_job,
        op_kwargs={"job_path": "weather/gold/aggregate_weather_gold.py"},
    )

    sync_clickhouse = PythonOperator(
        task_id="sync_weather_to_clickhouse",
        python_callable=run_spark_batch_job,
        op_kwargs={"job_path": "weather/gold/sync_weather_clickhouse.py"},
    )

    sync_alerts = PythonOperator(
        task_id="sync_weather_alerts_to_clickhouse",
        python_callable=run_spark_batch_job,
        op_kwargs={"job_path": "weather/gold/sync_weather_alerts_clickhouse.py"},
    )

    fetch_bronze >> transform_silver >> aggregate_gold >> sync_clickhouse >> sync_alerts