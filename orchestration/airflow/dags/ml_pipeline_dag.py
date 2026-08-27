from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.launch_spark_stream import run_batch_job  # noqa: E402

default_args = {
    "owner": "smart_agriculture",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _run_ml_pipeline(**context):
    run_batch_job("ml/predict_spark.py")


with DAG(
    dag_id="ml_pipeline",
    description="ML pipeline: train model + store predictions in ClickHouse",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Aden"),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "training", "prediction"],
) as dag:

    run_prediction = PythonOperator(
        task_id="train_and_predict",
        python_callable=_run_ml_pipeline,
    )

    run_prediction
