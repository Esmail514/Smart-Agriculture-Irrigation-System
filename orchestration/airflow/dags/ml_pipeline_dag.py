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


def _run_ml_training(**context):
    run_batch_job("ml/train.py")


def _run_ml_prediction(**context):
    run_batch_job("ml/predict.py")


def _check_model_exists(**context):
    import os

    model_path = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..", "src", "ml", "artifacts", "model.joblib",
    )
    if not os.path.exists(model_path):
        raise FileNotFoundError(
            f"ML model not found at {model_path}. "
            "Run training first or ensure the model artifact exists."
        )


with DAG(
    dag_id="ml_pipeline",
    description="ML pipeline: train model → store predictions in ClickHouse",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Aden"),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "training", "prediction"],
) as dag:

    check_model = PythonOperator(
        task_id="check_model_exists",
        python_callable=_check_model_exists,
    )

    train_model = PythonOperator(
        task_id="train_model",
        python_callable=_run_ml_training,
    )

    run_prediction = PythonOperator(
        task_id="run_prediction",
        python_callable=_run_ml_prediction,
    )

    check_model >> train_model >> run_prediction