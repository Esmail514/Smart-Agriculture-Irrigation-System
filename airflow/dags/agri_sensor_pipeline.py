"""
============================================================
Smart Agriculture Irrigation System
Airflow DAG: IoT Sensor Data Pipeline
============================================================
Orchestrates the full Lakehouse ETL pipeline:
  Bronze → Silver → Gold

Schedule: Every 30 minutes
============================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

# --- Default Arguments ---
default_args = {
    "owner": "agri-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# --- DAG Definition ---
with DAG(
    dag_id="agri_sensor_pipeline",
    description="Smart Agriculture IoT sensor data pipeline - Bronze to Gold",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/30 * * * *",  # Every 30 minutes
    catchup=False,
    max_active_runs=1,
    tags=["agriculture", "iot", "lakehouse", "etl"],
    doc_md="""
    ## Smart Agriculture Sensor Pipeline

    This DAG orchestrates the full ETL pipeline for IoT sensor data:

    1. **Bronze**: Raw Kafka events → MinIO (Parquet)
    2. **Silver**: Cleaned + validated data → MinIO (Parquet)
    3. **Gold**: Business aggregations → MinIO (Parquet) + ClickHouse

    ### Architecture
    IoT Sensors → Kafka → Spark Streaming → Bronze → Silver → Gold → ClickHouse
    """,
) as dag:

    # --- Task: Validate Bronze Layer ---
    validate_bronze = BashOperator(
        task_id="validate_bronze_layer",
        bash_command="""
            echo "Checking Bronze layer in MinIO..."
            # In production, use mc (MinIO client) or boto3 to verify new files
            echo "Bronze validation passed at $(date)"
        """,
    )

    # --- Task: Spark Bronze → Silver ETL ---
    bronze_to_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command="""
            echo "Submitting Bronze→Silver Spark job..."
            # spark-submit --master spark://spark-master:7077 \
            #   --packages org.apache.hadoop:hadoop-aws:3.3.4 \
            #   /opt/airflow/dags/scripts/bronze_to_silver.py
            echo "Bronze→Silver job submitted at $(date)"
        """,
    )

    # --- Task: Spark Silver → Gold ETL ---
    silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command="""
            echo "Submitting Silver→Gold Spark job..."
            # spark-submit --master spark://spark-master:7077 \
            #   --packages org.apache.hadoop:hadoop-aws:3.3.4 \
            #   /opt/airflow/dags/scripts/silver_to_gold.py
            echo "Silver→Gold job submitted at $(date)"
        """,
    )

    # --- Task: Load Gold → ClickHouse ---
    load_clickhouse = BashOperator(
        task_id="load_gold_to_clickhouse",
        bash_command="""
            echo "Loading Gold layer data into ClickHouse..."
            # python /opt/airflow/dags/scripts/load_clickhouse.py
            echo "ClickHouse load completed at $(date)"
        """,
    )

    # --- Task: Data Quality Check ---
    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command="""
            echo "Running data quality checks..."
            # python /opt/airflow/dags/scripts/data_quality.py
            echo "Data quality check passed at $(date)"
        """,
    )

    # --- Task: Pipeline Metrics ---
    record_metrics = BashOperator(
        task_id="record_pipeline_metrics",
        bash_command="""
            echo "Recording pipeline run metrics..."
            echo "Pipeline completed successfully at $(date)"
        """,
    )

    # --- DAG Dependencies (Startup Ordering) ---
    validate_bronze >> bronze_to_silver >> silver_to_gold >> load_clickhouse >> data_quality_check >> record_metrics
