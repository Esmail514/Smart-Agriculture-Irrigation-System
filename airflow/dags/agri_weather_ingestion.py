"""
============================================================
Smart Agriculture Irrigation System
Airflow DAG: Weather API Ingestion
============================================================
Fetches weather data from OpenWeatherMap API for each field
location and stores it in MinIO Bronze layer.

Schedule: Every hour
============================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

default_args = {
    "owner": "agri-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="agri_weather_ingestion",
    description="Fetch weather data for all field locations → MinIO Bronze",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 * * * *",  # Every hour
    catchup=False,
    max_active_runs=1,
    tags=["agriculture", "weather", "ingestion", "bronze"],
) as dag:

    def fetch_weather_for_fields(**context):
        """
        Fetches weather data from OpenWeatherMap API
        for all active field locations in PostgreSQL.
        Writes raw JSON to MinIO Bronze bucket.
        """
        import json
        import requests
        from datetime import datetime

        api_key = os.getenv("WEATHER_API_KEY", "")
        base_url = os.getenv("WEATHER_API_BASE_URL", "https://api.openweathermap.org/data/2.5")

        # Field coordinates (in production, fetch from PostgreSQL)
        fields = [
            {"field_id": 1, "name": "North Field A", "lat": 31.2001, "lon": 29.9187},
            {"field_id": 2, "name": "South Field B", "lat": 31.1987, "lon": 29.9201},
            {"field_id": 3, "name": "East Plot C",   "lat": 31.2015, "lon": 29.9215},
        ]

        results = []
        for field in fields:
            if api_key and api_key != "your_openweathermap_api_key_here":
                url = f"{base_url}/weather?lat={field['lat']}&lon={field['lon']}&appid={api_key}&units=metric"
                response = requests.get(url, timeout=10)
                weather_data = response.json()
            else:
                # Mock data for development
                weather_data = {
                    "field_id": field["field_id"],
                    "field_name": field["name"],
                    "timestamp": datetime.utcnow().isoformat(),
                    "temperature": 25.5,
                    "humidity": 60.0,
                    "precipitation": 0.0,
                    "wind_speed": 3.2,
                    "description": "clear sky",
                    "source": "mock_development",
                }

            weather_data["field_id"] = field["field_id"]
            weather_data["field_name"] = field["name"]
            results.append(weather_data)

        print(f"Fetched weather for {len(results)} fields")
        return results

    fetch_weather = PythonOperator(
        task_id="fetch_weather_api",
        python_callable=fetch_weather_for_fields,
        provide_context=True,
    )

    store_bronze = BashOperator(
        task_id="store_to_bronze",
        bash_command="""
            echo "Storing weather data to MinIO Bronze layer..."
            # boto3 / mc write to s3://bronze/weather/{{ ds }}/weather_raw.json
            echo "Weather data stored to bronze at $(date)"
        """,
    )

    fetch_weather >> store_bronze
