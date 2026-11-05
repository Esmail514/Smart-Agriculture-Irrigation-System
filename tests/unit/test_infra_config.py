"""Configuration-level regression tests for the A1-A8 plumbing fixes.

These read the source/yaml files as text so they run without Spark or a live
stack.  Behavioural tests live in test_features.py / test_irrigation_decisions.py
/ test_ch_sync.py / test_generator_anomalies.py.
"""

import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


# --- A1: src/ml must be mounted in every Spark service ---
def test_src_ml_mounted_in_all_spark_services():
    compose = _read("docker-compose.yml")
    assert compose.count("./src/ml:/opt/spark/apps/ml:rw") == 3


# --- A2: ML pipeline must consume the weather features ---
def test_ml_uses_weather_features():
    import pandas as pd
    from src.ml.feature_engineering import merge_weather

    zones = pd.DataFrame({"farm_id": [1, 2], "last_update": ["2026-10-07", "2026-10-07"]})
    weather = pd.DataFrame({"farm_id": [1], "weather_date": ["2026-10-07"],
                            "last_update": ["2026-10-07T00:00:00Z"],
                            "total_rain": [4.5], "avg_wind_speed": [2.0]})
    merged = merge_weather(zones, weather)
    assert list(merged["weather_available"]) == [1, 0]
    assert list(merged["total_rain"]) == [4.5, 0.0]


# --- A3: weather daily sync must be a full-mirror atomic replace ---
def test_weather_daily_sync_is_full_mirror():
    src = _read("src/processing/weather/gold/sync_weather_clickhouse.py")
    assert "atomic_replace" in src
    assert '.mode("append")' not in src


# --- A4: weather_irrigation_alerts producer exists and is wired into the DAG ---
def test_weather_alerts_producer_exists():
    src = _read("src/processing/weather/gold/sync_weather_alerts_clickhouse.py")
    assert "agri_gold.weather_irrigation_alerts" in src
    assert "irrigation_decision" in src


def test_weather_dag_has_alerts_task():
    dag = _read("orchestration/airflow/dags/weather_api_pipeline_dag.py")
    assert "sync_weather_alerts_to_clickhouse" in dag
    assert "sync_clickhouse >> sync_alerts" in dag


# --- A5: streaming queries must be stopped on shutdown ---
def test_gold_to_clickhouse_stops_streaming_queries():
    src = _read("src/processing/gold/gold_to_clickhouse.py")
    assert "query.stop()" in src
    assert "query.isActive" in src


# --- A6: latest tables must be replaced atomically (no empty-window truncate) ---
def test_gold_to_clickhouse_uses_atomic_replace():
    src = _read("src/processing/gold/gold_to_clickhouse.py")
    assert "atomic_replace" in src
    assert "TRUNCATE TABLE" not in src


# Atomic snapshot publication, interrupted writes, old-reader retention and
# legacy recovery are exercised behaviorally in test_snapshots.py. Directory
# rename is not an atomic swap on MinIO/S3 and is deliberately no longer used.


# --- A7: Grafana PostgreSQL datasource must point at the real DB ---
def test_grafana_has_postgres_env():
    compose = _read("docker-compose.yml")
    assert "POSTGRES_USER: ${POSTGRES_USER}" in compose
    assert "POSTGRES_DB: ${POSTGRES_DB}" in compose


def test_postgres_datasource_points_to_real_db():
    ds = _read("infra/grafana/provisioning/datasources/datasources.yml")
    assert "${POSTGRES_USER}" in ds
    assert "database: ${POSTGRES_DB}" in ds
    assert "agri_meta" not in ds
    assert "agri_user" not in ds
