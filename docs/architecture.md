# Architecture — Smart Agriculture Irrigation System

## Overview

This system is a production-grade, real-time data engineering pipeline designed for smart farm irrigation management. It ingests IoT sensor data and weather API data, processes it through a **medallion architecture** (Bronze → Silver → Gold), and serves analytics via ClickHouse and Grafana.

---

## Medallion Architecture

```
IoT Sensor Simulator
        │
        ▼
  Apache Kafka (sensor_data topic)
        │
        ▼  Spark Structured Streaming
   Bronze Layer (MinIO: s3a://agri-lakehouse/bronze/)
     Raw sensor readings + quarantine
        │
        ▼  Spark Structured Streaming
   Silver Layer (MinIO: s3a://agri-lakehouse/silver/)
     Cleaned, validated, enriched readings
        │
        ▼  Spark Structured Streaming (foreachBatch)
   Gold Layer (MinIO: s3a://agri-lakehouse/gold/)
     Zone summaries, farm summaries, irrigation alerts
        │
        ▼
   ClickHouse (columnar OLAP)
        │
        ▼
   Grafana Dashboards / Power BI
```

---

## Weather Pipeline — Domain-First Structure

The **weather pipeline** is organized domain-first (`weather/bronze/`, `weather/silver/`, `weather/gold/`) rather than layer-first, unlike the main sensor pipeline (`processing/bronze/`, `processing/silver/`, `processing/gold/`).

**This is intentional.** The weather pipeline is a **self-contained batch API pipeline** that calls the OpenWeatherMap API on a schedule (every 30 minutes via Airflow) — it is not a continuous Kafka stream. Because weather data has its own source, schema, validation rules, and aggregation logic that are entirely decoupled from sensor data, grouping it by domain first makes it much easier to understand, test, and extend independently.

The sensor pipeline, by contrast, is a continuous streaming pipeline with tightly coupled Bronze → Silver → Gold stages, so layer-first organization better reflects its data flow.

---

## Technology Stack

| Component        | Technology                          |
|-----------------|-------------------------------------|
| Ingestion       | Apache Kafka 3.7.1 (KRaft mode)     |
| Stream Processing | Apache Spark 3.5 Structured Streaming |
| Orchestration   | Apache Airflow 2.9.1 (CeleryExecutor) |
| Data Lake       | MinIO (S3-compatible)               |
| Reference DB    | PostgreSQL 14                       |
| Analytics       | ClickHouse (columnar OLAP)          |
| Monitoring      | Prometheus + Grafana                |
| ML              | scikit-learn, XGBoost, LightGBM, CatBoost |

---

## Service Layers

1. **Infrastructure** — PostgreSQL, Redis (Celery broker)
2. **Streaming Ingestion** — Kafka (KRaft, no ZooKeeper)
3. **Data Lake** — MinIO (Bronze / Silver / Gold zones)
4. **Processing Engine** — Spark Master + Worker + History Server
5. **Analytics** — ClickHouse
6. **Orchestration** — Airflow (Init + Webserver + Scheduler + Worker + Triggerer)
7. **Monitoring** — Prometheus + Grafana

---

## Docker Images

- **PostgreSQL** — uses the official `postgres:14-alpine` image. No custom Dockerfile needed.
- **Kafka** — uses the official `apache/kafka:3.7.1` image. No custom Dockerfile needed.
- **Spark** — uses `jupyter/pyspark-notebook:spark-3.5.0`. Extra JARs (Kafka connector, Hadoop-AWS, PostgreSQL JDBC, ClickHouse JDBC) are pre-downloaded into `infra/spark/jars/` and mounted at `/opt/spark/extra-jars/` at runtime. No custom Dockerfile is needed at this time — if additional Python packages are required in the future, create `infra/spark/Dockerfile`.
- **Airflow** — uses `apache/airflow:2.9.1`. Python dependencies (`docker`, etc.) are currently installed inline. If a `requirements.txt` grows to a non-trivial size, formalize with `infra/airflow/Dockerfile`.
- **ClickHouse** — uses `clickhouse/clickhouse-server:latest`. No custom Dockerfile needed.
- **Prometheus / Grafana** — standard official images, no customization.

---

## Folder Structure

See [README.md](../README.md) for the complete folder tree.
