# 🌱 Smart Agriculture Irrigation System

> Current repair and deployment guide: [Data model v2](docs/data-model-v2.md),
> [Upgrade steps](docs/upgrade-v2.md), [Monitoring](docs/observability-deployment.md).
> Accepted-only Bronze, explicit schemas, a ClickHouse star model and scoped application API
> replace earlier demo-only serving assumptions. Diagrams/reports below describe earlier stages;
> use these guides and current code for the revised contracts.
>
> Application backend v3: [Startup and administrator setup](docs/backend-application.md),
> [management/control endpoints](docs/API_CONTROL.md), [OpenAPI contract](docs/api-openapi.json).
> Email/password sessions, per-farm permissions, dashboards, command queues and an internal service-control agent
> are implemented for the Flutter website/mobile client.
### Enterprise-Grade Data Engineering Lakehouse — Docker Compose Stack

[![Apache Kafka](https://img.shields.io/badge/Kafka-3.7.1-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5.0-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.9.1-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-Latest-C72E49?logo=minio)](https://min.io/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-Latest-FFCC01?logo=clickhouse)](https://clickhouse.com/)

---

## 📐 Architecture

```
IoT Sensor Simulator ─────────────────────────┐
Weather API ──────────────────────────────────►│
                                               │
                                     ┌─────────▼─────────┐
                                     │   Apache Kafka     │
                                     │   (KRaft Mode)     │
                                     └─────────┬──────────┘
                                               │ Spark Streaming
                                     ┌─────────▼──────────┐
                                     │   MinIO: BRONZE     │  Raw Data
                                     │  (Parquet + JSON)   │
                                     └─────────┬──────────┘
                                               │ Spark Batch ETL
                                     ┌─────────▼──────────┐
                                     │   MinIO: SILVER     │  Cleaned + Validated
                                     │     (Parquet)       │
                                     └─────────┬──────────┘
                                               │ Spark Business Logic
                                     ┌─────────▼──────────┐
                                     │   MinIO: GOLD       │  Aggregated
                                     │     (Parquet)       │
                                     └──────┬─────┬───────┘
                                            │     │
                              ┌─────────────▼┐   ┌▼──────────────────┐
                              │  ClickHouse  │   │  Iceberg Catalog   │
                              │  (Analytics) │   │  (Future-Ready)    │
                              └──────┬───────┘   └────────────────────┘
                                     │
                              ┌──────▼───────┐
                              │   Power BI   │
                              │  Dashboard   │
                              └──────────────┘

Orchestration: Apache Airflow (LocalExecutor)
Monitoring:    Prometheus + Grafana
Reference DB:  PostgreSQL (Crops, Fields, Metadata)
```

---

## 🧱 Services Overview

| # | Service | Image | Purpose | Port(s) |
|---|---------|-------|---------|---------|
| 1 | **PostgreSQL** | `postgres:14-alpine` | Crop/field reference data + Airflow metadata | 5432 |
| 2 | **Kafka** | `apache/kafka:3.7.1` | Real-time IoT sensor streaming (KRaft) | 9092, 9094 |
| 3 | **Kafka UI** | `provectuslabs/kafka-ui` | Topic/consumer group monitoring | **8080** |
| 4 | **MinIO** | `minio/minio:latest` | S3-compatible Data Lake (Bronze/Silver/Gold) | **9000**, **9001** |
| 5 | **Spark Master** | `jupyter/pyspark-notebook:spark-3.5.0` | Spark cluster coordinator + streaming drivers | **8090**, 7077 |
| 6 | **Spark Worker** | `jupyter/pyspark-notebook:spark-3.5.0` | Task executor | **8091** |
| 7 | **Spark History** | `jupyter/pyspark-notebook:spark-3.5.0` | Completed job log viewer | **18080** |
| 8 | **ClickHouse** | `clickhouse/clickhouse-server` | Columnar OLAP for Gold layer | **8123**, 9002 |
| 9 | **Airflow Init** | `apache/airflow:2.9.1` | DB migration + admin user creation | — |
| 10 | **Airflow Webserver** | `apache/airflow:2.9.1` | DAG management UI | **8085** |
| 11 | **Airflow Scheduler** | `apache/airflow:2.9.1` | DAG parsing, triggering & task execution (LocalExecutor) | — |
| 12 | **Prometheus** | `prom/prometheus:latest` | Metrics collection | **9090** |
| 13 | **Grafana** | `grafana/grafana:latest` | Dashboards & alerting | **3000** |

> NOTE: Redis, the Celery worker and the triggerer were removed when Airflow
> was migrated from CeleryExecutor to LocalExecutor (single-host RAM budget).

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop ≥ 24.0
- Docker Compose v2 (included in Docker Desktop)
- Minimum 16GB RAM (recommended for full stack)
- 20GB free disk space

### 1. Clone & Configure

```bash
# The .env file is already pre-configured with development defaults.
# For production, change all passwords in .env before starting!
```

### 2. Setup Directory Structure

```powershell
# Windows PowerShell
.\scripts\setup-dirs.ps1
```

### 3. Launch the Stack

```bash
# Start all services (detached)
docker compose up -d

# Watch startup logs
docker compose logs -f

# Start only specific services
docker compose up -d postgres kafka minio
```

### 4. Verify Health

```bash
# Check all services are healthy
docker compose ps

# View logs for a specific service
docker compose logs kafka -f
docker compose logs spark-master -f
```

---

## 🌐 Access Points

| Interface | URL | Credentials |
|-----------|-----|-------------|
| **Kafka UI** | http://localhost:8080 | No auth |
| **MinIO Console** | http://localhost:9001 | `minio_admin` / *(see .env)* |
| **Spark Master UI** | http://localhost:8090 | No auth |
| **Spark Worker UI** | http://localhost:8091 | No auth |
| **Spark History Server** | http://localhost:18080 | No auth |
| **JupyterLab** | http://localhost:8888 | Token: *(see .env)* |
| **Airflow UI** | http://localhost:8085 | `admin` / *(see .env)* |
| **ClickHouse HTTP** | http://localhost:8123 | `agri_analytics` / *(see .env)* |
| **Prometheus** | http://localhost:9090 | No auth |
| **Grafana** | http://localhost:3000 | `admin` / *(see .env)* |

---

## ⚡ How The System Works

This project implements a production-grade, real-time & batch **Medallion Data Lakehouse Architecture** for IoT-based precision agriculture and smart irrigation management.

```
+------------------+      +-------------------+      +-------------------------+
|  IoT Sensors     | ---> |   Apache Kafka    | ---> |  Spark Structured       |
|  (Soil, Weather) |      | (agri.sensors.raw)|      |  Streaming (Bronze Job) |
+------------------+      +-------------------+      +-------------------------+
                                                                |
                                                                v
+------------------+      +-------------------+      +-------------------------+
| ClickHouse OLAP  | <--- |   Spark Batch ETL | <--- |   MinIO Object Storage  |
|  & Power BI UI   |      |  (Silver & Gold)  |      | (Bronze -> Silver -> Gold)|
+------------------+      +-------------------+      +-------------------------+
         ^                                                        |
         |                                                        v
+------------------+                                 +-------------------------+
| Airflow DAG      | -------------------------------> |  ML Pipeline            |
| Orchestration    |                                 | (Irrigation Prediction) |
+------------------+                                 +-------------------------+
```

### End-to-End Data Pipeline Flow:

1. **Data Ingestion & Simulation**:
   - The IoT simulator (`src/ingestion/sensor_simulator/`) streams continuous telemetry (soil moisture, temperature, humidity, pH, NPK levels) into **Apache Kafka** topics (`agri.sensors.raw`).
   - Weather data is fetched periodically from external weather APIs via Airflow scheduled DAGs (`orchestration/airflow/dags/weather_api_pipeline_dag.py`).

2. **Medallion Lakehouse Processing (Spark + MinIO)**:
   - **Bronze Layer (Raw Storage)**: Spark Structured Streaming (`src/processing/bronze/`) reads raw messages from Kafka and writes raw JSON/Parquet directly to `s3a://bronze/sensors/raw/` with full event metadata.
   - **Silver Layer (Cleaning & Enrichment)**: PySpark batch jobs (`src/processing/silver/`) read from Bronze, perform schema validation, unit conversions, remove duplicates, handle missing values, and join with PostgreSQL reference metadata (crops & soil characteristics), writing cleaned Parquet data to `s3a://silver/sensors/enriched/`.
   - **Gold Layer (Aggregations & Analytics)**: Spark jobs (`src/processing/gold/`) aggregate metrics across fields, crops, and temporal windows (hourly/daily averages, min/max thresholds, water consumption metrics), writing refined business datasets to `s3a://gold/sensors/aggregated/`.

3. **Analytics & Visualization Storage**:
   - The Gold layer aggregated data is loaded into **ClickHouse** (`sql/clickhouse/`), providing ultra-fast OLAP queries for interactive **Power BI** dashboards and Grafana monitoring.

4. **Machine Learning & Predictive Analytics**:
   - Automated ML pipelines (`src/ml/`) extract features from Silver and Gold data to train models (`train.py`) that predict crop water stress and optimize irrigation scheduling. Model inferences (`predict.py`) produce actionable watering decisions.

5. **Orchestration & Monitoring**:
   - **Apache Airflow** schedules and coordinates the end-to-end processing pipeline, retries failed steps, and triggers ML training DAGs (`orchestration/airflow/dags/`).
   - **Prometheus** scrapes operational metrics from Kafka, Spark, MinIO, ClickHouse, and Airflow, exposed visually on pre-configured **Grafana** dashboards.

---

## 📁 Project Structure

```
Smart Agriculture Irrigation System/
├── docker-compose.yml          ← Enterprise Lakehouse Docker Compose Stack
├── .env                        ← Environment variables & secrets
├── .gitignore
├── requirements.txt            ← Python dependencies
├── requirements-dev.txt        ← Development & testing dependencies
│
├── src/                        ← Core Application Code
│   ├── ingestion/              ← IoT Sensor Data Generators & Simulators
│   │   └── sensor_simulator/   ← Kafka Producer for real-time sensor streams
│   ├── processing/             ← Spark Data Processing Modules
│   │   ├── bronze/             ← Raw streaming ingestion (Kafka -> MinIO Bronze)
│   │   ├── silver/             ← Cleaning, validation & deduplication (Bronze -> Silver)
│   │   ├── gold/               ← Business aggregations & feature engineering (Silver -> Gold)
│   │   ├── weather/            ← Weather API ingestion & processing
│   │   └── common/             ← Shared PySpark utilities & S3/ClickHouse connectors
│   ├── ml/                     ← Machine Learning Modules
│   │   ├── build_training_dataset.py
│   │   ├── train.py            ← ML model training pipeline
│   │   ├── predict.py          ← ML inference service
│   │   ├── datasets/           ← Synthetic / processed training datasets
│   │   ├── models/             ← Saved ML models
│   │   └── pipelines/          ← Scikit-Learn / ML pipelines
│   └── common/                 ← Shared configuration, logging & schema tools
│
├── orchestration/              ← Workflow Orchestration
│   └── airflow/
│       ├── dags/               ← Airflow DAGs (sensor, weather & ML pipelines)
│       │   ├── sensor_pipeline_dag.py
│       │   ├── weather_api_pipeline_dag.py
│       │   └── ml_pipeline_dag.py
│       ├── config/             ← Airflow configuration
│       └── plugins/            ← Custom Airflow operators & hooks
│
├── sql/                        ← Database Initialization Scripts
│   ├── postgres/               ← Reference tables (Crops, Fields, Farms metadata)
│   └── clickhouse/             ← OLAP Schema & Gold layer analytics tables
│
├── infra/                      ← Container & Infrastructure Configurations
│   ├── spark/                  ← Spark Master/Worker config & log4j settings
│   ├── clickhouse/             ← ClickHouse server config & users XML
│   └── prometheus/             ← Prometheus scraping target configurations
│
├── scripts/                    ← Execution & Utility Scripts
│   ├── run-pipeline.ps1        ← PowerShell master runner script
│   ├── stop-pipeline.ps1       ← PowerShell shutdown script
│   ├── setup-dirs.ps1          ← Directory initialization script
│   ├── launch_stream_job.sh    ← Spark Streaming launcher
│   └── run_spark_batch_job.sh  ← Spark Batch ETL launcher
│
├── docs/                       ← Architecture & System Documentation
├── notebooks/                  ← PySpark & EDA Jupyter Notebooks
├── screenshots/                ← System dashboards & UI screenshots
└── tests/                      ← Unit & integration test suites
```

---

## 🗄️ Data Lake Layout (MinIO)

```
MinIO Object Storage
├── bronze/           ← Raw ingestion (Kafka → Parquet)
│   ├── sensors/raw/
│   └── weather/raw/
├── silver/           ← Cleaned + validated
│   └── sensors/enriched/
├── gold/             ← Business aggregations
│   └── sensors/aggregated/
├── checkpoints/      ← Spark Streaming checkpoints
├── logs/             ← Spark event logs (History Server)
└── warehouse/        ← Iceberg table data (future)
```

---

## 📊 Kafka Topics

| Topic | Partitions | Description |
|-------|-----------|-------------|
| `agri.sensors.raw` | 4 | Real-time soil moisture, temp, humidity readings |
| `agri.weather.raw` | 2 | Weather API responses |
| `agri.alerts` | 2 | Irrigation alerts and threshold breaches |
| `agri.irrigation.commands` | 2 | Valve open/close commands |

---

## 📐 Network Architecture

```
agri-net (172.20.0.0/24)        ← Primary data plane
  └── All services communicate here

monitoring-net                   ← Isolated metrics collection
  └── Prometheus ↔ Grafana ↔ ClickHouse ↔ MinIO
```

---

## 🔍 Monitoring Stack

### Prometheus Scrape Targets
- `kafka:9999` — JMX metrics
- `clickhouse:8001` — ClickHouse built-in metrics
- `minio:9000/minio/v2/metrics/cluster` — MinIO metrics
- `airflow-webserver:8080/metrics` — Airflow StatsD
- `spark-master:8090/metrics/json` — Spark metrics

### Grafana Datasources (auto-provisioned)
- **Prometheus** (default) — system metrics
- **ClickHouse** — Gold layer analytics
- **PostgreSQL** — crop/field reference data

---

## 🚀 Running The Pipelines

### 1. Run via PowerShell Master Script (Automated)

```powershell
# Start infrastructure and run pipeline steps automatically
.\scripts\run-pipeline.ps1

# Stop all services and pipeline components
.\scripts\stop-pipeline.ps1
```

### 2. Manual Pipeline Execution

#### Step A: Launch IoT Sensor Simulator (Kafka Producer)
```bash
# Run simulator script
python -m src.ingestion.sensor_simulator.producer
```

#### Step B: Execute Spark Jobs inside `spark-master` Container
```bash
# Enter Spark Master container
docker exec -it agri-spark-master bash

# Run Bronze Streaming Ingestion (Kafka -> MinIO Bronze)
/opt/spark/scripts/launch_stream_job.sh /opt/spark/apps/src/processing/bronze/streaming_ingestion.py

# Run Silver Batch ETL (Bronze -> MinIO Silver)
/opt/spark/scripts/run_spark_batch_job.sh /opt/spark/apps/src/processing/silver/clean_sensors.py

# Run Gold Batch Aggregations (Silver -> MinIO Gold -> ClickHouse)
/opt/spark/scripts/run_spark_batch_job.sh /opt/spark/apps/src/processing/gold/aggregate_metrics.py
```

#### Step C: Train & Predict with ML Pipelines
```bash
# Build training dataset from Silver/Gold layers
python -m src.ml.build_training_dataset

# Train model
python -m src.ml.train

# Run inference
python -m src.ml.predict
```

---

## 💡 Common Operations

```bash
# Restart a specific service
docker compose restart airflow-scheduler

# Scale Spark workers
docker compose up -d --scale spark-worker=2

# Execute a command inside a container
docker compose exec postgres psql -U agri_user -d agri_meta

# Force recreate a service
docker compose up -d --force-recreate clickhouse

# Stop and remove everything (keeps volumes)
docker compose down

# Destroy everything including volumes (WARNING: data loss!)
docker compose down -v
```

---

## 🛑 Startup Order (Dependencies)

```
postgres ──────────────────────────────────────────┐
minio → minio-init ───────────────────────────────►├── airflow-init
kafka → kafka-init                                  │       │
spark-master → spark-worker                         │   airflow-webserver
              → spark-history                       │   airflow-scheduler
                                                    │   (LocalExecutor)
prometheus → grafana
clickhouse
```

---

## ⚙️ Performance Tuning

| Service | Memory Limit | Notes |
|---------|-------------|-------|
| Spark Master | 2GB | Hosts streaming drivers (driver heap 768m) |
| Spark Worker | 2GB | Executor heap 768m, 2 cores |
| ClickHouse | 2GB | Self-limited to 90% of cgroup internally |
| Kafka | 1GB | JVM heap capped at 768m below the limit |
| MinIO | 768m | Object storage overhead |
| Airflow Webserver | 768m | LocalExecutor: scheduler runs tasks |
| Airflow Scheduler | 1GB | Includes task subprocesses |

> 💡 **Tip:** On systems with less than 16GB RAM, you can reduce Spark worker memory in `.env`:
> ```
> SPARK_WORKER_MEMORY=1g
> ```

---

## 🔐 Security Notes

- All passwords are stored in `.env` — **never commit it to git**
- Default `.gitignore` already excludes `.env`
- ClickHouse has a dedicated analytics user (`agri_analytics`) with database-level access control
- Kafka listeners are separated: internal (Docker network) vs external (localhost only)
- Airflow runs LocalExecutor - no Redis/Celery infrastructure required

---

## 📘 Iceberg-Ready Configuration

Even before adding Iceberg JARs, all Spark services are pre-configured with:
- `spark.sql.extensions=IcebergSparkSessionExtensions`
- `spark.sql.catalog.lakehouse` pointing to `s3a://warehouse/`
- S3FileIO configured for MinIO endpoint

To activate Iceberg, simply add to `SPARK_OPTS` in docker-compose:
```
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2
```

---

*Built for the Smart Agriculture Graduation Project — Data Engineering Bootcamp 2024*
