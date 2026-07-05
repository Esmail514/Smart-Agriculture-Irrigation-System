# 🌱 Smart Agriculture Irrigation System
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

Orchestration: Apache Airflow (CeleryExecutor)
Monitoring:    Prometheus + Grafana
Reference DB:  PostgreSQL (Crops, Fields, Metadata)
```

---

## 🧱 Services Overview

| # | Service | Image | Purpose | Port(s) |
|---|---------|-------|---------|---------|
| 1 | **PostgreSQL** | `postgres:14-alpine` | Crop/field reference data + Airflow metadata | 5432 |
| 2 | **Redis** | `redis:7.2-alpine` | Celery broker for Airflow | 6379 |
| 3 | **Kafka** | `apache/kafka:3.7.1` | Real-time IoT sensor streaming (KRaft) | 9092, 9094 |
| 4 | **Kafka UI** | `provectuslabs/kafka-ui` | Topic/consumer group monitoring | **8080** |
| 5 | **MinIO** | `minio/minio:latest` | S3-compatible Data Lake (Bronze/Silver/Gold) | **9000**, **9001** |
| 6 | **Spark Master** | `jupyter/pyspark-notebook:spark-3.5.0` | Spark cluster coordinator | **8090**, 7077 |
| 7 | **Spark Worker** | `jupyter/pyspark-notebook:spark-3.5.0` | Task executor | **8091** |
| 8 | **Spark History** | `jupyter/pyspark-notebook:spark-3.5.0` | Completed job log viewer | **18080** |
| 9 | **Jupyter** | `jupyter/pyspark-notebook:spark-3.5.0` | Interactive PySpark development | **8888** |
| 10 | **ClickHouse** | `clickhouse/clickhouse-server` | Columnar OLAP for Gold layer | **8123**, 9002 |
| 11 | **Airflow Init** | `apache/airflow:2.9.1` | DB migration + admin user creation | — |
| 12 | **Airflow Webserver** | `apache/airflow:2.9.1` | DAG management UI | **8085** |
| 13 | **Airflow Scheduler** | `apache/airflow:2.9.1` | DAG parsing & triggering | — |
| 14 | **Airflow Worker** | `apache/airflow:2.9.1` | Celery task executor | — |
| 15 | **Airflow Triggerer** | `apache/airflow:2.9.1` | Deferrable operators handler | — |
| 16 | **Prometheus** | `prom/prometheus:latest` | Metrics collection | **9090** |
| 17 | **Grafana** | `grafana/grafana:latest` | Dashboards & alerting | **3000** |

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
docker compose up -d postgres redis kafka minio
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

## 📁 Project Structure

```
final project/
├── docker-compose.yml          ← Main orchestration file
├── .env                        ← Environment variables (secrets!)
├── .gitignore
│
├── postgres/
│   └── init/
│       └── 01-init-databases.sql   ← Schema + seed data
│
├── kafka/
│   └── data/                       ← Kafka log data (volume)
│
├── spark/
│   ├── conf/
│   │   ├── spark-defaults.conf     ← S3A + Iceberg + Parquet config
│   │   └── log4j2.properties       ← Logging tuning
│   └── jars/                       ← Extra JARs (optional)
│
├── clickhouse/
│   ├── config/
│   │   ├── prometheus.xml          ← Metrics endpoint config
│   │   └── users.xml               ← Analytics user config
│   └── init/
│       └── 01-init-schema.sql      ← Gold layer tables
│
├── airflow/
│   ├── config/
│   │   └── airflow.cfg             ← Airflow settings override
│   ├── dags/
│   │   ├── agri_sensor_pipeline.py ← Bronze→Silver→Gold ETL DAG
│   │   └── agri_weather_ingestion.py ← Weather API ingestion DAG
│   └── plugins/                    ← Custom Airflow operators
│
├── prometheus/
│   ├── prometheus.yml              ← Scrape configurations
│   └── rules/
│       └── agri-alerts.yml         ← Alert rules
│
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── datasources.yml     ← Auto-configured datasources
│   │   └── dashboards/
│   │       └── dashboards.yml      ← Dashboard provisioning
│   └── dashboards/                 ← JSON dashboard files
│
├── notebooks/
│   └── 00_environment_test.ipynb   ← Full stack integration test
│
└── scripts/
    └── setup-dirs.ps1              ← Directory initialization
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
redis ─────────────────────────────────────────────┤
minio → minio-init ───────────────────────────────►├── airflow-init
kafka → kafka-init                                  │       │
spark-master → spark-worker                         │   airflow-webserver
              → spark-history                       │   airflow-scheduler
              → jupyter                             │   airflow-worker
                                                    │   airflow-triggerer
prometheus → grafana
clickhouse
```

---

## ⚙️ Performance Tuning

| Service | Memory Limit | Notes |
|---------|-------------|-------|
| Spark Worker | 3GB | Increase for larger datasets |
| Jupyter | 3GB | Includes driver memory |
| ClickHouse | 2GB | Increase for production |
| Kafka | 1GB | Sufficient for dev workloads |
| MinIO | 1GB | Object storage overhead |
| Airflow (each) | 1GB | Per service |

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
- Redis is password-protected with AOF persistence enabled

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
