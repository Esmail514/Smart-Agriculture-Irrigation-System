# Setup Guide — Smart Agriculture Irrigation System

## Prerequisites

- Docker Desktop ≥ 24.0
- Docker Compose v2 (bundled with Docker Desktop)
- 16 GB RAM minimum (full stack)
- 20 GB free disk space
- Python 3.11+ (for running the IoT simulator locally)

---

## 1. Clone and Configure

```bash
git clone <repo-url>
cd smart-agriculture-irrigation-system
```

Copy the example environment file and fill in real values:

```bash
cp .env.example .env
# Edit .env — change all passwords before starting in production!
```

> **Never commit a real `.env` file.** The `.gitignore` already excludes it.

---

## 2. Create the Directory Structure

Run the setup script to generate all required directories and `.gitkeep` placeholders:

```powershell
# Windows PowerShell
.\scripts\setup-dirs.ps1
```

---

## 3. Launch the Stack

```bash
# Start all services in detached mode
docker compose up -d

# Watch startup logs
docker compose logs -f

# Start only core infrastructure first (optional, faster)
docker compose up -d postgres kafka minio
```

---

## 4. Verify All Services are Healthy

```bash
docker compose ps
```

All services should show `healthy` or `running`.

---

## 5. Access the UI Dashboards

| Interface             | URL                        | Credentials          |
|-----------------------|----------------------------|----------------------|
| Kafka UI              | http://localhost:8080      | No auth              |
| MinIO Console         | http://localhost:9001      | See `.env`           |
| Spark Master UI       | http://localhost:8090      | No auth              |
| Spark History Server  | http://localhost:18080     | No auth              |
| Airflow UI            | http://localhost:8085      | See `.env`           |
| ClickHouse HTTP       | http://localhost:8123      | See `.env`           |
| Prometheus            | http://localhost:9090      | No auth              |
| Grafana               | http://localhost:3000      | See `.env`           |

---

## 6. Run the IoT Sensor Simulator

The sensor simulator runs locally and publishes to Kafka.

```powershell
# Activate virtualenv (Windows)
.\venv\Scripts\activate

# Run the simulator
python src/ingestion/sensor_simulator/simulator.py
```

---

## 7. Launch Spark Streaming Jobs (inside container)

```bash
docker exec -it agri-spark-master bash

# Bronze — Kafka → MinIO (streaming)
bash /opt/spark/scripts/launch_stream_job.sh bronze/bronze_streaming.py

# Silver — Bronze MinIO → Silver MinIO (streaming)
bash /opt/spark/scripts/launch_stream_job.sh silver/silver_streaming.py

# Gold — Silver MinIO → Gold MinIO (streaming)
bash /opt/spark/scripts/launch_stream_job.sh gold/gold_streaming.py
```

---

## 8. Run ML Pipelines

```bash
# Build training dataset
python src/ml/build_training_dataset.py

# Train the irrigation prediction model
python src/ml/train.py

# Run predictions
python src/ml/predict.py
```

---

## 9. Troubleshooting

**Postgres / ClickHouse not initializing schemas on first boot:**
```bash
# Force fresh initialization
docker compose down -v   # removes volumes!
docker compose up -d
```

**Spark job can't import `common.*`:**
- Ensure `src/processing` is mounted at `/opt/spark/apps` and `src/common` is accessible at `/opt/spark/apps/common`.
- Check that `launch_stream_job.sh` is zipping `src/common` correctly.

**Airflow DAGs not appearing:**
```bash
docker compose logs airflow-scheduler
# Also check: orchestration/airflow/dags/ is mounted correctly in docker-compose.yml
```
