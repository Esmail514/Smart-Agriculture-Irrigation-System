#!/usr/bin/env bash
# Finite batch jobs run in the master container; streaming owns worker slots.
set -euo pipefail
JOB_PATH="${1:-}"
APPS_DIR="${SPARK_APPS_DIR:-/opt/spark/apps}"
case "$JOB_PATH" in
    weather/bronze/fetch_weather_bronze.py|weather/silver/transform_weather_silver.py|weather/gold/aggregate_weather_gold.py|weather/gold/sync_weather_clickhouse.py|weather/gold/sync_weather_alerts_clickhouse.py|ml/predict_spark.py) ;;
    *) echo "Unsupported batch job: $JOB_PATH" >&2; exit 2 ;;
esac
if [[ ! -f "$APPS_DIR/$JOB_PATH" || ! -d "$APPS_DIR/common" ]]; then
    echo "Missing batch job or common package" >&2
    exit 2
fi
# Dependencies are installed and verified when the image is built.
COMMON_ZIP=$(mktemp /tmp/agri-batch-common.XXXXXXXX.zip)
trap 'rm -f "$COMMON_ZIP"' EXIT
python3 - "$APPS_DIR/common" "$COMMON_ZIP" <<'PY'
import os
import sys
import zipfile
with zipfile.ZipFile(sys.argv[2], 'w') as archive:
    for root, dirs, files in os.walk(sys.argv[1]):
        dirs[:] = [name for name in dirs if name != '__pycache__']
        for name in files:
            if name.endswith('.py'):
                full = os.path.join(root, name)
                archive.write(full, 'common/' + os.path.relpath(full, sys.argv[1]))
PY
LOG_FILE="$APPS_DIR/logs/${JOB_PATH%.py}.log"
mkdir -p "$(dirname "$LOG_FILE")"
# local[1] avoids waiting forever behind four never-ending streaming apps.
# pipefail propagates both Spark failures and log write failures to Airflow.
PYTHONPATH="$APPS_DIR:${PYTHONPATH:-}" spark-submit \
    --master 'local[1]' \
    --deploy-mode client \
    --conf spark.cores.max=1 \
    --py-files "$COMMON_ZIP" \
    "$APPS_DIR/$JOB_PATH" 2>&1 | tee "$LOG_FILE"
