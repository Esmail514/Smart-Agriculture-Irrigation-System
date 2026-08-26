#!/bin/bash
# Runs inside the agri-spark-master container via docker exec from Airflow.
# Prevents launching a second instance of the same job if it is already running.

set -e

JOB_FILENAME="$1"
if [ -z "$JOB_FILENAME" ]; then
    echo "Usage: launch_stream_job.sh <job_filename.py>"
    exit 1
fi

SCRIPT_NAME=$(basename "${JOB_FILENAME}")
if pgrep -f "${SCRIPT_NAME}" > /dev/null 2>&1; then
    echo "already_running: ${JOB_FILENAME} is already running - ignoring this request"
    exit 0
fi

APPS_DIR="/opt/spark/apps"
COMMON_DIR="/opt/spark/apps/common"   # src/common is mounted here (read-only)
COMMON_ZIP="/tmp/common.zip"          # writable temp location for --py-files packaging

if [ -d "${COMMON_DIR}" ]; then
    rm -f "${COMMON_ZIP}"
    python3 -c "
import zipfile
import os

common_dir = '${COMMON_DIR}'
zip_path = '${COMMON_ZIP}'
# The folder name ('common') must appear as the package root inside the zip
# so that 'from common.config import ...' works after adding it via --py-files
package_name = os.path.basename(common_dir.rstrip('/'))

with zipfile.ZipFile(zip_path, 'w') as zf:
    for root, dirs, files in os.walk(common_dir):
        for f in files:
            if not f.endswith('.py'):
                continue
            full = os.path.join(root, f)
            rel = os.path.relpath(full, common_dir)
            arcname = os.path.join(package_name, rel)
            zf.write(full, arcname)
"
    PY_FILES_ARG="--py-files ${COMMON_ZIP}"
else
    echo "WARNING: ${COMMON_DIR} not found - proceeding without --py-files"
    PY_FILES_ARG=""
fi

LOG_FILE="${APPS_DIR}/logs/${JOB_FILENAME%.py}.log"
mkdir -p "$(dirname "${LOG_FILE}")"

# nohup + & → fire-and-forget for streaming jobs (run indefinitely)
PYTHONPATH="/opt/spark/apps:${PYTHONPATH}" nohup spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    ${PY_FILES_ARG} \
    "${APPS_DIR}/${JOB_FILENAME}" >> "${LOG_FILE}" 2>&1 &

DRIVER_PID=$!
sleep 3

if ! kill -0 "${DRIVER_PID}" 2>/dev/null; then
    echo "launch_failed: ${JOB_FILENAME} crashed within first 3 seconds - last 30 lines:"
    tail -n 30 "${LOG_FILE}"
    exit 1
fi

echo "launched_ok: ${JOB_FILENAME} pid=${DRIVER_PID} log=${LOG_FILE}"
exit 0
