#!/bin/bash
# Runs synchronous PySpark batch jobs (spark-submit exits on its own since
# these are not streaming queries). Unlike launch_stream_job.sh which is for
# perpetually running streaming jobs.

JOB_PATH="$1"   # Relative path inside /opt/spark/apps, e.g. weather/silver/transform_weather_silver.py
if [ -z "$JOB_PATH" ]; then
    echo "Usage: run_spark_batch_job.sh <relative/path/to/file.py>"
    exit 1
fi

APPS_DIR="/opt/spark/apps"
JOB_DIR=$(dirname "${APPS_DIR}/${JOB_PATH}")
JOB_UTILS_ZIP="/tmp/$(basename ${JOB_DIR})_utils.zip"

COMMON_DIR="/opt/spark/apps/common"
COMMON_ZIP="/tmp/common.zip"

# Package common/ as --py-files so 'from common.X import ...' works inside spark-submit
if [ -d "${COMMON_DIR}" ]; then
    rm -f "${COMMON_ZIP}"
    python3 -c "
import zipfile
import os

common_dir = '${COMMON_DIR}'
zip_path = '${COMMON_ZIP}'
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
    PY_FILES_ARG="${COMMON_ZIP}"
else
    PY_FILES_ARG=""
fi

# Also zip any .py files in the same directory as the job (e.g. weather_config.py)
rm -f "${JOB_UTILS_ZIP}"
python3 -c "
import zipfile
import os

job_dir = '${JOB_DIR}'
zip_path = '${JOB_UTILS_ZIP}'

with zipfile.ZipFile(zip_path, 'w') as zf:
    for f in os.listdir(job_dir):
        if f.endswith('.py'):
            zf.write(os.path.join(job_dir, f), f)
"

if [ -n "${PY_FILES_ARG}" ]; then
    PY_FILES_COMBINED="${PY_FILES_ARG},${JOB_UTILS_ZIP}"
else
    PY_FILES_COMBINED="${JOB_UTILS_ZIP}"
fi

LOG_FILE="${APPS_DIR}/logs/$(basename ${JOB_PATH%.py}).log"
mkdir -p "$(dirname "${LOG_FILE}")"

# Synchronous (no nohup/&): exit code of this script = actual job exit code.
# This lets Airflow know if the job truly succeeded or failed, and lets the
# task chain (bronze >> silver >> gold) wait for each step to finish.
PYTHONPATH="/opt/spark/apps:${PYTHONPATH}" spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --py-files "${PY_FILES_COMBINED}" \
    "${APPS_DIR}/${JOB_PATH}" \
    2>&1 | tee "${LOG_FILE}"

# PIPESTATUS[0] is critical — without it the pipe exit code reflects tee
# (always succeeds), not the actual spark-submit result.
exit ${PIPESTATUS[0]}
