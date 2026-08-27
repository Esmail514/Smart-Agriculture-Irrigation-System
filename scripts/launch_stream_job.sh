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
# Bracket-escape first char + exclude this shell ($$) and its parent ($PPID):
# the launcher's own command line contains the script name (as an argument),
# so a naive pgrep -f always self-matches and every launch reports
# "already_running" without ever starting the job.
PGREP_PATTERN="[${SCRIPT_NAME:0:1}]${SCRIPT_NAME:1}"
# Exclude every process whose own cmdline is part of THIS launcher invocation
# (the script shell, its $() subshells, wrappers) - they all inherit the same
# argv and would otherwise always self-match. Only foreign processes
# (the actual python/java drivers) may satisfy the guard.
OTHER_MATCHES=$(pgrep -f "${PGREP_PATTERN}" 2>/dev/null | while IFS= read -r pid; do
    CMDLINE=$(tr '\0' ' ' < "/proc/${pid}/cmdline" 2>/dev/null || true)
    case "${CMDLINE}" in
        *launch_stream_job*) ;; # our own launcher tree - ignore
        *) echo "${pid}" ;;
    esac
done || true)
if [ -n "${OTHER_MATCHES}" ]; then
    echo "already_running: ${JOB_FILENAME} is already running - ignoring this request"
    exit 0
fi

APPS_DIR="/opt/spark/apps"
COMMON_DIR="/opt/spark/apps/common"   # src/common is mounted here (read-only)
# Unique per-invocation: consecutive DAG task launches run back-to-back and
# would otherwise rewrite a SHARED /tmp/common.zip while an earlier
# spark-submit is still reading it (FileNotFoundException at driver start).
COMMON_ZIP="/tmp/common.$$.$(date +%s).zip"

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
# spark.cores.max=1: each app gets exactly 1 executor (1 core, 512m heap)
# so all 4 streaming apps share the 4-core worker concurrently.
PYTHONPATH="/opt/spark/apps:${PYTHONPATH}" nohup spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.cores.max=1 \
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
