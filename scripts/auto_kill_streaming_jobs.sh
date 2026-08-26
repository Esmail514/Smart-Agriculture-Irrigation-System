#!/bin/bash
# ==============================================================================
# auto_kill_streaming_jobs.sh
# Runs in the background on spark-master to kill bronze, silver, and gold
# streaming processes every 5 minutes (300 seconds).
# ==============================================================================

INTERVAL_SECONDS=${KILL_INTERVAL_SECONDS:-300}
JOBS=("bronze_streaming.py" "silver_streaming.py" "gold_streaming.py")

echo "[auto_killer] Started auto-kill daemon for streaming jobs (interval: ${INTERVAL_SECONDS}s)"

while true; do
    sleep "${INTERVAL_SECONDS}"
    echo "[auto_killer] $(date -u '+%Y-%m-%d %H:%M:%S UTC') - Checking and terminating streaming jobs..."
    
    for job in "${JOBS[@]}"; do
        PIDS=$(pgrep -f "${job}" || true)
        if [ -n "${PIDS}" ]; then
            echo "[auto_killer] Terminating ${job} (PIDs: ${PIDS})"
            kill -15 ${PIDS} 2>/dev/null || true
            sleep 2
            # Force kill if still running
            kill -9 ${PIDS} 2>/dev/null || true
        else
            echo "[auto_killer] ${job} is not running"
        fi
    done
done
