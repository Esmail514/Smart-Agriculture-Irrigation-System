#!/usr/bin/env bash
# Timed restarts interrupt healthy jobs and are no longer part of deployment.
echo "Periodic stream termination is disabled. Use Airflow to launch missing jobs; restart deliberately during maintenance." >&2
exit 2
