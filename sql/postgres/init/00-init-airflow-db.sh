#!/usr/bin/env bash
(
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    --set=airflow_db="${POSTGRES_AIRFLOW_DB:-airflow}" <<'SQL'
SELECT format('CREATE DATABASE %I', :'airflow_db')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'airflow_db')\gexec
SQL
)
