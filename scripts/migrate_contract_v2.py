"""Offline ClickHouse migration; retains original tables, never deletes volumes.

Run after stopping Spark writers. Credentials are environment variables.
Supports retries: migrated engines are skipped; failed scratch copies rebuilt.
"""
import argparse
import json
import os
import re
from pathlib import Path

import requests


class ClickHouse:
    def __init__(self, url):
        self.url = url

    def run(self, sql):
        response = requests.post(self.url, data=sql,
                                 auth=(os.getenv("CLICKHOUSE_USER", "agri_analytics"),
                                       os.getenv("CLICKHOUSE_PASSWORD", "")), timeout=(5, 300), allow_redirects=False,
                                 proxies={"http": "", "https": "", "all": ""})
        if 300 <= response.status_code < 400:
            raise RuntimeError("ClickHouse migration target redirected; use its direct endpoint")
        response.raise_for_status()
        return response.text

    def rows(self, sql):
        return [json.loads(x) for x in self.run(sql + " FORMAT JSONEachRow").splitlines() if x]


ENGINES = {
    "zone_summary_history": ("last_update", "toYYYYMM(last_update)", "farm_id, zone_id, last_update"),
    "farm_summary_history": ("last_update", "toYYYYMM(last_update)", "farm_id, last_update"),
    "irrigation_alerts": ("event_time", "toYYYYMM(event_time)", "event_time, reading_id"),
    "ml_predictions": ("prediction_time", "toYYYYMM(prediction_date)", "zone_id, prediction_date, prediction_id"),
}


def target_ddl(table, scratch):
    root = Path(__file__).resolve().parents[1]
    sql = (root / "sql/clickhouse/init/01-init-schema.sql").read_text(encoding="utf-8")
    pattern = rf"CREATE TABLE IF NOT EXISTS agri_gold\.{re.escape(table)}\s*\(.*?;"
    match = re.search(pattern, sql, flags=re.DOTALL)
    if match is None:
        raise RuntimeError(f"Missing target contract for {table}")
    return match.group().replace(f"CREATE TABLE IF NOT EXISTS agri_gold.{table}",
                                 f"CREATE TABLE {scratch}", 1).rstrip(";")


def migrate(client):
    tables = {r["name"]: r["engine"] for r in client.rows(
        "SELECT name, engine FROM system.tables WHERE database='agri_gold'")}
    for table in ENGINES:
        if tables.get(table) not in (None, "MergeTree", "ReplacingMergeTree"):
            raise RuntimeError(f"Unexpected engine for {table}: {tables[table]}")
    # A completed exchange followed by interrupted backup rename is recoverable.
    for table in ENGINES:
        scratch, backup = f"{table}_contract_v2", f"{table}_legacy_v1"
        if tables.get(table) == "ReplacingMergeTree" and scratch in tables and backup not in tables:
            client.run(f"RENAME TABLE agri_gold.{scratch} TO agri_gold.{backup}")
        elif tables.get(table) == "MergeTree" and backup in tables:
            raise RuntimeError(f"Existing backup {backup}; review before migrating {table}")
    # Key timestamp changes cannot be ALTERed in place. History is rebuilt
    # directly from the target DDL, leaving the original backup unchanged.
    for table in ("zone_summary_latest",):
        if table in tables:
            for column in ("moisture_min", "moisture_max", "recommended_water_mm"):
                client.run(f"ALTER TABLE agri_gold.{table} MODIFY COLUMN {column} Nullable(Float64)")
            client.run(f"ALTER TABLE agri_gold.{table} MODIFY COLUMN last_update DateTime64(6, 'UTC')")
            client.run(f"ALTER TABLE agri_gold.{table} ADD COLUMN IF NOT EXISTS snapshot_id Int64 DEFAULT 0")
    for table in ("farm_summary_latest",):
        if table in tables:
            client.run(f"ALTER TABLE agri_gold.{table} MODIFY COLUMN last_update DateTime64(6, 'UTC')")
            client.run(f"ALTER TABLE agri_gold.{table} ADD COLUMN IF NOT EXISTS snapshot_id Int64 DEFAULT 0")
    if "weather_irrigation_alerts" in tables:
        client.run("ALTER TABLE agri_gold.weather_irrigation_alerts MODIFY COLUMN expected_rain Nullable(Float64)")
        client.run("ALTER TABLE agri_gold.weather_irrigation_alerts MODIFY COLUMN recommended_water_mm Nullable(Float64)")
        client.run("ALTER TABLE agri_gold.weather_irrigation_alerts ADD COLUMN IF NOT EXISTS observed_rain_mm Nullable(Float64)")
        client.run("ALTER TABLE agri_gold.weather_irrigation_alerts ADD COLUMN IF NOT EXISTS weather_available UInt8 DEFAULT 0")
    for table, (version, partition, order) in ENGINES.items():
        if table not in tables or tables[table] == "ReplacingMergeTree":
            continue
        if tables[table] != "MergeTree":
            raise RuntimeError(f"Unexpected engine for {table}: {tables[table]}")
        scratch = f"agri_gold.{table}_contract_v2"
        client.run(f"DROP TABLE IF EXISTS {scratch}")
        client.run(target_ddl(table, scratch))
        # Existing contract columns are copied by name; new revision column
        # defaults to zero, so v2 source batches (starting at one) win ties.
        ddl = target_ddl(table, scratch)
        body = re.search(r"\((.*)\)\s*ENGINE", ddl, re.DOTALL).group(1)
        columns = [line.strip().split()[0] for line in body.splitlines() if line.strip()]
        columns = [name for name in columns if name != "snapshot_id"]
        names = ", ".join(columns)
        client.run(f"INSERT INTO {scratch} ({names}) SELECT {names} FROM agri_gold.{table}")
        client.run(f"EXCHANGE TABLES agri_gold.{table} AND {scratch}")
        client.run(f"RENAME TABLE {scratch} TO agri_gold.{table}_legacy_v1")
        print(f"Migrated {table}; retained {table}_legacy_v1")
    root = Path(__file__).resolve().parents[1]
    for filename in ("01-init-schema.sql", "02-star-schema.sql"):
        # These project-controlled DDL files contain no semicolons in literals.
        sql = (root / "sql/clickhouse/init" / filename).read_text(encoding="utf-8")
        sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
        for statement in sql.split(";"):
            if statement.strip():
                client.run(statement)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8123")
    args = parser.parse_args()
    migrate(ClickHouse(args.url))
