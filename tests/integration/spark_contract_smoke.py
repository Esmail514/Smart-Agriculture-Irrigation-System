"""Run in Spark 3.5; no external services or persistent data required.

From Pipeline in PowerShell (the source and tests mounts are read-only)::

    docker run --rm --network none --entrypoint /bin/bash `
      --mount "type=bind,source=$PWD/src,target=/review/src,readonly" `
      --mount "type=bind,source=$PWD/tests,target=/review/tests,readonly" `
      -e PYTHONPATH=/review/src:/review/src/processing `
      jupyter/pyspark-notebook:spark-3.5.0 -c `
      'spark-submit --driver-memory 768m /review/tests/integration/spark_contract_smoke.py'
"""
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import (
    DecimalType, LongType, StringType, StructField, StructType, TimestampType,
)

from common.metadata_snapshot import materialize_metadata_snapshot
from common.schemas import sensor_schema
from common.sensor_contract import sensor_ingress_error
from processing.bronze.bronze_streaming import route_sensor_rows
from weather.contracts import bronze_schema, flatten_record
from weather.silver.transform_weather_silver import transform as weather_silver


def check_metadata(spark):
    identity = 2**60 + 1
    schema = StructType([
        StructField("sensor_id", LongType(), False),
        StructField("name", StringType(), False),
        StructField("threshold", DecimalType(8, 2), True),
        StructField("observed_at", TimestampType(), True),
    ])
    instant = datetime(2026, 10, 7, 12)
    frame = spark.createDataFrame([(identity, "اختبار", Decimal("30.25"), instant)], schema)
    copied = materialize_metadata_snapshot(spark, frame, name="smoke", max_rows=2)
    assert copied.schema == schema
    row = copied.collect()[0]
    assert row.sensor_id == identity and row.threshold == Decimal("30.25")
    assert row.observed_at == instant and row.name == "اختبار"
    empty = materialize_metadata_snapshot(spark, spark.createDataFrame([], schema), name="empty", max_rows=2)
    assert empty.schema == schema and empty.count() == 0
    try:
        materialize_metadata_snapshot(spark, frame.unionByName(frame), name="overflow", max_rows=1)
    except RuntimeError as error:
        assert "limit" in str(error)
    else:
        raise AssertionError("Metadata bound did not fail closed")


def check_admission(spark):
    identity = 2**60 + 1
    instant = datetime(2026, 10, 7, 12)
    base = dict(schema_version=1, sensor_id=identity, sensor_code="S1", zone_id=2,
                zone_name="Zone", farm_id=3, farm_name="Farm", crop_id=4, crop_name="Crop",
                soil_moisture=40.0, soil_temperature=25.0, air_temperature=30.0,
                air_humidity=60.0, reading_id="reading", timestamp="2026-10-07T12:00:00.000000Z")
    payloads = [json.dumps(base), json.dumps(dict(base, soil_moisture=None)),
                json.dumps(dict(base, soil_moisture=float("nan"))),
                json.dumps(dict(base, soil_moisture=10**400)),
                json.dumps(dict(base, farm_id=9)), "{malformed", None,
                json.dumps(dict(base, timestamp="2026-10-07T12:05:01.000000Z")),
                json.dumps(dict(base, farm_name=" ")), json.dumps(base)]
    wire_schema = "kafka_offset long, raw_value string, kafka_timestamp timestamp"
    wire = spark.createDataFrame([(i, raw, instant if i != 9 else None) for i, raw in enumerate(payloads)], wire_schema)
    parsed = (wire.withColumn("contract_error", f.udf(sensor_ingress_error, StringType())("raw_value", "kafka_timestamp"))
              .withColumn("sensor", f.from_json("raw_value", sensor_schema))
              .select("sensor.*", "raw_value", "kafka_timestamp", "kafka_offset", "contract_error"))
    names = ["sensor_id", "sensor_code", "zone_id", "zone_name", "farm_id", "farm_name", "crop_id", "crop_name"]
    registry_schema = StructType([StructField("registry_" + name, LongType() if name.endswith("_id") else StringType(), False) for name in names])
    registry = spark.createDataFrame([tuple(base[name] for name in names)], registry_schema)
    registry = materialize_metadata_snapshot(spark, registry, name="registry", max_rows=2)
    accepted, rejected = route_sensor_rows(parsed, registry)
    accepted_rows = accepted.select("kafka_offset", "sensor_id").collect()
    rejected_rows = rejected.select("kafka_offset", "rejection_reason").collect()
    assert [(r.kafka_offset, r.sensor_id) for r in accepted_rows] == [(0, identity)]
    offsets = [r.kafka_offset for r in accepted_rows + rejected_rows]
    assert sorted(offsets) == list(range(len(payloads))) and len(offsets) == len(set(offsets))
    reasons = {r.kafka_offset: r.rejection_reason for r in rejected_rows}
    assert reasons[4] == "unknown_sensor_or_relationship"
    assert reasons[7] == "future_timestamp" and reasons[9] == "missing_ingress_timestamp"
    # Re-executing the second plan must preserve the same routing decision.
    assert {r.kafka_offset for r in rejected.select("kafka_offset").collect()} == set(reasons)


def check_weather(spark):
    now = datetime(2026, 10, 7, 12, tzinfo=timezone.utc)
    farm = dict(farm_id=1, farm_name="Farm", latitude=15.0, longitude=44.0)
    payload = dict(cod=200, dt=int(now.timestamp()), main=dict(temp=28, humidity=60, pressure=1010),
                   wind=dict(speed=2), weather=[dict(main="Clear", description="clear")])
    record = flatten_record(farm, payload, now)
    records = [record, dict(record, fetched_at=record["fetched_at"] + timedelta(minutes=1)),
               dict(record, schema_version=1), dict(record, temperature=None),
               dict(record, observation_time_utc=record["observation_time_utc"] + timedelta(minutes=11))]
    nullable = StructType([StructField(field.name, field.dataType, True) for field in bronze_schema().fields])
    accepted, rejected = weather_silver(spark.createDataFrame(records, nullable))
    rows = accepted.collect()
    assert len(rows) == 1 and rows[0].fetched_at == record["fetched_at"] + timedelta(minutes=1)
    assert rejected.count() == 3


def main():
    spark = (SparkSession.builder.master("local[2]").appName("isolated-contract-smoke")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        check_metadata(spark)
        print("PASS typed metadata / Long64 / Decimal / UTC / empty / row bound", flush=True)
        check_admission(spark)
        print("PASS sensor acceptance / quarantine partition / replay stability", flush=True)
        check_weather(spark)
        print("PASS Weather Silver version / temporal / null rejection / latest dedup", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
