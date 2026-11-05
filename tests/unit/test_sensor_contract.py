import json
import sys
from datetime import datetime, timedelta, timezone

import pytest
from common.sensor_contract import sensor_json_error, sensor_ingress_error


def valid():
    return {"schema_version": 1, "sensor_id": 1, "zone_id": 1, "farm_id": 1, "crop_id": 1,
            "sensor_code": "S1", "zone_name": "Zone", "farm_name": "Farm", "crop_name": "Crop",
            "reading_id": "unique", "soil_moisture": 40, "soil_temperature": 25,
            "air_temperature": 30, "air_humidity": 65, "timestamp": "2026-01-01T12:00:00.000000Z"}


def test_accepts_valid_and_legacy_version():
    data = valid()
    assert sensor_json_error(json.dumps(data)) is None
    del data["schema_version"]
    assert sensor_json_error(json.dumps(data)) is None


@pytest.mark.parametrize("field,value", [("sensor_id", True), ("farm_id", 1.0), ("zone_id", 0),
    ("crop_id", 2**63), ("schema_version", 2), ("reading_id", " "), ("soil_moisture", None),
    ("soil_moisture", float('nan')), ("air_temperature", float('inf')), ("air_humidity", 101),
    ("soil_temperature", -11), ("timestamp", "2026-02-30T12:00:00.000000Z"),
    ("timestamp", "2026-01-01T12:00:00.000000+03:00")])
def test_rejects_invalid_contract(field, value):
    data = valid()
    data[field] = value
    assert sensor_json_error(json.dumps(data)) is not None


def test_all_missing_required_fields_are_rejected():
    for field in valid():
        if field == "schema_version":
            continue
        data = valid()
        del data[field]
        assert sensor_json_error(json.dumps(data)) is not None, field


def test_json_duplicate_keys_and_unknown_fields_rejected():
    assert sensor_json_error('{"sensor_id":1,"sensor_id":2}') == "malformed_json"
    assert sensor_json_error('[]') == "expected_object"
    data = valid()
    data["unknown"] = 1
    assert sensor_json_error(json.dumps(data)) == "unexpected_fields"


def test_future_events_cannot_advance_watermark():
    data = valid()
    data["timestamp"] = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    assert sensor_json_error(json.dumps(data)) == "future_timestamp"


def test_huge_numeric_measurement_is_quarantined_without_overflow():
    data = valid()
    data["soil_moisture"] = 10**400
    assert sensor_json_error(json.dumps(data)) == "invalid_soil_moisture"


def test_deeply_nested_poison_json_is_quarantined_without_crashing(monkeypatch):
    depth = sys.getrecursionlimit() + 100
    # CPython decoder limits differ by release: either reject the nesting
    # directly or reject the decoded array, but never fail the admission job.
    assert sensor_json_error("[" * depth + "0" + "]" * depth) in {"malformed_json", "expected_object"}
    def exceeded_stack(*args, **kwargs):
        raise RecursionError("decoder nesting limit")
    monkeypatch.setattr(json, "loads", exceeded_stack)
    assert sensor_json_error("{}") == "malformed_json"


def test_stable_broker_time_gives_same_future_rejection_on_every_replay():
    data = valid()
    ingress = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    data["timestamp"] = (ingress + timedelta(seconds=301)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    raw = json.dumps(data)
    # The test wall clock is many days later than this historical Kafka event;
    # it still fails exactly as it did when accepted/quarantine queries began.
    assert sensor_ingress_error(raw, ingress) == "future_timestamp"
    assert sensor_ingress_error(raw, ingress.replace(tzinfo=None)) == "future_timestamp"
    assert sensor_ingress_error(raw, ingress) == "future_timestamp"


def test_stable_ingress_boundary_and_missing_evidence():
    data = valid()
    ingress = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    data["timestamp"] = (ingress + timedelta(seconds=300)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    raw = json.dumps(data)
    assert sensor_ingress_error(raw, ingress) is None
    assert sensor_ingress_error(raw, None) == "missing_ingress_timestamp"
    assert sensor_ingress_error(raw, "2026-01-01") == "invalid_ingress_timestamp"
