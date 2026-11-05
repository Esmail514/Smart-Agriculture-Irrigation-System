"""Wire-level dashboard contracts: ClickHouse integer strings and ML evidence."""
from datetime import datetime, timezone
import json

import pytest
from fastapi.testclient import TestClient
from src.api import main as api


@pytest.fixture
def client(monkeypatch):
    user = {"id": "1", "platform_role": "admin", "farm_ids": [1], "farm_roles": {1: "owner"}, "capabilities": []}
    api.app.dependency_overrides[api.account] = lambda: user
    monkeypatch.setattr(api, "pg", lambda *args: [{"zone_id": 10, "crop_id": 7,
        "zone_name": "Zone", "moisture_min": 30.0, "moisture_max": 70.0}])
    yield TestClient(api.app)
    api.app.dependency_overrides.pop(api.account, None)


def now():
    return datetime.now(timezone.utc).isoformat()


def test_field_sensor_count_is_json_integer_when_clickhouse_quotes_uint64(client, monkeypatch):
    monkeypatch.setattr(api, "pg", lambda *args: [{"zone_id": 10, "crop_name": "Tomato", "zone_name": "Zone", "area_hectare": 1}])
    monkeypatch.setattr(api, "ch", lambda *args: [{"zone_id": "10", "last_update": now(), "water_status": "Optimal",
        "sensor_count": "8", "avg_soil_moisture": 40.0}])
    result = client.get("/farms/1/fields")
    assert result.status_code == 200
    assert result.json()[0]["sensor_count"] == 8
    assert type(result.json()[0]["sensor_count"]) is int


def test_admin_dashboard_counts_are_numbers_not_clickhouse_strings(client, monkeypatch):
    monkeypatch.setattr(api, "pg", lambda *args: [{"farm_count": 1}])
    monkeypatch.setattr(api, "ch", lambda *args: [{"reporting_zone_count": "4", "dry_zone_count": "2", "latest_event_time": now()}])
    result = client.get("/admin/dashboard")
    assert result.status_code == 200
    assert result.json()["measurements"]["reporting_zone_count"] == 4
    assert result.json()["measurements"]["dry_zone_count"] == 2


def prediction(evidence):
    evidence = {"crop_id": 7, "moisture_min": 30.0, "moisture_max": 70.0, **evidence}
    return {"zone_id": "10", "zone_name": "Zone", "needs_irrigation_today": 1,
            "needs_irrigation_tomorrow": 1, "prediction_confidence": 0.8,
            "model_version": "v2", "prediction_time": now(), "feature_snapshot": json.dumps(evidence)}


@pytest.mark.parametrize("evidence", [{"observation_time": now(), "target": "tomorrow_irrigation"},
    {"observation_time": now(), "target": "current_dryness", "forecast_available": True},
    {"observation_time": now()}])
def test_api_does_not_relabel_unsupported_ml_evidence_as_current_dryness(client, monkeypatch, evidence):
    monkeypatch.setattr(api, "ch", lambda *args: [prediction(evidence)])
    result = client.get("/farms/1/predictions")
    assert result.status_code == 200
    assert result.json() == []


def test_valid_prediction_preserves_probability_and_has_no_fake_tomorrow(client, monkeypatch):
    evidence = {"observation_time": now(), "target": "current_dryness", "forecast_available": False,
                "label_source": "crop_moisture_threshold_rule", "irrigation_probability": 0.8}
    monkeypatch.setattr(api, "ch", lambda *args: [prediction(evidence)])
    result = client.get("/farms/1/predictions")
    assert result.status_code == 200
    row = result.json()[0]
    assert row["irrigation_probability"] == 0.8
    assert row["needs_irrigation_tomorrow"] is None
    assert row["target"] == "current_dryness"


def test_new_alert_event_is_not_resolved_by_old_event_resolution(client, monkeypatch):
    timestamp = now()
    monkeypatch.setattr(api, "ch", lambda *args: [{"zone_id": "10", "crop_id": "7", "zone_name": "Zone", "water_status": "Dry", "last_update": timestamp, "moisture_min": 30, "moisture_max": 70, "recommended_water_mm": 10}])
    monkeypatch.setattr(api, "pg", lambda sql, *args: [{"zone_id": 10, "crop_id": 7, "zone_name": "Zone", "moisture_min": 30, "moisture_max": 70, "recommended_water_mm": 10}]
                        if "FROM zones" in sql else [{"zone_id": 10, "event_time": "2020-01-01T00:00:00Z", "resolved": True}])
    result = client.get("/farms/1/alerts")
    assert result.status_code == 200
    assert result.json()[0]["resolved"] is False


def test_changed_crop_metadata_does_not_reuse_previous_crop_recommendation(client, monkeypatch):
    monkeypatch.setattr(api, "pg", lambda *args: [{"zone_id": 10, "crop_id": 20, "crop_name": "New crop", "zone_name": "Zone", "area_hectare": 1}])
    monkeypatch.setattr(api, "ch", lambda *args: [{"zone_id": "10", "crop_id": "7", "last_update": now(), "water_status": "Dry",
        "sensor_count": "8", "avg_soil_moisture": 15.0, "recommended_water_mm": 50.0}])
    result = client.get("/farms/1/fields")
    assert result.status_code == 200
    assert result.json()[0]["crop"] == "New crop"
    assert result.json()[0]["status"] == "unknown"
    assert result.json()[0]["recommended_water_mm"] is None


@pytest.mark.parametrize("crop_id,timestamp", [(8, None), (7, "2099-01-01T00:00:00Z"), (7, "invalid")])
def test_alerts_hide_changed_crop_and_unusable_observation_times(client, monkeypatch, crop_id, timestamp):
    monkeypatch.setattr(api, "ch", lambda *args: [{"zone_id": "10", "crop_id": crop_id,
        "zone_name": "Old name", "water_status": "Dry", "last_update": timestamp or now()}])
    monkeypatch.setattr(api, "pg", lambda sql, *args: [{"zone_id": 10, "crop_id": 7, "zone_name": "Current name", "moisture_min": 30, "moisture_max": 70, "recommended_water_mm": 10}]
                        if "FROM zones" in sql else [])
    assert client.get("/farms/1/alerts").json() == []


def test_alert_uses_authoritative_current_zone_name(client, monkeypatch):
    monkeypatch.setattr(api, "ch", lambda *args: [{"zone_id": "10", "crop_id": 7,
        "zone_name": "Old name", "water_status": "Dry", "last_update": now(), "moisture_min": 30, "moisture_max": 70, "recommended_water_mm": 10}])
    monkeypatch.setattr(api, "pg", lambda sql, *args: [{"zone_id": 10, "crop_id": 7, "zone_name": "Current name", "moisture_min": 30, "moisture_max": 70, "recommended_water_mm": 10}]
                        if "FROM zones" in sql else [])
    assert client.get("/farms/1/alerts").json()[0]["message"] == "Current name: Dry"


def test_farm_access_does_not_depend_on_measurement_store(client, monkeypatch):
    def unavailable(*args):
        raise AssertionError("Access must not query measurements or catalog")
    monkeypatch.setattr(api, "ch", unavailable)
    monkeypatch.setattr(api, "pg", unavailable)
    assert client.get("/farms/1/access").json() == {
        "farm_id": "1", "capabilities": ["resolve_alerts", "control_irrigation"]}
    assert client.get("/farms/2/access").status_code == 403


@pytest.mark.parametrize("metadata", [{"crop_id": 8}, {"moisture_min": 25}, {"moisture_max": None}, {"crop_id": True}])
def test_predictions_hide_changed_crop_rules_or_missing_metadata(client, monkeypatch, metadata):
    evidence = {"observation_time": now(), "target": "current_dryness", "forecast_available": False,
                "label_source": "crop_moisture_threshold_rule", "irrigation_probability": 0.8, **metadata}
    monkeypatch.setattr(api, "ch", lambda *args: [prediction(evidence)])
    assert client.get("/farms/1/predictions").json() == []


@pytest.mark.parametrize("changed", ["moisture_min", "moisture_max", "recommended_water_mm"])
def test_field_decisions_are_invalidated_when_same_crop_rule_changes(client, monkeypatch, changed):
    rule = {"moisture_min": 30, "moisture_max": 70, "recommended_water_mm": 10}
    catalog = {"zone_id": 10, "crop_id": 7, "crop_name": "Tomato", "zone_name": "Zone", "area_hectare": 1, **rule}
    observation = {"zone_id": 10, "crop_id": 7, "last_update": now(), "water_status": "Dry",
                   "sensor_count": 1, "avg_soil_moisture": 15, **rule}
    monkeypatch.setattr(api, "pg", lambda *args: [catalog])
    monkeypatch.setattr(api, "ch", lambda *args: [observation])
    assert client.get("/farms/1/fields").json()[0]["recommended_water_mm"] == 10
    catalog[changed] += 1
    data = client.get("/farms/1/fields").json()[0]
    assert data["status"] == "unknown" and data["recommended_water_mm"] is None
    assert data["moisture_pct"] == 15


def test_malformed_field_timestamp_becomes_unknown_instead_of_server_error(client, monkeypatch):
    monkeypatch.setattr(api, "pg", lambda *args: [{"zone_id": 10, "crop_name": "Tomato", "zone_name": "Zone", "area_hectare": 1}])
    monkeypatch.setattr(api, "ch", lambda *args: [{"zone_id": 10, "last_update": "not-a-date"}])
    response = client.get("/farms/1/fields")
    assert response.status_code == 200
    assert response.json()[0]["last_observed_at"] is None


def test_rule_guard_tolerates_aggregate_roundoff_but_not_real_rule_change():
    rule = {"crop_id": 7, "moisture_min": 30.1, "moisture_max": 70.2, "recommended_water_mm": 10.3}
    observation = {**rule, "moisture_min": 30.10000000000001}
    assert api.current_rule(observation, rule)
    assert not api.current_rule(observation, {**rule, "moisture_min": 30.11})
