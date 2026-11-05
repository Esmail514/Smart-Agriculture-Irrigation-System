from datetime import datetime, timezone
import json
import pytest
from fastapi.testclient import TestClient
from src.api import main as api

client = TestClient(api.app)
HEADER = {"Authorization": "Bearer test-token"}


def setup_account(monkeypatch):
    monkeypatch.setenv("API_ACCOUNTS_JSON", json.dumps([
        {"token": "test-token", "id": "u1", "farm_ids": [1], "full_name": "Viewer"}]))


def test_token_auth_and_scope_fail_closed(monkeypatch):
    setup_account(monkeypatch)
    assert client.get("/me").status_code == 401
    assert client.get("/me", headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert client.get("/me", headers=HEADER).json()["capabilities"] == []
    monkeypatch.setattr(api, "ch", lambda *args: (_ for _ in ()).throw(AssertionError("must not query")))
    for endpoint in ["fields", "devices", "weather", "readings", "alerts"]:
        assert client.get(f"/farms/2/{endpoint}", headers=HEADER).status_code == 403


def test_fields_stale_data_not_presented_as_current(monkeypatch):
    setup_account(monkeypatch)
    monkeypatch.setattr(api, "pg", lambda *args: [{"zone_id": 10, "crop_name": "Tomato", "zone_name": "Z10", "area_hectare": 1.2}])
    monkeypatch.setattr(api, "ch", lambda *args: [{"zone_id": 10, "avg_soil_moisture": 70, "last_update": "2020-01-01 00:00:00", "water_status": "Optimal"}])
    field = client.get("/farms/1/fields", headers=HEADER).json()[0]
    assert field["moisture_pct"] is None
    assert field["status"] == "unknown"
    assert field["last_watered"] is None
    assert field["irrigation_status"] == "unknown"


def test_observed_weather_units_and_realistic_forecast(monkeypatch):
    setup_account(monkeypatch)
    monkeypatch.setattr(api, "ch", lambda *args: [{"temperature": 30, "humidity": 60, "wind_speed": 10,
        "weather_condition": "Clear", "observation_time_utc": datetime.now(timezone.utc).isoformat()}])
    response = client.get("/farms/1/weather", headers=HEADER)
    assert response.status_code == 200
    data = response.json()
    assert data["current"]["wind_speed_kmh"] == 36
    assert data["current"]["rain_chance_pct"] is None
    assert len(data["forecast"]) == 5
    for item in data["forecast"]:
        assert "day" in item
        assert "temp_max" in item and "temp_min" in item
        assert "condition" in item
        assert "rain_chance_pct" in item
        assert item["temp_max"] > item["temp_min"]


def test_weather_stale_returns_503(monkeypatch):
    setup_account(monkeypatch)
    monkeypatch.setattr(api, "ch", lambda *args: [{"observation_time_utc": "2020-01-01 00:00:00"}])
    assert client.get("/farms/1/weather", headers=HEADER).status_code == 503


def test_metric_parameter_whitelist_and_farm_bound_sql(monkeypatch):
    setup_account(monkeypatch)
    calls = []
    monkeypatch.setattr(api, "ch", lambda sql, params: calls.append((sql, params)) or [])
    assert client.get("/farms/1/readings?metric=x%3BDROP", headers=HEADER).status_code == 422
    assert not calls
    assert client.get("/farms/1/readings?field_id=123&range=7d", headers=HEADER).status_code == 200
    assert "farm_id={farm:Int64}" in calls[0][0]
    assert calls[0][1] == {"farm": 1, "days": 7, "zone": 123}


def test_backend_errors_do_not_expose_secrets(monkeypatch):
    import requests
    monkeypatch.setattr(api, "pg", lambda *args: [])
    monkeypatch.setattr(api.requests, "post", lambda *a, **k: (_ for _ in ()).throw(requests.Timeout("secret")))
    response = client.get("/health")
    # PostgreSQL may fail first; either dependency failure stays a clean 503.
    assert response.status_code == 503
    assert "secret" not in response.text


@pytest.mark.parametrize("payload", ['null', '[]', '{"value": NaN}', '{"value": Infinity}', '{bad'])
def test_invalid_warehouse_response_stays_clean_unavailable(monkeypatch, payload):
    class Response:
        status_code = 200
        text = payload
        def raise_for_status(self): pass
    monkeypatch.setattr(api, "pg", lambda *args: [])
    monkeypatch.setattr(api.requests, "post", lambda *args, **kwargs: Response())
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json()["detail"] == "Measurements unavailable"
