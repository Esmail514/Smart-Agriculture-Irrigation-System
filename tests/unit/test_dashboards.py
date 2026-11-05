"""Dashboard scope, unavailable data, freshness and bounded telemetry."""
import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from src.api import main as api
from src.api.security import account


@pytest.fixture
def client():
    api.app.dependency_overrides[account] = lambda: {
        'id': '2', 'farm_ids': [1], 'farm_roles': {1: 'viewer'}, 'platform_role': 'user', 'capabilities': []}
    yield TestClient(api.app)
    api.app.dependency_overrides.clear()


def test_dashboard_is_scoped_before_data_access(client, monkeypatch):
    monkeypatch.setattr(api, 'pg', lambda *a: pytest.fail('Must not query another farm'))
    assert client.get('/farms/2/dashboard').status_code == 403
    assert client.get('/farms/2/analytics/soil').status_code == 403
    assert client.get('/farms/2/predictions').status_code == 403
    assert client.get('/admin/dashboard').status_code == 403


def test_dashboard_optional_dependencies_and_unknown_measurements(client, monkeypatch):
    monkeypatch.setattr(api, 'fields', lambda *a: [
        {'status': 'unknown', 'moisture_pct': None, 'recommended_water_mm': None}])
    monkeypatch.setattr(api, 'devices', lambda *a: [{'status': 'offline'}])
    monkeypatch.setattr(api, 'alerts', lambda *a: [{'resolved': True}, {'resolved': False}])
    monkeypatch.setattr(api, 'readings', lambda *a, **k: [])
    unavailable = lambda *a: (_ for _ in ()).throw(HTTPException(503, 'Temporarily unavailable'))
    monkeypatch.setattr(api, 'weather', unavailable)
    monkeypatch.setattr(api, 'predictions', unavailable)
    response = client.get('/farms/1/dashboard?range=24h')
    assert response.status_code == 200
    data = response.json()
    assert data['weather'] is None and data['weather_error']
    assert data['predictions'] == [] and data['prediction_error']
    assert data['capabilities'] == []
    assert data['kpis']['unknown_zone_count'] == 1
    assert data['kpis']['online_sensor_count'] == 0
    assert data['kpis']['active_alert_count'] == 1
    assert data['kpis']['avg_soil_moisture'] is None
    assert data['kpis']['recommended_water_mm'] is None
    assert client.get('/farms/1/dashboard?range=invalid').status_code == 422


def test_resolutions_apply_only_to_same_event(client, monkeypatch):
    now = datetime.now(timezone.utc)
    resolution_time = now - timedelta(minutes=1)

    def metadata(sql, *_):
        if 'FROM zones' in sql:
            return [{'zone_id': 3, 'crop_id': 2, 'zone_name': 'Current zone', 'moisture_min': 30, 'moisture_max': 70, 'recommended_water_mm': 10}]
        return [{'zone_id': 3, 'event_time': resolution_time, 'resolved': True}]

    monkeypatch.setattr(api, 'pg', metadata)
    monkeypatch.setattr(api, 'ch', lambda *a: [{'zone_id': 3, 'crop_id': 2, 'zone_name': 'Zone',
                                             'water_status': 'Dry', 'last_update': now, 'moisture_min': 30, 'moisture_max': 70, 'recommended_water_mm': 10}])
    assert client.get('/farms/1/alerts').json()[0]['resolved'] is False
    resolution_time = now
    current = client.get('/farms/1/alerts').json()[0]
    assert current['resolved'] is True
    assert current['message'] == 'Current zone: Dry'


def test_prediction_requires_fresh_valid_evidence(client, monkeypatch):
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(api, 'pg', lambda *args: [{'zone_id': 4, 'crop_id': 2,
        'zone_name': 'Current zone', 'moisture_min': 30, 'moisture_max': 70}])
    def rows(*args):
        return [{'zone_id': index, 'prediction_time': now, 'needs_irrigation_today': 1,
                 'needs_irrigation_tomorrow': 1, 'prediction_confidence': 0.9, 'feature_snapshot': evidence}
                for index, evidence in enumerate([
                    '[]', '{bad json', json.dumps({'observation_time': 'bad-time'}),
                    json.dumps({'observation_time': (now - timedelta(hours=1)).isoformat()}),
                    json.dumps({'observation_time': now.isoformat(), 'irrigation_probability': 0.9,
                                'target': 'current_dryness', 'forecast_available': False,
                                'label_source': 'crop_moisture_threshold_rule', 'crop_id': 2,
                                'moisture_min': 30, 'moisture_max': 70})])]
    monkeypatch.setattr(api, 'ch', rows)
    data = client.get('/farms/1/predictions').json()
    assert len(data) == 1 and data[0]['zone_id'] == '4'
    assert data[0]['needs_irrigation_tomorrow'] is None
    assert data[0]['target'] == 'current_dryness'


def test_metrics_do_not_label_user_urls(client, monkeypatch):
    monkeypatch.setattr(api, 'ch', lambda *a: [])
    client.get('/farms/1/readings?secret=private')
    client.get('/random-private-path')
    text = client.get('/metrics').text
    assert 'route="/farms/{farm_id}/readings"' in text
    assert 'route="unmatched"' in text
    assert 'private' not in text and '/farms/1/' not in text


def test_live_health_is_independent_of_databases(client, monkeypatch):
    monkeypatch.setattr(api, 'pg', lambda *a: pytest.fail('Liveness must not query databases'))
    assert client.get('/health/live').status_code == 200


def test_farms_metadata_survives_measurement_outage(client, monkeypatch):
    monkeypatch.setattr(api, 'pg', lambda *a: [{'farm_id': 1, 'farm_name': 'Own farm', 'field_count': 2,
                                             'latitude': 15, 'longitude': 44}])
    monkeypatch.setattr(api, 'ch', lambda *a: (_ for _ in ()).throw(HTTPException(503, 'Measurements unavailable')))
    response = client.get('/farms')
    assert response.status_code == 200
    assert response.json()[0]['active_alert_count'] is None
    assert response.json()[0]['alerts_available'] is False
