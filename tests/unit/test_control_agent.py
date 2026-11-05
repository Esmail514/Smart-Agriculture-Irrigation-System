"""Private control-agent behavior with Docker and Airflow adapters isolated."""
from types import SimpleNamespace
import docker
import pytest
from fastapi.testclient import TestClient
from src.control_agent import main as control


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv('CONTROL_AGENT_TOKEN', 'private-test-token')
    return TestClient(control.app, headers={'Authorization': 'Bearer private-test-token'})


def test_auth_and_allowlist_reject_before_engine_access(client, monkeypatch):
    monkeypatch.setattr(control, 'client', lambda: pytest.fail('Docker must not be called'))
    assert client.get('/services', headers={'Authorization': 'Bearer wrong'}).status_code == 401
    assert client.post('/services/api/stop').status_code == 404
    assert client.post('/services/postgres/stop').status_code == 422
    assert client.post('/services/minio/stop').status_code == 422
    assert client.post('/services/kafka/exec').status_code == 422
    monkeypatch.setenv('CONTROL_AGENT_TOKEN', '')
    assert client.get('/services').status_code == 503


def test_start_running_container_is_idempotent(client, monkeypatch):
    calls = []
    container = SimpleNamespace(status='running', reload=lambda: calls.append('reload'),
                                start=lambda: pytest.fail('Already running'))
    engine = SimpleNamespace(containers=SimpleNamespace(get=lambda name: container), close=lambda: calls.append('close'))
    monkeypatch.setattr(control, 'client', lambda: engine)
    response = client.post('/services/kafka/start')
    assert response.status_code == 200 and response.json()['changed'] is False
    assert calls == ['reload', 'close']


def test_missing_optional_container_reports_unprovisioned(client, monkeypatch):
    def missing(name):
        raise docker.errors.NotFound('missing secret')
    monkeypatch.setattr(control, 'client', lambda: SimpleNamespace(containers=SimpleNamespace(get=missing), close=lambda: None))
    data = client.get('/services').json()
    assert all(row['available'] is False and row['allowed_actions'] == [] for row in data)
    response = client.post('/services/sensor-simulator/start')
    assert response.status_code == 409 and 'secret' not in response.text


def test_airflow_actions_use_fixed_dags_and_strict_state(client, monkeypatch):
    calls = []
    def airflow(method, path, payload=None):
        calls.append((method, path, payload))
        return {'dag_id': 'ml_pipeline', 'is_paused': False, 'dag_run_id': payload.get('dag_run_id', 'run'), 'state': 'queued'}
    monkeypatch.setattr(control, 'airflow', airflow)
    assert client.post('/pipelines/arbitrary/trigger').status_code == 404
    assert client.patch('/pipelines/ml_pipeline/state', json={'is_paused': 'false'}).status_code == 422
    assert not calls
    assert client.patch('/pipelines/ml_pipeline/state', json={'is_paused': False}).status_code == 200
    response = client.post('/pipelines/ml_pipeline/trigger')
    assert response.status_code == 200 and response.json()['accepted'] is True
    assert calls[0] == ('PATCH', 'dags/ml_pipeline', {'is_paused': False})
    assert calls[1][1] == 'dags/ml_pipeline/dagRuns' and calls[1][2]['conf'] == {}


@pytest.mark.parametrize('data', [[], {'dag_id': 'ml_pipeline', 'is_paused': 'false'},
                                 {'dag_id': 'another_dag', 'is_paused': False}])
def test_airflow_adapter_rejects_invalid_state_payloads(client, monkeypatch, data):
    monkeypatch.setenv('AIRFLOW_ADMIN_USER', 'admin')
    monkeypatch.setenv('AIRFLOW_ADMIN_PASSWORD', 'private-secret')
    class Response:
        status_code = 200
        def raise_for_status(self):
            pass
        def json(self):
            return data
    monkeypatch.setattr(control.requests, 'request', lambda *a, **k: Response())
    response = client.patch('/pipelines/ml_pipeline/state', json={'is_paused': False})
    assert response.status_code == 503
    assert 'private-secret' not in response.text


def test_airflow_adapter_refuses_redirect_without_sending_auth_to_another_host(client, monkeypatch):
    monkeypatch.setenv('AIRFLOW_ADMIN_USER', 'admin')
    monkeypatch.setenv('AIRFLOW_ADMIN_PASSWORD', 'private-secret')
    received = {}
    def request(*args, **kwargs):
        received.update(kwargs)
        return SimpleNamespace(status_code=302)
    monkeypatch.setattr(control.requests, 'request', request)
    response = client.post('/pipelines/ml_pipeline/trigger')
    assert response.status_code == 503
    assert received['allow_redirects'] is False
    assert received['timeout'] == (3, 5)


def test_docker_transport_timeout_is_sanitized(client, monkeypatch):
    import requests
    def timeout(name):
        raise requests.Timeout('internal secret socket details')
    monkeypatch.setattr(control, 'client', lambda: SimpleNamespace(containers=SimpleNamespace(get=timeout), close=lambda: None))
    response = client.get('/services')
    assert response.status_code == 503
    assert 'secret' not in response.text


def test_airflow_valid_boolean_false_is_preserved(client, monkeypatch):
    monkeypatch.setenv('AIRFLOW_ADMIN_USER', 'admin')
    monkeypatch.setenv('AIRFLOW_ADMIN_PASSWORD', 'private-secret')
    class Response:
        status_code = 200
        def raise_for_status(self):
            pass
        def json(self):
            return {'dag_id': 'ml_pipeline', 'is_paused': False, 'is_active': True}
    monkeypatch.setattr(control.requests, 'request', lambda *a, **k: Response())
    response = client.patch('/pipelines/ml_pipeline/state', json={'is_paused': False})
    assert response.status_code == 200
    assert response.json()['is_paused'] is False


def test_airflow_invalid_run_state_returns_503_instead_of_type_error(client, monkeypatch):
    monkeypatch.setenv('AIRFLOW_ADMIN_USER', 'admin')
    monkeypatch.setenv('AIRFLOW_ADMIN_PASSWORD', 'private-secret')
    class Response:
        status_code = 200
        def raise_for_status(self):
            pass
        def json(self):
            return {'dag_run_id': 'run-1', 'state': []}
    monkeypatch.setattr(control.requests, 'request', lambda *a, **k: Response())
    assert client.post('/pipelines/ml_pipeline/trigger').status_code == 503
