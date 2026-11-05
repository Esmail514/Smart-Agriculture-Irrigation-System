"""API control boundaries with transactional PostgreSQL and private-agent fakes."""
from contextlib import contextmanager
from datetime import datetime, timezone
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from src.api import control_routes as routes
from src.api import control_client


@pytest.fixture
def boundary(monkeypatch):
    user = {"id": "1", "platform_role": "user", "farm_ids": [10], "farm_roles": {10: "viewer"}}
    app = FastAPI()
    app.include_router(routes.router)
    app.dependency_overrides[routes.current_account] = lambda: user
    connection = object()
    calls, audits = [], []
    respond = {"fn": lambda sql, params: []}

    @contextmanager
    def connect():
        yield connection

    def query(sql, params=(), conn=None):
        calls.append((sql, params, conn))
        return respond["fn"](sql, params)

    def audit(conn, actor, action, **kwargs):
        audits.append((conn, actor, action, kwargs))
        return len(audits)

    monkeypatch.setattr(routes.db, "connect", connect)
    monkeypatch.setattr(routes.db, "query", query)
    monkeypatch.setattr(routes.db, "audit", audit)
    return TestClient(app), user, respond, calls, audits, connection


def test_viewer_cannot_queue_or_resolve(boundary):
    client, user, response, calls, audits, _ = boundary
    result = client.post("/farms/10/irrigation-commands", json={"device_id": "pump-a", "duration_seconds": 30, "idempotency_key": "request-123"})
    assert result.status_code == 403
    result = client.patch("/farms/10/alerts/4", json={"resolved": True, "event_time": "2026-10-11T00:00:00Z"})
    assert result.status_code == 403
    assert not calls and not audits


def test_manager_cannot_cross_farm_boundary(boundary):
    client, user, _, calls, audits, _ = boundary
    user["farm_roles"][10] = "manager"
    result = client.post("/farms/20/irrigation-commands", json={"device_id": "pump-a", "duration_seconds": 30, "idempotency_key": "request-123"})
    assert result.status_code == 403
    assert not calls and not audits


def test_nonadmin_cannot_change_global_settings(boundary):
    client, _, _, calls, audits, _ = boundary
    assert client.patch("/admin/settings", json={"values": {"simulator.enabled": False}}).status_code == 403
    assert not calls and not audits


@pytest.mark.parametrize("values", [{"simulator.anomaly_rate": True}, {"simulator.interval_seconds": "1"},
                                     {"simulator.interval_seconds": 0}, {"arbitrary.env": "injected"},
                                     {"irrigation.max_duration_seconds": 1.5}])
def test_settings_reject_non_numeric_or_unallowlisted_values(boundary, values):
    client, user, _, calls, audits, _ = boundary
    user["platform_role"] = "admin"
    assert client.patch("/admin/settings", json={"values": values}).status_code == 422
    assert not calls and not audits


def test_settings_and_audit_share_transaction(boundary):
    client, user, response, calls, audits, connection = boundary
    user["platform_role"] = "admin"
    response["fn"] = lambda sql, params: [{"key": params[0]}]
    result = client.patch("/admin/settings", json={"values": {"simulator.interval_seconds": 2.0}})
    assert result.status_code == 200
    assert result.json()["application_status"] == "saved_configuration"
    assert all(call[2] is connection for call in calls)
    assert audits[0][0] is connection


def test_queue_registered_actuator_is_queued_and_audited(boundary):
    client, user, response, calls, audits, connection = boundary
    user["farm_roles"][10] = "manager"
    def respond(sql, params):
        if "SELECT * FROM api_irrigation_devices" in sql:
            assert "farm_id=%s AND enabled FOR UPDATE" in sql
            assert params == ("pump-a", 10)
            return [{"device_id": "pump-a", "farm_id": 10, "enabled": True}]
        if "INSERT INTO irrigation_commands" in sql:
            return [{"id": params[0], "status": "queued", "device_id": params[2], "duration_seconds": params[3]}]
        return []
    response["fn"] = respond
    result = client.post("/farms/10/irrigation-commands", json={"device_id": "pump-a", "duration_seconds": 30, "idempotency_key": "request-123"})
    assert result.status_code == 202
    assert result.json()["status"] == "queued"
    assert audits[0][2] == "irrigation.queued"
    assert audits[0][0] is connection
    assert all(call[2] is connection for call in calls)


def test_sensor_is_not_an_irrigation_actuator(boundary):
    client, user, _, _, audits, _ = boundary
    user["farm_roles"][10] = "owner"
    result = client.post("/farms/10/irrigation-commands", json={"device_id": "soil-sensor-1", "duration_seconds": 30, "idempotency_key": "request-123"})
    assert result.status_code == 404
    assert not audits


def test_retry_returns_original_command_without_rechecking_device(boundary):
    client, user, response, calls, audits, _ = boundary
    user["farm_roles"][10] = "owner"
    row = {"id": str(uuid.uuid4()), "device_id": "pump-a", "duration_seconds": 30, "status": "cancelled"}
    response["fn"] = lambda sql, params: [row] if "SELECT * FROM irrigation_commands" in sql else []
    result = client.post("/farms/10/irrigation-commands", json={"device_id": "pump-a", "duration_seconds": 30, "idempotency_key": "request-123"})
    assert result.status_code == 202
    assert result.json()["id"] == row["id"]
    assert len(calls) == 1 and not audits


def test_reusing_idempotency_key_for_another_payload_conflicts(boundary):
    client, user, response, _, audits, _ = boundary
    user["farm_roles"][10] = "owner"
    response["fn"] = lambda sql, params: [{"device_id": "pump-a", "duration_seconds": 31}]
    result = client.post("/farms/10/irrigation-commands", json={"device_id": "pump-a", "duration_seconds": 30, "idempotency_key": "request-123"})
    assert result.status_code == 409
    assert not audits


def test_running_command_cannot_be_cancelled(boundary):
    client, user, response, _, audits, _ = boundary
    user["farm_roles"][10] = "manager"
    response["fn"] = lambda sql, params: [{"status": "running"}] if sql.startswith("SELECT") else []
    result = client.post(f"/farms/10/irrigation-commands/{uuid.uuid4()}/cancel")
    assert result.status_code == 409
    assert not audits


def test_unconfigured_agent_returns_unavailable(boundary, monkeypatch):
    client, user, _, _, _, _ = boundary
    user["platform_role"] = "admin"
    monkeypatch.delenv("CONTROL_AGENT_URL", raising=False)
    monkeypatch.delenv("CONTROL_AGENT_TOKEN", raising=False)
    assert client.get("/admin/system/services").status_code == 503


@pytest.mark.parametrize("service,action", [("api", "stop"), ("postgres", "stop"), ("clickhouse", "start"), ("kafka", "shell")])
def test_service_action_allowlist_blocks_invalid_operations(boundary, service, action):
    client, user, _, calls, audits, _ = boundary
    user["platform_role"] = "admin"
    assert client.post(f"/admin/system/services/{service}/{action}").status_code == 422
    assert not calls and not audits


def test_pipeline_pause_requires_json_boolean(boundary):
    client, user, _, calls, audits, _ = boundary
    user["platform_role"] = "admin"
    assert client.patch("/admin/pipelines/ml_pipeline/state", json={"is_paused": "false"}).status_code == 422
    assert not calls and not audits


def test_self_demotion_is_blocked_for_string_user_identifier(boundary):
    client, user, _, calls, audits, _ = boundary
    user["platform_role"] = "admin"
    assert client.patch("/admin/users/1", json={"is_active": False}).status_code == 409
    assert not calls and not audits


def test_password_whitespace_is_preserved():
    assert routes.UserCreate(email="a@b.co", full_name="Person", password=" spaced password ").password == " spaced password "


def test_admin_can_clear_actuator_zone_assignment(boundary):
    client, user, response, calls, audits, _ = boundary
    user["platform_role"] = "admin"
    response["fn"] = lambda sql, params: [{"device_id": "pump-a", "farm_id": 10, "zone_id": None}]
    result = client.patch("/admin/farms/10/irrigation-devices/pump-a", json={"zone_id": None})
    assert result.status_code == 200 and result.json()["zone_id"] is None
    assert not any("FROM zones" in call[0] for call in calls)
    assert any("zone_id=%s" in call[0] and call[1][0] is None for call in calls)
    assert audits[0][2] == "irrigation_device.updated"


def test_nullable_catalog_fields_can_clear_without_allowing_required_nulls(boundary):
    client, user, response, calls, audits, _ = boundary
    user["platform_role"] = "admin"
    response["fn"] = lambda sql, params: [{"farm_id": 10, "city": None}]
    assert client.patch("/admin/farms/10", json={"city": None}).status_code == 200
    assert client.patch("/admin/farms/10", json={"farm_name": None}).status_code == 422
    assert client.patch("/admin/farms/10/irrigation-devices/pump-a", json={"enabled": None}).status_code == 422


def test_alert_resolution_checks_zone_farm_membership(boundary):
    client, user, _, calls, audits, _ = boundary
    user["farm_roles"][10] = "owner"
    result = client.patch("/farms/10/alerts/900", json={"resolved": True, "event_time": "2026-10-11T00:00:00Z"})
    assert result.status_code == 404
    assert "AND farm_id=%s" in calls[0][0]
    assert calls[0][1] == (900, 10)
    assert not audits


def test_resolving_changed_alert_requires_refresh(boundary, monkeypatch):
    from src.api import main
    client, user, response, _, audits, _ = boundary
    user["farm_roles"][10] = "owner"
    response["fn"] = lambda sql, params: [{"zone_id": 4, "farm_id": 10}]
    monkeypatch.setattr(main, "ch", lambda *args: [{"last_update": "2026-10-11T00:01:00Z"}])
    result = client.patch("/farms/10/alerts/4", json={"resolved": True, "event_time": "2026-10-11T00:00:00Z"})
    assert result.status_code == 409
    assert not audits


@pytest.mark.parametrize("changed_rule", [False, True])
def test_resolution_requires_current_rule_even_when_crop_and_event_match(boundary, monkeypatch, changed_rule):
    from src.api import main
    client, user, response, _, audits, _ = boundary
    user["farm_roles"][10] = "owner"
    timestamp = datetime.now(timezone.utc).isoformat()
    rule = {"moisture_min": 30, "moisture_max": 70, "recommended_water_mm": 10}
    def respond(sql, params):
        if "FROM zones" in sql:
            return [{"zone_id": 4, "crop_id": 2, "farm_id": 10}]
        if "FROM irrigation_rules" in sql:
            return [{**rule, "moisture_min": 31 if changed_rule else 30}]
        if "INSERT INTO alert_resolutions" in sql:
            return [{"resolved": True}]
        return []
    response["fn"] = respond
    monkeypatch.setattr(main, "ch", lambda *args: [{"last_update": timestamp, "crop_id": 2, **rule}])
    result = client.patch("/farms/10/alerts/4", json={"resolved": True, "event_time": timestamp})
    assert result.status_code == (409 if changed_rule else 200)
    assert len(audits) == (0 if changed_rule else 1)


def test_http_control_agent_never_redirects_or_exposes_token(monkeypatch):
    monkeypatch.setenv("CONTROL_AGENT_URL", "http://control-agent:8001")
    monkeypatch.setenv("CONTROL_AGENT_TOKEN", "internal-secret")
    class Response:
        status_code = 200
        def json(self):
            return [{"name": "kafka", "state": "running"}]
    received = {}
    def request(method, url, **kwargs):
        received.update(kwargs)
        assert method == "GET" and url == "http://control-agent:8001/services"
        return Response()
    monkeypatch.setattr(control_client.requests, "request", request)
    assert control_client.call_agent("GET", "/services")[0]["name"] == "kafka"
    assert received["allow_redirects"] is False
    assert received["headers"]["Authorization"] == "Bearer internal-secret"


def test_external_action_commits_request_before_call_and_preserves_accepted_result(monkeypatch):
    from fastapi import HTTPException
    user = {"id": "1"}
    monkeypatch.setenv("CONTROL_AGENT_URL", "http://control-agent:8001")
    monkeypatch.setenv("CONTROL_AGENT_TOKEN", "private-test-token")
    events = []
    @contextmanager
    def connect():
        events.append("begin")
        if "agent" in events:
            raise HTTPException(503, "PostgreSQL restarting")
        yield object()
        events.append("committed")
    def audit(*args, **kwargs):
        events.append(args[2])
        return 1
    def agent(*args):
        assert events[-1] == "committed"
        events.append("agent")
        return {"accepted": True, "service": "postgres"}
    monkeypatch.setattr(routes.db, "connect", connect)
    monkeypatch.setattr(routes.db, "audit", audit)
    monkeypatch.setattr(routes, "call_agent", agent)
    result = routes.external_operation(user, "service.restart", "service", "postgres", "POST", "/services/postgres/restart")
    assert result["accepted"] is True
    assert result["outcome_audit_recorded"] is False
    assert "service.restart.requested" in events


def test_external_action_does_not_run_if_intent_audit_cannot_commit(monkeypatch):
    from fastapi import HTTPException
    calls = []
    monkeypatch.setenv("CONTROL_AGENT_URL", "http://control-agent:8001")
    monkeypatch.setenv("CONTROL_AGENT_TOKEN", "private-test-token")
    @contextmanager
    def unavailable():
        raise HTTPException(503, "Database unavailable")
        yield
    monkeypatch.setattr(routes.db, "connect", unavailable)
    monkeypatch.setattr(routes, "call_agent", lambda *args: calls.append(args))
    with pytest.raises(HTTPException):
        routes.external_operation({"id": "1"}, "service.restart", "service", "kafka", "POST", "/services/kafka/restart")
    assert not calls


@pytest.mark.parametrize("upstream_status", [404, 409, 422])
def test_api_control_client_forwards_safe_static_conflict_status(monkeypatch, upstream_status):
    from fastapi import HTTPException
    monkeypatch.setenv("CONTROL_AGENT_URL", "http://control-agent:8010")
    monkeypatch.setenv("CONTROL_AGENT_TOKEN", "private-test-token")
    class Response:
        status_code = upstream_status
        text = "sensitive upstream details"
    monkeypatch.setattr(control_client.requests, "request", lambda *a, **k: Response())
    with pytest.raises(HTTPException) as caught:
        control_client.call_agent("POST", "/services/kafka/start")
    assert caught.value.status_code == upstream_status
    assert "sensitive" not in caught.value.detail


def test_api_control_client_rejects_invalid_envelope_and_uses_outer_timeout(monkeypatch):
    from fastapi import HTTPException
    monkeypatch.setenv("CONTROL_AGENT_URL", "http://control-agent:8010")
    monkeypatch.setenv("CONTROL_AGENT_TOKEN", "private-test-token")
    class Response:
        status_code = 200
        def json(self):
            return {"services": []}
    def request(*args, **kwargs):
        assert kwargs["timeout"] == (3, 30)
        return Response()
    monkeypatch.setattr(control_client.requests, "request", request)
    with pytest.raises(HTTPException) as caught:
        control_client.call_agent("GET", "/services")
    assert caught.value.status_code == 503
