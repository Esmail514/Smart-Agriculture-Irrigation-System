"""Unit tests for the Actuator Execution Service (Closed-Loop Irrigation Control)."""
import os
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from src.control.actuator_executor import (
    execute_pending_commands,
    execute_single_command,
    is_executor_connected,
    set_executor_connected,
    ActuatorExecutorWorker,
)


class FakeDb:
    def __init__(self):
        self.commands = {}
        self.devices = {}
        self.audits = []

    def query(self, sql, params=(), conn=None):
        sql_upper = sql.upper().strip()

        # SELECT pending queued commands
        if "FROM IRRIGATION_COMMANDS WHERE STATUS='QUEUED'" in sql_upper:
            limit = params[0] if params else 10
            queued = [
                dict(cmd)
                for cmd in self.commands.values()
                if cmd.get("status") == "queued"
            ]
            return queued[:limit]

        # SELECT single command status
        if "SELECT STATUS FROM IRRIGATION_COMMANDS WHERE ID=" in sql_upper:
            cmd_id = str(params[0])
            if cmd_id in self.commands:
                return [{"status": self.commands[cmd_id]["status"]}]
            return []

        # SELECT device enabled status
        if "FROM API_IRRIGATION_DEVICES" in sql_upper:
            farm_id, device_id = params[0], params[1]
            key = (farm_id, device_id)
            if key in self.devices:
                return [{"enabled": self.devices[key]["enabled"]}]
            return []

        # UPDATE to failed
        if "SET STATUS='FAILED'" in sql_upper:
            cmd_id = str(params[0])
            if cmd_id in self.commands and self.commands[cmd_id]["status"] == "queued":
                self.commands[cmd_id]["status"] = "failed"
                self.commands[cmd_id]["failure_reason"] = "device_disabled"
                self.commands[cmd_id]["updated_at"] = datetime.now(timezone.utc)
                return [dict(self.commands[cmd_id])]
            return []

        # UPDATE to running
        if "SET STATUS='RUNNING'" in sql_upper:
            cmd_id = str(params[0])
            if cmd_id in self.commands and self.commands[cmd_id]["status"] == "queued":
                self.commands[cmd_id]["status"] = "running"
                self.commands[cmd_id]["started_at"] = datetime.now(timezone.utc)
                self.commands[cmd_id]["updated_at"] = datetime.now(timezone.utc)
                return [dict(self.commands[cmd_id])]
            return []

        # UPDATE to completed
        if "SET STATUS='COMPLETED'" in sql_upper:
            cmd_id = str(params[0])
            if cmd_id in self.commands and self.commands[cmd_id]["status"] == "running":
                self.commands[cmd_id]["status"] = "completed"
                self.commands[cmd_id]["completed_at"] = datetime.now(timezone.utc)
                self.commands[cmd_id]["updated_at"] = datetime.now(timezone.utc)
                return [dict(self.commands[cmd_id])]
            return []

        return []

    def audit(self, conn, actor_id, action, *, farm_id=None, target_type=None, target_id=None, details=None):
        event = {
            "id": len(self.audits) + 1,
            "actor_id": actor_id,
            "action": action,
            "farm_id": farm_id,
            "target_type": target_type,
            "target_id": target_id,
            "details": details or {},
        }
        self.audits.append(event)
        return event["id"]


@pytest.fixture
def fake_db(monkeypatch):
    db_instance = FakeDb()
    from src.control import actuator_executor
    monkeypatch.setattr(actuator_executor.db, "query", db_instance.query)
    monkeypatch.setattr(actuator_executor.db, "audit", db_instance.audit)
    set_executor_connected(False)
    yield db_instance
    set_executor_connected(False)


def test_executor_connection_reporting(monkeypatch):
    set_executor_connected(False)
    assert not is_executor_connected()

    # Manual registration
    set_executor_connected(True)
    assert is_executor_connected()
    set_executor_connected(False)
    assert not is_executor_connected()

    # Environment variable check
    monkeypatch.setenv("IRRIGATION_EXECUTOR_CONNECTED", "true")
    assert is_executor_connected()
    monkeypatch.setenv("IRRIGATION_EXECUTOR_CONNECTED", "false")
    assert not is_executor_connected()


def test_command_execution_success(fake_db):
    cmd_id = str(uuid.uuid4())
    fake_db.devices[(1, "pump-north")] = {"enabled": True}
    fake_db.commands[cmd_id] = {
        "id": cmd_id,
        "farm_id": 1,
        "device_id": "pump-north",
        "duration_seconds": 60,
        "idempotency_key": "cmd-001",
        "requested_by": 42,
        "status": "queued",
        "created_at": datetime.now(timezone.utc),
    }

    mock_kafka = MagicMock()
    results = execute_pending_commands(
        db_conn=object(),
        max_commands=5,
        simulate_time=True,
        kafka_producer=mock_kafka,
    )

    assert len(results) == 1
    assert results[0]["status"] == "completed"
    assert results[0]["started_at"] is not None
    assert results[0]["completed_at"] is not None

    # Check database state
    stored = fake_db.commands[cmd_id]
    assert stored["status"] == "completed"

    # Check audits
    audit_actions = [a["action"] for a in fake_db.audits]
    assert "irrigation.started" in audit_actions
    assert "irrigation.completed" in audit_actions

    # Check Kafka events
    assert mock_kafka.emit.call_count >= 2
    emitted_types = [call[0][0] for call in mock_kafka.emit.call_args_list]
    assert "irrigation.started" in emitted_types
    assert "irrigation.completed" in emitted_types


def test_command_execution_fails_on_disabled_device(fake_db):
    cmd_id = str(uuid.uuid4())
    # Device exists but is disabled
    fake_db.devices[(1, "pump-broken")] = {"enabled": False}
    fake_db.commands[cmd_id] = {
        "id": cmd_id,
        "farm_id": 1,
        "device_id": "pump-broken",
        "duration_seconds": 30,
        "idempotency_key": "cmd-002",
        "requested_by": 42,
        "status": "queued",
        "created_at": datetime.now(timezone.utc),
    }

    mock_kafka = MagicMock()
    results = execute_pending_commands(
        db_conn=object(),
        kafka_producer=mock_kafka,
    )

    assert len(results) == 1
    assert results[0]["status"] == "failed"
    assert results[0]["failure_reason"] == "device_disabled"

    # Audits
    audit_actions = [a["action"] for a in fake_db.audits]
    assert "irrigation.failed" in audit_actions
    failed_audit = next(a for a in fake_db.audits if a["action"] == "irrigation.failed")
    assert failed_audit["details"]["failure_reason"] == "device_disabled"


def test_command_execution_fails_on_missing_device(fake_db):
    cmd_id = str(uuid.uuid4())
    # Device does not exist in api_irrigation_devices
    fake_db.commands[cmd_id] = {
        "id": cmd_id,
        "farm_id": 1,
        "device_id": "nonexistent-valve",
        "duration_seconds": 30,
        "idempotency_key": "cmd-003",
        "requested_by": 42,
        "status": "queued",
        "created_at": datetime.now(timezone.utc),
    }

    results = execute_pending_commands(db_conn=object())
    assert len(results) == 1
    assert results[0]["status"] == "failed"
    assert results[0]["failure_reason"] == "device_disabled"


def test_cancellation_before_execution(fake_db):
    cmd_id = str(uuid.uuid4())
    fake_db.devices[(1, "pump-north")] = {"enabled": True}
    fake_db.commands[cmd_id] = {
        "id": cmd_id,
        "farm_id": 1,
        "device_id": "pump-north",
        "duration_seconds": 60,
        "idempotency_key": "cmd-004",
        "requested_by": 42,
        "status": "cancelled",  # Cancelled before executor gets to it
        "created_at": datetime.now(timezone.utc),
    }

    cmd = dict(fake_db.commands[cmd_id])
    res = execute_single_command(cmd, conn=object())
    assert res["status"] == "cancelled"
    assert "started_at" not in fake_db.commands[cmd_id]
    assert len(fake_db.audits) == 0


def test_cancellation_during_execution(fake_db, monkeypatch):
    cmd_id = str(uuid.uuid4())
    fake_db.devices[(1, "pump-north")] = {"enabled": True}
    fake_db.commands[cmd_id] = {
        "id": cmd_id,
        "farm_id": 1,
        "device_id": "pump-north",
        "duration_seconds": 60,
        "idempotency_key": "cmd-005",
        "requested_by": 42,
        "status": "queued",
        "created_at": datetime.now(timezone.utc),
    }

    # Simulate cancellation happening during the delay/run
    original_query = fake_db.query

    def query_with_mid_run_cancellation(sql, params=(), conn=None):
        if "SELECT STATUS FROM IRRIGATION_COMMANDS WHERE ID=" in sql.upper():
            # If command was transitioned to running, cancel it
            if fake_db.commands[cmd_id]["status"] == "running":
                fake_db.commands[cmd_id]["status"] = "cancelled"
        return original_query(sql, params, conn)

    from src.control import actuator_executor
    monkeypatch.setattr(actuator_executor.db, "query", query_with_mid_run_cancellation)

    cmd = dict(fake_db.commands[cmd_id])
    res = execute_single_command(cmd, conn=object(), simulate_time=True, step_delay=0.01)
    assert res["status"] == "cancelled"
    # Started audit should exist, but completed audit must NOT exist
    audit_actions = [a["action"] for a in fake_db.audits]
    assert "irrigation.started" in audit_actions
    assert "irrigation.completed" not in audit_actions


def test_actuator_executor_worker_lifecycle(fake_db):
    worker = ActuatorExecutorWorker(
        poll_interval=0.05,
        max_commands=5,
        simulate_time=True,
        db_conn=object(),
    )

    assert not worker.is_active()
    assert not is_executor_connected()

    with worker:
        assert worker.is_active()
        assert is_executor_connected()

    assert not worker.is_active()
    assert not is_executor_connected()
