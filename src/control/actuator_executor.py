"""Actuator Execution Service (Closed-Loop Irrigation Control).

Polls and executes pending irrigation commands:
- Queries PostgreSQL `irrigation_commands` where `status = 'queued'`.
- Validates device enablement in `api_irrigation_devices`.
  If disabled, marks command as `failed` with `failure_reason = 'device_disabled'`.
- Transitions status to `running` with `started_at = now()`.
- Supports cancellations (`status = 'cancelled'`) before and during command execution.
- Emits audit log events (`irrigation.started`, `irrigation.completed`, `irrigation.failed`).
- Emits Kafka command events to topic `agri.irrigation.commands` when Kafka is available.
- Completes execution with `status = 'completed'` and `completed_at = now()`.
"""
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

from src.api import db

logger = logging.getLogger(__name__)

KAFKA_TOPIC_IRRIGATION = "agri.irrigation.commands"

# Registry for tracking running worker instances and connected state
_ACTIVE_WORKERS: set["ActuatorExecutorWorker"] = set()
_MANUAL_CONNECTED_FLAG: bool = False


def is_executor_connected() -> bool:
    """Check if an actuator executor is registered, running, or configured active."""
    if _MANUAL_CONNECTED_FLAG:
        return True
    if any(worker.is_active() for worker in list(_ACTIVE_WORKERS)):
        return True
    env_flag = os.getenv("IRRIGATION_EXECUTOR_CONNECTED", "").strip().lower()
    return env_flag in ("1", "true", "yes", "on")


def set_executor_connected(connected: bool) -> None:
    """Manually register or unregister the executor connected status."""
    global _MANUAL_CONNECTED_FLAG
    _MANUAL_CONNECTED_FLAG = bool(connected)


class ActuatorKafkaClient:
    """Optional Kafka client helper for emitting actuator command events."""

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self._producer = None
        self._initialized = False

    def _get_producer(self):
        if self._initialized:
            return self._producer
        self._initialized = True
        if not self.bootstrap_servers:
            return None
        try:
            from kafka import KafkaProducer  # type: ignore

            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=3000,
                max_block_ms=3000,
            )
        except Exception as exc:
            logger.debug("Kafka producer unavailable for actuator events: %s", exc)
            self._producer = None
        return self._producer

    def emit(self, event_type: str, payload: dict[str, Any]) -> bool:
        """Emit command event to Kafka if available."""
        producer = self._get_producer()
        if not producer:
            return False
        try:
            event = {
                "event_type": event_type,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                **payload,
            }
            producer.send(KAFKA_TOPIC_IRRIGATION, event)
            producer.flush(timeout=1.0)
            return True
        except Exception as exc:
            logger.warning("Failed to publish actuator event %s to Kafka: %s", event_type, exc)
            return False

    def close(self) -> None:
        if self._producer:
            try:
                self._producer.close(timeout=1.0)
            except Exception:
                pass
            self._producer = None
        self._initialized = False


def _emit_kafka(producer, event_type: str, payload: dict[str, Any]) -> None:
    if producer is not None:
        if hasattr(producer, "emit"):
            producer.emit(event_type, payload)
        elif hasattr(producer, "send"):
            try:
                event = {
                    "event_type": event_type,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    **payload,
                }
                producer.send(KAFKA_TOPIC_IRRIGATION, json.dumps(event).encode("utf-8"))
            except Exception as exc:
                logger.debug("Failed sending Kafka event: %s", exc)


def execute_single_command(
    command: dict[str, Any],
    conn=None,
    simulate_time: bool = True,
    step_delay: float = 0.0,
    kafka_client=None,
) -> dict[str, Any]:
    """Execute a single irrigation command transitioning through queued -> running -> completed/failed."""
    cmd_id = str(command["id"])
    farm_id = command["farm_id"]
    device_id = command["device_id"]
    duration_seconds = command.get("duration_seconds", 0)
    requested_by = command.get("requested_by")

    # 1. Verify not cancelled before running
    check_rows = db.query(
        "SELECT status FROM irrigation_commands WHERE id=%s",
        (cmd_id,),
        conn=conn,
    )
    if not check_rows or check_rows[0]["status"] != "queued":
        logger.info("Command %s was cancelled or processed prior to start", cmd_id)
        return check_rows[0] if check_rows else command

    # 2. Check device enablement in api_irrigation_devices
    device_rows = db.query(
        "SELECT enabled FROM api_irrigation_devices WHERE farm_id=%s AND device_id=%s",
        (farm_id, device_id),
        conn=conn,
    )
    is_enabled = bool(device_rows and device_rows[0].get("enabled", False))

    if not is_enabled:
        failed_rows = db.query(
            "UPDATE irrigation_commands SET status='failed', failure_reason='device_disabled', updated_at=now() "
            "WHERE id=%s AND status='queued' RETURNING *",
            (cmd_id,),
            conn=conn,
        )
        if failed_rows:
            db.audit(
                conn,
                requested_by,
                "irrigation.failed",
                farm_id=farm_id,
                target_type="irrigation_command",
                target_id=cmd_id,
                details={"failure_reason": "device_disabled", "device_id": device_id},
            )
            _emit_kafka(
                kafka_client,
                "irrigation.failed",
                {
                    "command_id": cmd_id,
                    "farm_id": farm_id,
                    "device_id": device_id,
                    "status": "failed",
                    "failure_reason": "device_disabled",
                },
            )
            return failed_rows[0]
        return command

    # 3. Transition to running
    running_rows = db.query(
        "UPDATE irrigation_commands SET status='running', started_at=now(), updated_at=now() "
        "WHERE id=%s AND status='queued' RETURNING *",
        (cmd_id,),
        conn=conn,
    )
    if not running_rows:
        # Command was cancelled or acquired concurrently
        return command

    updated_command = running_rows[0]
    db.audit(
        conn,
        requested_by,
        "irrigation.started",
        farm_id=farm_id,
        target_type="irrigation_command",
        target_id=cmd_id,
        details={"device_id": device_id, "duration_seconds": duration_seconds},
    )
    _emit_kafka(
        kafka_client,
        "irrigation.started",
        {
            "command_id": cmd_id,
            "farm_id": farm_id,
            "device_id": device_id,
            "action": "start",
            "duration_seconds": duration_seconds,
            "status": "running",
        },
    )

    # 4. Check for cancellation during execution / simulation delay
    if simulate_time and step_delay > 0:
        time.sleep(step_delay)

    mid_check = db.query(
        "SELECT status FROM irrigation_commands WHERE id=%s",
        (cmd_id,),
        conn=conn,
    )
    if mid_check and mid_check[0]["status"] == "cancelled":
        logger.info("Command %s was cancelled during execution", cmd_id)
        return mid_check[0]

    # 5. Transition to completed
    completed_rows = db.query(
        "UPDATE irrigation_commands SET status='completed', completed_at=now(), updated_at=now() "
        "WHERE id=%s AND status='running' RETURNING *",
        (cmd_id,),
        conn=conn,
    )
    if not completed_rows:
        return updated_command

    final_command = completed_rows[0]
    db.audit(
        conn,
        requested_by,
        "irrigation.completed",
        farm_id=farm_id,
        target_type="irrigation_command",
        target_id=cmd_id,
        details={"device_id": device_id, "duration_seconds": duration_seconds},
    )
    _emit_kafka(
        kafka_client,
        "irrigation.completed",
        {
            "command_id": cmd_id,
            "farm_id": farm_id,
            "device_id": device_id,
            "action": "stop",
            "status": "completed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    return final_command


def execute_pending_commands(
    db_conn=None,
    max_commands: int = 10,
    simulate_time: bool = True,
    step_delay: float = 0.0,
    kafka_producer=None,
) -> list[dict[str, Any]]:
    """Query and execute queued irrigation commands.

    Args:
        db_conn: Optional open PostgreSQL connection (if None, connects automatically).
        max_commands: Maximum queued commands to process per cycle.
        simulate_time: Whether execution is simulated (instantaneous or with delay).
        step_delay: Optional simulated delay in seconds between start and completion.
        kafka_producer: Optional Kafka producer or ActuatorKafkaClient.

    Returns:
        List of processed command row dictionaries.
    """
    kafka_client = kafka_producer or ActuatorKafkaClient()

    def _process(conn):
        commands = db.query(
            "SELECT id, farm_id, device_id, duration_seconds, idempotency_key, requested_by, status, created_at "
            "FROM irrigation_commands WHERE status='queued' ORDER BY created_at ASC LIMIT %s",
            (max_commands,),
            conn=conn,
        )
        results = []
        for cmd in commands:
            res = execute_single_command(
                command=cmd,
                conn=conn,
                simulate_time=simulate_time,
                step_delay=step_delay,
                kafka_client=kafka_client,
            )
            results.append(res)
        return results

    if db_conn is not None:
        return _process(db_conn)

    with db.connect() as conn:
        return _process(conn)


class ActuatorExecutorWorker:
    """Standalone worker that executes queued irrigation commands in a polling loop."""

    def __init__(
        self,
        poll_interval: float = 1.0,
        max_commands: int = 10,
        simulate_time: bool = True,
        step_delay: float = 0.0,
        db_conn=None,
        kafka_bootstrap: str | None = None,
    ) -> None:
        self.poll_interval = poll_interval
        self.max_commands = max_commands
        self.simulate_time = simulate_time
        self.step_delay = step_delay
        self.db_conn = db_conn
        self.kafka_client = ActuatorKafkaClient(bootstrap_servers=kafka_bootstrap)
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def is_active(self) -> bool:
        """Return True if worker is currently running."""
        with self._lock:
            return self._running and (self._thread is not None and self._thread.is_alive())

    def run_once(self) -> list[dict[str, Any]]:
        """Run a single execution cycle."""
        return execute_pending_commands(
            db_conn=self.db_conn,
            max_commands=self.max_commands,
            simulate_time=self.simulate_time,
            step_delay=self.step_delay,
            kafka_producer=self.kafka_client,
        )

    def _loop(self) -> None:
        logger.info("ActuatorExecutorWorker loop started")
        while True:
            with self._lock:
                if not self._running:
                    break
            try:
                self.run_once()
            except Exception as exc:
                logger.exception("Error in ActuatorExecutorWorker loop: %s", exc)
            time.sleep(self.poll_interval)
        logger.info("ActuatorExecutorWorker loop stopped")

    def start(self) -> None:
        """Start worker in a background daemon thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            _ACTIVE_WORKERS.add(self)
            self._thread = threading.Thread(target=self._loop, daemon=True, name="ActuatorExecutorWorker")
            self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        """Stop worker and join background thread."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            _ACTIVE_WORKERS.discard(self)

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self.kafka_client.close()

    def __enter__(self) -> "ActuatorExecutorWorker":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
