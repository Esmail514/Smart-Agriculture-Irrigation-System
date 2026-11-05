"""Unit tests for simulator enhancements:
- Soil-Type Drainage Physics
- PostgreSQL Reconnection Resilience
- Dynamic Simulator Settings
- Kafka Producer Compression
"""
from datetime import datetime, timezone
import os
import psycopg2
from psycopg2 import pool as pg_pool
from unittest.mock import MagicMock, patch
import pytest

from src.ingestion.sensor_simulator import config
from src.ingestion.sensor_simulator.database import Database
from src.ingestion.sensor_simulator.generator import DataGenerator, SOIL_DRAINAGE_FACTORS
from src.ingestion.sensor_simulator.producer import SensorProducer
from src.ingestion.sensor_simulator.runtime import RuntimeSettings, runtime_settings


# ---------------------------------------------------------------------------
# 1. Soil-Type Drainage Physics Tests
# ---------------------------------------------------------------------------

def test_soil_drainage_factors_values():
    assert SOIL_DRAINAGE_FACTORS == {
        "Sandy": 1.4,
        "Loam": 1.0,
        "Silty": 0.85,
        "Clay": 0.65,
    }


def test_soil_drainage_physics_depletion():
    crop_config = {
        1: {
            "air_temp": (25.0, 25.0),
            "soil_temp": (20.0, 20.0),
            "humidity": (50.0, 50.0),
            "optimal_moisture": (20.0, 80.0),
            "critical_low": 5.0,
            "critical_high": 95.0,
        }
    }

    depletions = {}
    for soil in ["Sandy", "Loam", "Silty", "Clay", None]:
        gen = DataGenerator(crop_config)
        sensor = {
            "sensor_id": 1,
            "sensor_code": "SN-01",
            "zone_id": 10,
            "zone_name": "Z-10",
            "farm_id": 1,
            "farm_name": "F-01",
            "crop_id": 1,
            "crop_name": "Crop A",
            "city": "DEFAULT",
        }
        if soil is not None:
            sensor["soil_type"] = soil

        # Establish fixed previous state
        gen.state[1] = {
            "soil_moisture": 50.0,
            "timestamp": "2026-11-04T12:00:00.000000Z",
        }

        # Fix random jitter to 0 for deterministic comparison
        with patch("random.uniform", return_value=0.0):
            reading = gen._generate_clean_reading(
                sensor,
                utc_now=datetime.fromisoformat("2026-11-04T12:10:00+00:00"),
            )
        depletions[soil] = 50.0 - reading["soil_moisture"]

    # Sandy (1.4) > Loam (1.0) > Silty (0.85) > Clay (0.65)
    assert depletions["Sandy"] > depletions["Loam"]
    assert depletions["Loam"] > depletions["Silty"]
    assert depletions["Silty"] > depletions["Clay"]
    # Missing soil_type defaults to 1.0 (Loam factor)
    assert pytest.approx(depletions[None], rel=1e-3) == depletions["Loam"]


def test_load_sensors_includes_soil_type():
    with patch.object(Database, "_connect_with_retry") as mock_connect:
        mock_pool = MagicMock()
        mock_connect.return_value = mock_pool
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_pool.getconn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = Database()
        mock_cursor.description = [
            ("sensor_id",), ("sensor_code",), ("zone_id",), ("zone_name",),
            ("soil_type",), ("crop_id",), ("crop_name",), ("farm_id",),
            ("farm_name",), ("city",),
        ]
        mock_cursor.fetchall.return_value = [
            (1, "SN-01", 10, "Zone A", "Clay", 1, "Wheat", 100, "Farm 1", "Sanaa"),
        ]

        sensors = db.load_sensors()
        assert len(sensors) == 1
        assert sensors[0]["soil_type"] == "Clay"

        # Check SQL query text contains z.soil_type
        executed_query = mock_cursor.execute.call_args[0][0]
        assert "z.soil_type" in executed_query


# ---------------------------------------------------------------------------
# 2. PostgreSQL Reconnection Resilience Tests
# ---------------------------------------------------------------------------

def test_database_cursor_reconnects_when_pool_closed():
    with patch.object(Database, "_connect_with_retry") as mock_connect:
        pool1 = MagicMock()
        pool1.closed = True

        pool2 = MagicMock()
        pool2.closed = False
        conn2 = MagicMock()
        conn2.closed = False
        cursor2 = MagicMock()
        conn2.cursor.return_value = cursor2
        pool2.getconn.return_value = conn2

        mock_connect.side_effect = [pool1, pool2]

        db = Database()
        assert db.pool is pool1

        # Using _cursor should detect pool1.closed and reconnect to pool2
        with db._cursor() as cur:
            assert cur is cursor2
        assert db.pool is pool2
        assert mock_connect.call_count == 2


def test_database_cursor_reconnects_on_operational_error():
    with patch.object(Database, "_connect_with_retry") as mock_connect:
        pool1 = MagicMock()
        pool1.closed = False
        pool1.getconn.side_effect = psycopg2.OperationalError("server closed connection unexpectedly")

        pool2 = MagicMock()
        pool2.closed = False
        conn2 = MagicMock()
        conn2.closed = False
        cursor2 = MagicMock()
        conn2.cursor.return_value = cursor2
        pool2.getconn.return_value = conn2

        mock_connect.side_effect = [pool1, pool2]

        db = Database()
        with db._cursor() as cur:
            assert cur is cursor2
        assert db.pool is pool2
        assert mock_connect.call_count == 2


def test_database_cursor_reconnects_on_pool_error():
    with patch.object(Database, "_connect_with_retry") as mock_connect:
        pool1 = MagicMock()
        pool1.closed = False
        pool1.getconn.side_effect = pg_pool.PoolError("connection pool exhausted")

        pool2 = MagicMock()
        pool2.closed = False
        conn2 = MagicMock()
        conn2.closed = False
        cursor2 = MagicMock()
        conn2.cursor.return_value = cursor2
        pool2.getconn.return_value = conn2

        mock_connect.side_effect = [pool1, pool2]

        db = Database()
        with db._cursor() as cur:
            assert cur is cursor2
        assert db.pool is pool2
        assert mock_connect.call_count == 2


# ---------------------------------------------------------------------------
# 3. Dynamic Simulator Settings Tests
# ---------------------------------------------------------------------------

def test_config_schema_version_default_and_env(monkeypatch):
    assert config.SCHEMA_VERSION in (1, 2)
    monkeypatch.setenv("SIMULATOR_SCHEMA_VERSION", "2")
    # Verify parsing behavior
    assert int(os.getenv("SIMULATOR_SCHEMA_VERSION", "1")) == 2


def test_runtime_settings_simulate_irrigation_and_schema_version():
    defaults = RuntimeSettings(
        interval_seconds=5.0,
        anomaly_rate=0.03,
        enabled=True,
        simulate_irrigation=False,
        schema_version=1,
    )
    # Default inheritance
    res = runtime_settings({}, defaults)
    assert res.simulate_irrigation is False
    assert res.schema_version == 1

    # Explicit values
    res = runtime_settings(
        {
            "simulator.simulate_irrigation": True,
            "simulator.schema_version": 2,
        },
        defaults,
    )
    assert res.simulate_irrigation is True
    assert res.schema_version == 2


def test_runtime_settings_validation_errors():
    defaults = RuntimeSettings(5.0, 0.03)

    # simulate_irrigation must be boolean
    with pytest.raises(ValueError, match="simulate_irrigation"):
        runtime_settings({"simulator.simulate_irrigation": "true"}, defaults)

    with pytest.raises(ValueError, match="simulate_irrigation"):
        runtime_settings({"simulator.simulate_irrigation": 1}, defaults)

    # schema_version must be 1 or 2
    with pytest.raises(ValueError, match="schema_version"):
        runtime_settings({"simulator.schema_version": "1"}, defaults)

    with pytest.raises(ValueError, match="schema_version"):
        runtime_settings({"simulator.schema_version": 0}, defaults)

    with pytest.raises(ValueError, match="schema_version"):
        runtime_settings({"simulator.schema_version": 3}, defaults)


# ---------------------------------------------------------------------------
# 4. Kafka Producer Compression Tests
# ---------------------------------------------------------------------------

def test_sensor_producer_uses_gzip_compression():
    with patch("src.ingestion.sensor_simulator.producer.KafkaProducer") as mock_kafka:
        SensorProducer()
        assert mock_kafka.called
        kwargs = mock_kafka.call_args[1]
        assert kwargs.get("compression_type") == "gzip"
