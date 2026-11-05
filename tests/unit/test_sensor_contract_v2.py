"""Unit tests for the Version 2 Sensor Wire Contract and Simulator Generator."""
import json
import math
import pytest
from src.common.sensor_contract import (
    sensor_json_error,
    sensor_ingress_error,
    RANGES_V2,
)
from src.ingestion.sensor_simulator.generator import DataGenerator


def valid_v2():
    return {
        "schema_version": 2,
        "sensor_id": 101,
        "zone_id": 12,
        "farm_id": 3,
        "crop_id": 1,
        "sensor_code": "SN-NPK-01",
        "zone_name": "Tomato-Alpha",
        "farm_name": "Green Valley",
        "crop_name": "Tomato",
        "reading_id": "v2-reading-unique-001",
        "soil_moisture": 42.5,
        "soil_temperature": 24.0,
        "air_temperature": 29.5,
        "air_humidity": 60.0,
        "soil_ph": 6.8,
        "nitrogen": 75.0,
        "phosphorus": 35.0,
        "potassium": 190.0,
        "electrical_conductivity": 1.65,
        "battery_pct": 98.5,
        "timestamp": "2026-06-01T12:00:00.000000Z",
    }


def test_accepts_valid_v2_contract():
    data = valid_v2()
    assert sensor_json_error(json.dumps(data)) is None


@pytest.mark.parametrize(
    "field,bad_value",
    [
        ("soil_ph", 2.9),
        ("soil_ph", 10.1),
        ("soil_ph", -1.0),
        ("soil_ph", 15.0),
        ("nitrogen", -1),
        ("nitrogen", 501),
        ("phosphorus", -1),
        ("phosphorus", 301),
        ("potassium", -1),
        ("potassium", 601),
        ("electrical_conductivity", -0.1),
        ("electrical_conductivity", 30.1),
        ("battery_pct", -0.5),
        ("battery_pct", 100.5),
    ],
)
def test_rejects_out_of_range_v2_fields(field, bad_value):
    data = valid_v2()
    data[field] = bad_value
    error = sensor_json_error(json.dumps(data))
    assert error == f"invalid_{field}"


@pytest.mark.parametrize(
    "field,invalid_type_value",
    [
        ("soil_ph", None),
        ("soil_ph", True),
        ("soil_ph", "6.5"),
        ("soil_ph", float("nan")),
        ("soil_ph", float("inf")),
        ("nitrogen", None),
        ("nitrogen", [50]),
        ("phosphorus", {"p": 20}),
        ("potassium", False),
        ("electrical_conductivity", float("-inf")),
        ("battery_pct", None),
    ],
)
def test_rejects_invalid_types_for_v2_fields(field, invalid_type_value):
    data = valid_v2()
    data[field] = invalid_type_value
    error = sensor_json_error(json.dumps(data))
    assert error == f"invalid_{field}"


@pytest.mark.parametrize(
    "missing_field",
    [
        "soil_ph",
        "nitrogen",
        "phosphorus",
        "potassium",
        "electrical_conductivity",
        "battery_pct",
    ],
)
def test_rejects_missing_v2_fields_in_schema_v2(missing_field):
    data = valid_v2()
    del data[missing_field]
    error = sensor_json_error(json.dumps(data))
    assert error == f"invalid_{missing_field}"


def test_rejects_unexpected_fields_in_v2():
    data = valid_v2()
    data["arbitrary_injected_metric"] = 123
    assert sensor_json_error(json.dumps(data)) == "unexpected_fields"


def test_v1_rejects_payload_containing_v2_fields():
    data = valid_v2()
    data["schema_version"] = 1
    # Version 1 schema does not permit v2 metrics -> must return unexpected_fields
    assert sensor_json_error(json.dumps(data)) == "unexpected_fields"


def test_unsupported_schema_version_rejected():
    data = valid_v2()
    data["schema_version"] = 3
    assert sensor_json_error(json.dumps(data)) == "unsupported_schema_version"
    data["schema_version"] = "2"
    assert sensor_json_error(json.dumps(data)) == "unsupported_schema_version"


def test_data_generator_generates_valid_v2_readings():
    crop_config = {
        1: {
            "air_temp": (20, 35),
            "soil_temp": (18, 28),
            "humidity": (40, 70),
            "optimal_moisture": (30, 45),
            "critical_low": 20,
            "critical_high": 60,
        }
    }
    generator = DataGenerator(crop_config=crop_config, schema_version=2)
    sensor = {
        "sensor_id": 55,
        "sensor_code": "SN-55",
        "zone_id": 2,
        "zone_name": "Z-02",
        "farm_id": 1,
        "farm_name": "Test Farm",
        "crop_id": 1,
        "crop_name": "Tomato",
        "city": "DEFAULT",
    }

    # Generate 10 consecutive clean readings and verify against sensor wire contract
    generator.anomaly_rate = 0.0
    for _ in range(10):
        reading = generator.generate(sensor)
        assert reading is not None
        assert reading["schema_version"] == 2
        for v2_field, (low, high) in RANGES_V2.items():
            assert v2_field in reading
            val = reading[v2_field]
            assert low <= val <= high
            assert math.isfinite(val)

        # Wire contract validation must accept the generated JSON
        raw = json.dumps(reading)
        assert sensor_json_error(raw) is None
