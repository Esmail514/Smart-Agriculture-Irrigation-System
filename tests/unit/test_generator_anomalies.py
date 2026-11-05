"""Tests for the sensor simulator anomaly injection (A8: frozen_reading).

The generator module imports its config as a top-level `config` module, so the
sensor simulator directory is added to sys.path (same trick the simulator uses).
"""

import os
import sys
import json
import pytest

SIM_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..", "src", "ingestion", "sensor_simulator"
    )
)
if SIM_DIR not in sys.path:
    sys.path.insert(0, SIM_DIR)

import generator as gen

FROZEN_ONLY = {
    "null_field": 0.0,
    "out_of_range": 0.0,
    "frozen_reading": 1.0,
    "dropout": 0.0,
}


def _crop_config():
    return {
        1: {
            "air_temp": (24, 38),
            "soil_temp": (22, 30),
            "humidity": (55, 85),
            "optimal_moisture": (30, 45),
            "critical_low": 20,
            "critical_high": 60,
        }
    }


def _sensor():
    return {
        "sensor_id": 9001,
        "sensor_code": "TM-01-1",
        "zone_id": 1,
        "zone_name": "Tomato-01",
        "farm_id": 100,
        "farm_name": "Farm A",
        "crop_id": 1,
        "crop_name": "Tomato",
        "city": "Al Hudaydah",
    }


def test_frozen_reading_freezes_previous_values(monkeypatch):
    generator = gen.DataGenerator(_crop_config())
    generator.state[9001] = {
        "soil_moisture": 33.3,
        "soil_temperature": 24.4,
        "air_temperature": 28.8,
        "air_humidity": 60.0,
    }
    monkeypatch.setattr(gen, "ANOMALY_RATE", 1.0)
    monkeypatch.setattr(gen, "ANOMALY_TYPE_WEIGHTS", FROZEN_ONLY)

    reading = generator.generate(_sensor())

    assert reading["soil_moisture"] == 33.3
    assert reading["soil_temperature"] == 24.4
    assert reading["air_temperature"] == 28.8
    assert reading["air_humidity"] == 60.0


def test_dropout_returns_none(monkeypatch):
    generator = gen.DataGenerator(_crop_config())
    generator.state[9001] = {"soil_moisture": 33.3}
    monkeypatch.setattr(gen, "ANOMALY_RATE", 1.0)
    monkeypatch.setattr(
        gen,
        "ANOMALY_TYPE_WEIGHTS",
        {"null_field": 0.0, "out_of_range": 0.0, "frozen_reading": 0.0, "dropout": 1.0},
    )
    assert generator.generate(_sensor()) is None


def test_null_field_nulls_exactly_one_metric(monkeypatch):
    generator = gen.DataGenerator(_crop_config())
    monkeypatch.setattr(gen, "ANOMALY_RATE", 1.0)
    monkeypatch.setattr(
        gen,
        "ANOMALY_TYPE_WEIGHTS",
        {"null_field": 1.0, "out_of_range": 0.0, "frozen_reading": 0.0, "dropout": 0.0},
    )
    reading = generator.generate(_sensor())
    fields = ["soil_moisture", "soil_temperature", "air_temperature", "air_humidity"]
    assert sum(1 for fld in fields if reading.get(fld) is None) == 1


def test_clean_state_kept_after_dropout(monkeypatch):
    generator = gen.DataGenerator(_crop_config())
    generator.state[9001] = {"soil_moisture": 33.3}
    monkeypatch.setattr(gen, "ANOMALY_RATE", 1.0)
    monkeypatch.setattr(
        gen,
        "ANOMALY_TYPE_WEIGHTS",
        {"null_field": 0.0, "out_of_range": 0.0, "frozen_reading": 0.0, "dropout": 1.0},
    )
    generator.generate(_sensor())
    # State must keep continuity (a clean reading) for the next cycle.
    assert generator.state[9001]["soil_moisture"] is not None


@pytest.mark.parametrize("bounds,diurnal", [((-10, -9), -30), ((69, 70), 30)])
def test_clean_generator_respects_admission_contract_at_temperature_extremes(monkeypatch, bounds, diurnal):
    from src.common.sensor_contract import sensor_json_error
    config = _crop_config()
    config[1]["air_temp"] = bounds
    config[1]["soil_temp"] = bounds
    generator = gen.DataGenerator(config)
    generator.anomaly_rate = 0
    monkeypatch.setattr(generator, "_diurnal_offset", lambda *args: diurnal)
    reading = generator.generate(_sensor())
    assert sensor_json_error(json.dumps(reading)) is None
