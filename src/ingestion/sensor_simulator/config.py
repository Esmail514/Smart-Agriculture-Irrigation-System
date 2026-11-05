import os
import sys

# Make the project root importable when running the simulator standalone
# (e.g. `python simulator.py` with CWD = src/ingestion/sensor_simulator/).
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.common.config.kafka import KAFKA_BOOTSTRAP_SERVERS, SENSOR_TOPIC
from src.common.config.postgres import POSTGRES_CONFIG


TOPIC_NAME = SENSOR_TOPIC

SEND_INTERVAL = int(os.getenv("SEND_INTERVAL", 5))
if SEND_INTERVAL < 1:
    raise ValueError("SEND_INTERVAL must be positive")
SIMULATE_IRRIGATION = os.getenv("SIMULATE_IRRIGATION", "false").lower() == "true"
SCHEMA_VERSION = int(os.getenv("SIMULATOR_SCHEMA_VERSION", "1"))

PER_SENSOR_DELAY_MIN = 0.1
PER_SENSOR_DELAY_MAX = 0.3

LOCAL_UTC_OFFSET_HOURS = 3

REGION_PROFILES = {
    "Al Hudaydah": {
        "temp_baseline_offset": 2.0,
        "diurnal_amplitude": 5.0,
        "humidity_baseline_offset": 8.0,
    },
    "Al Jawf": {
        "temp_baseline_offset": 1.0,
        "diurnal_amplitude": 11.0,
        "humidity_baseline_offset": -12.0,
    },
    "DEFAULT": {
        "temp_baseline_offset": 0.0,
        "diurnal_amplitude": 6.0,
        "humidity_baseline_offset": 0.0,
    },
}

IRRIGATION_RECOVERY_TARGET_RATIO = 0.85
EVAPOTRANSPIRATION_BASE_RATE = 0.15
EVAPOTRANSPIRATION_TEMP_FACTOR = 0.02

ANOMALY_RATE = float(os.getenv("ANOMALY_RATE", 0.03))
if not 0 <= ANOMALY_RATE <= 1:
    raise ValueError("ANOMALY_RATE must be between 0 and 1")

ANOMALY_TYPE_WEIGHTS = {
    "null_field": 0.35,
    "out_of_range": 0.30,
    "frozen_reading": 0.20,
    "dropout": 0.15,
}
