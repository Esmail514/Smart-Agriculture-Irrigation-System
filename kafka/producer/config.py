import os

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5433)),
    "database": os.getenv("POSTGRES_DB", "smart_agriculture"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "root"),
}

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9094"
)

TOPIC_NAME = "sensor-data"

SEND_INTERVAL = int(os.getenv("SEND_INTERVAL", 5))

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

ANOMALY_TYPE_WEIGHTS = {
    "null_field": 0.35,
    "out_of_range": 0.30,
    "frozen_reading": 0.20,
    "dropout": 0.15,
}