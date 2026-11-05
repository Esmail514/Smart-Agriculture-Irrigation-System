import os
import sys

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

try:
    from src.common.config.kafka import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_WEATHER
    from src.common.config.postgres import POSTGRES_CONFIG
    from src.common.config.weather import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL
except ImportError:
    from common.config.kafka import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_WEATHER
    from common.config.postgres import POSTGRES_CONFIG
    from common.config.weather import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL

__all__ = [
    "API_KEY",
    "BASE_URL",
    "KAFKA_BOOTSTRAP_SERVERS",
    "POSTGRES_CONFIG",
    "SCHEMA_VERSION",
    "TOPIC_NAME",
    "WEATHER_POLL_INTERVAL",
]

TOPIC_NAME = KAFKA_TOPIC_WEATHER
SCHEMA_VERSION = 2

WEATHER_POLL_INTERVAL = int(os.getenv("WEATHER_POLL_INTERVAL", "300"))
if WEATHER_POLL_INTERVAL < 10:
    raise ValueError("WEATHER_POLL_INTERVAL must be at least 10 seconds")

# API key from environment or common config (never hardcoded in source)
API_KEY = os.getenv("OPENWEATHER_API_KEY", "") or OPENWEATHER_API_KEY
BASE_URL = os.getenv("OPENWEATHER_BASE_URL", "") or OPENWEATHER_BASE_URL or "https://api.openweathermap.org/data/2.5/weather"
