import logging
import math
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from .config import (
        API_KEY,
        BASE_URL,
        SCHEMA_VERSION,
        WEATHER_POLL_INTERVAL,
    )
    from .database import Database
    from .producer import WeatherProducer
except ImportError:
    from config import (
        API_KEY,
        BASE_URL,
        SCHEMA_VERSION,
        WEATHER_POLL_INTERVAL,
    )
    from database import Database
    from producer import WeatherProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("weather_collector")

running = True


def _handle_shutdown(signum, frame):
    global running
    logger.info("shutdown_signal_received signal=%s", signum)
    running = False


def pause(seconds):
    deadline = time.monotonic() + seconds
    while running and time.monotonic() < deadline:
        time.sleep(min(0.5, max(0.0, deadline - time.monotonic())))


class WeatherCollector:

    def __init__(self, db=None, producer=None, api_key=None, base_url=None):
        self.db = db or Database()
        self.producer = producer or WeatherProducer()
        self.api_key = api_key or API_KEY
        self.base_url = base_url or BASE_URL
        self.session = self._init_session()
        self.farms = []

    def _init_session(self):
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        return session

    def reload_farms(self):
        self.farms = self.db.load_farms()
        return self.farms

    def fetch_farm_weather(self, farm):
        lat = float(farm["latitude"])
        lon = float(farm["longitude"])
        farm_id = farm["farm_id"]
        farm_name = str(farm["farm_name"])

        response = self.session.get(
            self.base_url,
            params={
                "lat": lat,
                "lon": lon,
                "appid": self.api_key,
                "units": "metric",
            },
            timeout=(5, 15),
        )
        if response.status_code != 200:
            raise RuntimeError(f"Weather API status {response.status_code} for farm {farm_id}")

        data = response.json()
        now = datetime.now(timezone.utc)
        timestamp = data.get("dt", int(now.timestamp()))
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)) or not math.isfinite(timestamp):
            timestamp = int(now.timestamp())

        main_data = data.get("main") or {}
        wind_data = data.get("wind") or {}
        weather_list = data.get("weather") or []
        condition = weather_list[0] if weather_list else {}

        record = {
            "schema_version": SCHEMA_VERSION,
            "weather_reading_id": f"{farm_id}-{int(timestamp)}",
            "farm_id": farm_id,
            "farm_name": farm_name,
            "latitude": lat,
            "longitude": lon,
            "temperature": float(main_data.get("temp", 0.0)),
            "feels_like": float(main_data.get("feels_like", main_data.get("temp", 0.0))),
            "humidity": float(main_data.get("humidity", 0.0)),
            "pressure": float(main_data.get("pressure", 1013.0)),
            "wind_speed": float(wind_data.get("speed", 0.0)),
            "weather_condition": str(condition.get("main", "Unknown")),
            "weather_description": str(condition.get("description", "")),
            "cloud_cover": float((data.get("clouds") or {}).get("all", 0.0)),
            "rain_1h_mm": float((data.get("rain") or {}).get("1h", 0.0)),
            "timestamp": now.isoformat(),
        }
        return record

    def poll_and_publish(self):
        if not self.farms:
            self.reload_farms()

        successful = 0
        for farm in self.farms:
            if not running:
                break
            try:
                record = self.fetch_farm_weather(farm)
                self.producer.send(record)
                successful += 1
                logger.info(
                    "weather_collected farm_id=%s farm_name='%s' temp=%.1fC hum=%.1f%% cond='%s'",
                    farm["farm_id"],
                    farm["farm_name"],
                    record["temperature"],
                    record["humidity"],
                    record["weather_condition"],
                )
            except (requests.RequestException, RuntimeError, ValueError) as exc:
                logger.error(
                    "weather_fetch_failed farm_id=%s error=%s",
                    farm["farm_id"],
                    exc,
                )

        if successful > 0:
            self.producer.flush()
            logger.info("weather_poll_cycle_complete published_count=%d", successful)
        return successful

    def close(self):
        try:
            self.session.close()
        except requests.RequestException as err:
            logger.debug("session_close_error error=%s", err)
        self.producer.close()
        self.db.close()


def run_collector(poll_interval=WEATHER_POLL_INTERVAL):
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    logger.info("weather_collector_starting poll_interval=%ds", poll_interval)
    collector = WeatherCollector()
    try:
        collector.reload_farms()
        while running:
            collector.poll_and_publish()
            pause(poll_interval)
    finally:
        logger.info("weather_collector_stopping")
        collector.close()
        logger.info("weather_collector_stopped")


if __name__ == "__main__":
    interval = int(sys.argv[1]) if len(sys.argv) > 1 else WEATHER_POLL_INTERVAL
    run_collector(poll_interval=interval)
