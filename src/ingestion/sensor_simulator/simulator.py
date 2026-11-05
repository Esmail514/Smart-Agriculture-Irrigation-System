import logging
import random
import signal
import time

try:
    from .config import (
        ANOMALY_RATE,
        PER_SENSOR_DELAY_MAX,
        PER_SENSOR_DELAY_MIN,
        SCHEMA_VERSION,
        SEND_INTERVAL,
        SIMULATE_IRRIGATION,
    )
    from .database import Database
    from .generator import DataGenerator
    from .producer import SensorProducer
    from .runtime import RuntimeSettings, runtime_settings
except ImportError:
    from config import (
        ANOMALY_RATE,
        PER_SENSOR_DELAY_MAX,
        PER_SENSOR_DELAY_MIN,
        SCHEMA_VERSION,
        SEND_INTERVAL,
        SIMULATE_IRRIGATION,
    )
    from database import Database
    from generator import DataGenerator
    from producer import SensorProducer
    from runtime import RuntimeSettings, runtime_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("simulator")

running = True


def _handle_shutdown(signum, frame):
    global running
    logger.info("shutdown_signal_received signal=%s", signum)
    running = False


def pause(seconds):
    deadline = time.monotonic() + seconds
    while running and time.monotonic() < deadline:
        time.sleep(min(0.5, max(0, deadline - time.monotonic())))


def main():
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)
    database = None
    producer = None
    try:
        database = Database()
        sensors = database.load_sensors()
        if not sensors:
            raise RuntimeError("No sensors registered in PostgreSQL")
        crop_config = database.load_crop_config()
        missing = {s["crop_id"] for s in sensors} - set(crop_config)
        if missing:
            raise RuntimeError(f"Missing configuration for crop IDs: {sorted(missing)}")
        defaults = RuntimeSettings(
            interval_seconds=SEND_INTERVAL,
            anomaly_rate=ANOMALY_RATE,
            enabled=True,
            simulate_irrigation=SIMULATE_IRRIGATION,
            schema_version=SCHEMA_VERSION,
        )
        settings = defaults
        try:
            generator = DataGenerator(crop_config, schema_version=settings.schema_version)
        except TypeError:
            generator = DataGenerator(crop_config)
        producer = SensorProducer()
        refresh_at = 0
        metadata_refresh_at = 0
        next_batch_at = 0
        logger.info("sensors_loaded count=%d", len(sensors))
        while running:
            if time.monotonic() >= refresh_at:
                refresh_at = time.monotonic() + 30
                try:
                    updated = runtime_settings(database.load_runtime_settings(), defaults)
                    if updated.interval_seconds != settings.interval_seconds:
                        next_batch_at = time.monotonic() + updated.interval_seconds
                    settings = updated
                    generator.anomaly_rate = settings.anomaly_rate
                    if hasattr(generator, "schema_version"):
                        generator.schema_version = settings.schema_version
                    if hasattr(generator, "simulate_irrigation"):
                        generator.simulate_irrigation = settings.simulate_irrigation
                except Exception:
                    logger.warning('simulator_configuration_refresh_unavailable; retaining last valid configuration')
            if time.monotonic() >= metadata_refresh_at:
                metadata_refresh_at = time.monotonic() + 30
                try:
                    new_sensors = database.load_sensors()
                    new_crops = database.load_crop_config()
                    if not new_sensors or {s['crop_id'] for s in new_sensors} - set(new_crops):
                        raise ValueError('Incomplete sensor configuration')
                    sensors, generator.crop_config = new_sensors, new_crops
                except Exception:
                    logger.warning('simulator_metadata_refresh_unavailable; retaining last valid metadata')
            if not settings.enabled or time.monotonic() < next_batch_at:
                pause(1 if not settings.enabled else min(1, next_batch_at - time.monotonic()))
                continue
            for sensor in sensors:
                if not running:
                    break
                if time.monotonic() >= refresh_at:
                    refresh_at = time.monotonic() + 30
                    try:
                        settings = runtime_settings(database.load_runtime_settings(), defaults)
                        generator.anomaly_rate = settings.anomaly_rate
                        if hasattr(generator, "schema_version"):
                            generator.schema_version = settings.schema_version
                        if hasattr(generator, "simulate_irrigation"):
                            generator.simulate_irrigation = settings.simulate_irrigation
                    except Exception:
                        logger.warning('simulator_configuration_refresh_unavailable; retaining last valid configuration')
                if not settings.enabled:
                    break

                reading = generator.generate(sensor)

                if not running:
                    break

                if reading is None:
                    continue

                producer.send(reading)
                logger.debug("reading_sent sensor_id=%s", sensor["sensor_id"])

                pause(random.uniform(PER_SENSOR_DELAY_MIN, PER_SENSOR_DELAY_MAX))

            producer.flush()
            next_batch_at = time.monotonic() + settings.interval_seconds

    finally:
        try:
            if producer is not None:
                producer.close()
        finally:
            if database is not None:
                database.close()
        logger.info("simulator_stopped")


if __name__ == "__main__":
    main()
