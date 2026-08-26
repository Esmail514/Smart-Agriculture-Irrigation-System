import logging
import random
import signal
import sys
import time

from config import PER_SENSOR_DELAY_MAX, PER_SENSOR_DELAY_MIN, SEND_INTERVAL
from database import Database
from generator import DataGenerator
from producer import SensorProducer

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


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


def main():
    database = Database()
    sensors = database.load_sensors()

    if not sensors:
        logger.error("no_sensors_loaded - تحقق من جدول sensors بقاعدة البيانات")
        sys.exit(1)


    crop_config = database.load_crop_config()
    generator = DataGenerator(crop_config)
    producer = SensorProducer()

    logger.info("sensors_loaded count=%d", len(sensors))

    try:
        while running:
            for sensor in sensors:
                if not running:
                    break

                reading = generator.generate(sensor)

                if reading is None:
                    continue

                producer.send(reading)
                logger.debug("reading_sent sensor_id=%s", sensor["sensor_id"])

                time.sleep(random.uniform(PER_SENSOR_DELAY_MIN, PER_SENSOR_DELAY_MAX))

            producer.flush()
            time.sleep(SEND_INTERVAL)

    finally:
        logger.info("closing_producer")
        producer.close()
        logger.info("closing_database_pool")
        database.close()
        logger.info("simulator_stopped")


if __name__ == "__main__":
    main()