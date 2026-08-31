import logging
import math
import random
from datetime import datetime, timedelta, timezone

from config import (
    ANOMALY_RATE,
    ANOMALY_TYPE_WEIGHTS,
    EVAPOTRANSPIRATION_BASE_RATE,
    EVAPOTRANSPIRATION_TEMP_FACTOR,
    IRRIGATION_RECOVERY_TARGET_RATIO,
    LOCAL_UTC_OFFSET_HOURS,
    REGION_PROFILES,
)

logger = logging.getLogger("generator")


class DataGenerator:

    def __init__(self, crop_config):
        self.crop_config = crop_config
        self.state = {}

    def _diurnal_offset(self, local_hour_fraction, amplitude):
        radians = 2 * math.pi * (local_hour_fraction - 15) / 24
        return amplitude * math.cos(radians)

    def _local_hour_fraction(self, utc_now):
        local_time = utc_now + timedelta(hours=LOCAL_UTC_OFFSET_HOURS)
        return local_time.hour + local_time.minute / 60.0

    def _region_profile(self, city):
        return REGION_PROFILES.get(city, REGION_PROFILES["DEFAULT"])

    def _generate_clean_reading(self, sensor, utc_now):

        crop = self.crop_config[sensor["crop_id"]]
        region = self._region_profile(sensor.get("city"))
        sensor_id = sensor["sensor_id"]

        hour_fraction = self._local_hour_fraction(utc_now)
        diurnal = self._diurnal_offset(hour_fraction, region["diurnal_amplitude"])

        air_temp_base = sum(crop["air_temp"]) / 2 + region["temp_baseline_offset"]
        soil_temp_base = sum(crop["soil_temp"]) / 2 + region["temp_baseline_offset"] * 0.6

        air_temp = air_temp_base + diurnal + random.uniform(-0.6, 0.6)
        soil_temp = soil_temp_base + diurnal * 0.4 + random.uniform(-0.3, 0.3)

        air_temp = round(min(max(air_temp, crop["air_temp"][0] - 3), crop["air_temp"][1] + 5), 2)
        soil_temp = round(min(max(soil_temp, crop["soil_temp"][0] - 2), crop["soil_temp"][1] + 3), 2)

        humidity_base = sum(crop["humidity"]) / 2 + region["humidity_baseline_offset"]
        humidity = humidity_base - diurnal * 1.3 + random.uniform(-2, 2)
        humidity = round(min(max(humidity, 5), 100), 2)

        previous = self.state.get(sensor_id)
        moisture_min, moisture_max = crop["optimal_moisture"]

        if previous is None or previous.get("soil_moisture") is None:
            soil_moisture = random.uniform(moisture_min, moisture_max)
        else:
            depletion = EVAPOTRANSPIRATION_BASE_RATE + max(0, air_temp - 25) * EVAPOTRANSPIRATION_TEMP_FACTOR
            soil_moisture = previous["soil_moisture"] - depletion + random.uniform(-0.1, 0.1)

            if soil_moisture <= moisture_min:
                target = moisture_max * IRRIGATION_RECOVERY_TARGET_RATIO
                soil_moisture = target + random.uniform(-1.0, 1.0)
                logger.info(
                    "irrigation_triggered sensor_id=%s moisture_before=%.2f new=%.2f",
                    sensor_id, previous["soil_moisture"], soil_moisture
                )

        soil_moisture = round(min(max(soil_moisture, crop["critical_low"]), crop["critical_high"]), 2)

        reading = {
            "sensor_id": sensor["sensor_id"],
            "sensor_code": sensor["sensor_code"],
            "zone_id": sensor["zone_id"],
            "zone_name": sensor["zone_name"],
            "farm_id": sensor["farm_id"],
            "farm_name": sensor["farm_name"],
            "crop_id": sensor["crop_id"],
            "crop_name": sensor["crop_name"],
            "soil_moisture": soil_moisture,
            "soil_temperature": soil_temp,
            "air_temperature": air_temp,
            "air_humidity": humidity,
            "reading_id": f"{sensor_id}-{int(utc_now.timestamp() * 1000)}",
            "timestamp": utc_now.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
        }

        self.state[sensor_id] = dict(reading)
        return reading

    def _inject_anomaly(self, reading, sensor_id, previous=None):

        anomaly_type = random.choices(
            population=list(ANOMALY_TYPE_WEIGHTS.keys()),
            weights=list(ANOMALY_TYPE_WEIGHTS.values()),
            k=1,
        )[0]

        if anomaly_type == "null_field":
            field = random.choice(
                ["soil_moisture", "soil_temperature", "air_temperature", "air_humidity"]
            )
            reading[field] = None
            logger.warning("anomaly_injected type=null_field sensor_id=%s field=%s", sensor_id, field)

        elif anomaly_type == "out_of_range":
            field, bad_value = random.choice([
                ("soil_moisture", round(random.uniform(120, 250), 2)),
                ("air_temperature", round(random.uniform(80, 120), 2)),
                ("soil_moisture", round(random.uniform(-50, -5), 2)),
            ])
            reading[field] = bad_value
            logger.warning(
                "anomaly_injected type=out_of_range sensor_id=%s field=%s value=%s",
                sensor_id, field, bad_value
            )

        elif anomaly_type == "frozen_reading":
            previous = previous or self.state.get(sensor_id)
            if previous:
                reading["soil_moisture"] = previous["soil_moisture"]
                reading["soil_temperature"] = previous["soil_temperature"]
                reading["air_temperature"] = previous["air_temperature"]
                reading["air_humidity"] = previous["air_humidity"]
                logger.warning("anomaly_injected type=frozen_reading sensor_id=%s", sensor_id)

        elif anomaly_type == "dropout":
            logger.warning("anomaly_injected type=dropout sensor_id=%s (no message sent)", sensor_id)
            return None

        return reading

    def generate(self, sensor):
        utc_now = datetime.now(timezone.utc)
        sensor_id = sensor["sensor_id"]
        previous = self.state.get(sensor_id)
        reading = self._generate_clean_reading(sensor, utc_now)

        if random.random() < ANOMALY_RATE:
            reading = self._inject_anomaly(reading, sensor_id, previous)

        return reading