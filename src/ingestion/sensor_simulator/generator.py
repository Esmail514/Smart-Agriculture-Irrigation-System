import logging
import math
import random
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from src.common.sensor_contract import RANGES

try:
    from .config import (
        SIMULATE_IRRIGATION,
        ANOMALY_RATE,
        ANOMALY_TYPE_WEIGHTS,
        EVAPOTRANSPIRATION_BASE_RATE,
        EVAPOTRANSPIRATION_TEMP_FACTOR,
        IRRIGATION_RECOVERY_TARGET_RATIO,
        LOCAL_UTC_OFFSET_HOURS,
        REGION_PROFILES,
    )
except ImportError:
    from config import (
        SIMULATE_IRRIGATION,
        ANOMALY_RATE,
        ANOMALY_TYPE_WEIGHTS,
        EVAPOTRANSPIRATION_BASE_RATE,
        EVAPOTRANSPIRATION_TEMP_FACTOR,
        IRRIGATION_RECOVERY_TARGET_RATIO,
        LOCAL_UTC_OFFSET_HOURS,
        REGION_PROFILES,
    )

logger = logging.getLogger("generator")

SOIL_DRAINAGE_FACTORS = {
    "Sandy": 1.4,
    "Loam": 1.0,
    "Silty": 0.85,
    "Clay": 0.65,
}


class DataGenerator:

    def __init__(self, crop_config, schema_version: int = 1, simulate_irrigation: bool = None):
        self.crop_config = crop_config
        self.schema_version = schema_version
        self.simulate_irrigation = simulate_irrigation
        self.state = {}
        self.anomaly_rate = None

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

        air_low, air_high = RANGES["air_temperature"]
        soil_low, soil_high = RANGES["soil_temperature"]
        air_temp = round(min(max(air_temp, max(air_low, crop["air_temp"][0] - 3)), min(air_high, crop["air_temp"][1] + 5)), 2)
        soil_temp = round(min(max(soil_temp, max(soil_low, crop["soil_temp"][0] - 2)), min(soil_high, crop["soil_temp"][1] + 3)), 2)

        humidity_base = sum(crop["humidity"]) / 2 + region["humidity_baseline_offset"]
        humidity = humidity_base - diurnal * 1.3 + random.uniform(-2, 2)
        humidity = round(min(max(humidity, 5), 100), 2)

        previous = self.state.get(sensor_id)
        moisture_min, moisture_max = crop["optimal_moisture"]

        if previous is None or previous.get("soil_moisture") is None:
            soil_moisture = random.uniform(moisture_min, moisture_max)
        else:
            depletion = EVAPOTRANSPIRATION_BASE_RATE + max(0, air_temp - 25) * EVAPOTRANSPIRATION_TEMP_FACTOR
            drainage_factor = SOIL_DRAINAGE_FACTORS.get(sensor.get("soil_type"), 1.0)
            depletion *= drainage_factor
            previous_time = previous.get("timestamp")
            elapsed_minutes = (max(0, (utc_now - datetime.fromisoformat(previous_time.replace("Z", "+00:00"))).total_seconds()) / 60
                               if previous_time else 1.0)
            soil_moisture = previous["soil_moisture"] - depletion * elapsed_minutes + random.uniform(-0.1, 0.1)

            simulate_irr = self.simulate_irrigation if self.simulate_irrigation is not None else SIMULATE_IRRIGATION
            if simulate_irr and soil_moisture <= moisture_min:
                target = moisture_min + (moisture_max - moisture_min) * IRRIGATION_RECOVERY_TARGET_RATIO
                soil_moisture = target + random.uniform(-1.0, 1.0)
                logger.info(
                    "irrigation_triggered sensor_id=%s moisture_before=%.2f new=%.2f",
                    sensor_id, previous["soil_moisture"], soil_moisture
                )

        soil_moisture = round(min(max(soil_moisture, crop["critical_low"]), crop["critical_high"]), 2)

        reading = {
            "schema_version": self.schema_version,
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
            "reading_id": f"{sensor_id}-{uuid4().hex}",
            "timestamp": utc_now.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
        }

        if self.schema_version == 2:
            prev_v2 = previous or {}
            # Soil pH: (3.0, 10.0) - baseline ~6.8
            ph_base = prev_v2.get("soil_ph", random.uniform(6.5, 7.2))
            reading["soil_ph"] = round(min(10.0, max(3.0, ph_base + random.uniform(-0.02, 0.02))), 2)

            # Nitrogen: (0, 500) - baseline ~65
            n_base = prev_v2.get("nitrogen", random.uniform(50.0, 90.0))
            reading["nitrogen"] = round(min(500.0, max(0.0, n_base + random.uniform(-0.5, 0.5))), 1)

            # Phosphorus: (0, 300) - baseline ~35
            p_base = prev_v2.get("phosphorus", random.uniform(25.0, 45.0))
            reading["phosphorus"] = round(min(300.0, max(0.0, p_base + random.uniform(-0.3, 0.3))), 1)

            # Potassium: (0, 600) - baseline ~180
            k_base = prev_v2.get("potassium", random.uniform(140.0, 220.0))
            reading["potassium"] = round(min(600.0, max(0.0, k_base + random.uniform(-0.8, 0.8))), 1)

            # Electrical conductivity: (0, 30) - baseline ~1.5
            ec_base = prev_v2.get("electrical_conductivity", random.uniform(1.2, 2.0))
            reading["electrical_conductivity"] = round(min(30.0, max(0.0, ec_base + random.uniform(-0.02, 0.02))), 2)

            # Battery pct: (0, 100) - baseline ~95, gradual depletion
            batt_base = prev_v2.get("battery_pct", random.uniform(92.0, 99.0))
            reading["battery_pct"] = round(min(100.0, max(1.0, batt_base - random.uniform(0.0, 0.02))), 1)

        self.state[sensor_id] = dict(reading)
        return reading

    def _inject_anomaly(self, reading, sensor_id, previous=None):

        anomaly_type = random.choices(
            population=list(ANOMALY_TYPE_WEIGHTS.keys()),
            weights=list(ANOMALY_TYPE_WEIGHTS.values()),
            k=1,
        )[0]

        if anomaly_type == "null_field":
            fields_pool = ["soil_moisture", "soil_temperature", "air_temperature", "air_humidity"]
            if self.schema_version == 2:
                fields_pool.extend(["soil_ph", "nitrogen", "phosphorus", "potassium", "electrical_conductivity", "battery_pct"])
            field = random.choice(fields_pool)
            reading[field] = None
            logger.warning("anomaly_injected type=null_field sensor_id=%s field=%s", sensor_id, field)

        elif anomaly_type == "out_of_range":
            options = [
                ("soil_moisture", round(random.uniform(120, 250), 2)),
                ("air_temperature", round(random.uniform(80, 120), 2)),
                ("soil_moisture", round(random.uniform(-50, -5), 2)),
            ]
            if self.schema_version == 2:
                options.extend([
                    ("soil_ph", 12.5),
                    ("nitrogen", 750.0),
                    ("battery_pct", 150.0),
                ])
            field, bad_value = random.choice(options)
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
                if self.schema_version == 2:
                    for v2_field in ("soil_ph", "nitrogen", "phosphorus", "potassium", "electrical_conductivity", "battery_pct"):
                        if v2_field in previous:
                            reading[v2_field] = previous[v2_field]
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

        if random.random() < (ANOMALY_RATE if self.anomaly_rate is None else self.anomaly_rate):
            reading = self._inject_anomaly(reading, sensor_id, previous)

        return reading
