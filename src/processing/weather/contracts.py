"""Versioned weather observation contract (metric units, UTC timestamps)."""
import json
import math
from datetime import datetime, timezone

CONTRACT_VERSION = 2
NUMERIC_RANGES = {"temperature": (-20, 55), "humidity": (0, 100), "pressure": (850, 1100),
                  "wind_speed": (0, 60), "cloud_cover": (0, 100), "rain_1h_mm": (0, 500)}
MAX_FUTURE_SECONDS = 600
MAX_OBSERVATION_AGE_SECONDS = 21600


def weather_record_error(record):
    """Shared typed admission check, including historical Silver revalidation.

    Compare observation time with its recorded fetch time, rather than wall
    time: replaying a valid historical observation must remain valid.
    """
    if hasattr(record, "asDict"):
        record = record.asDict()
    if not isinstance(record, dict):
        return "expected_weather_object"
    if type(record.get("schema_version")) is not int or record["schema_version"] != CONTRACT_VERSION:
        return "unsupported_weather_schema_version"
    farm_id = record.get("farm_id")
    if type(farm_id) is not int or not 0 < farm_id <= 2**63 - 1:
        return "invalid_weather_farm_id"
    for name in ["weather_reading_id", "farm_name", "weather_condition"]:
        if not isinstance(record.get(name), str) or not record[name].strip():
            return f"invalid_{name}"
    if not isinstance(record.get("weather_description"), str):
        return "invalid_weather_description"
    for name, (low, high) in dict(NUMERIC_RANGES, latitude=(-90, 90), longitude=(-180, 180), feels_like=(-100, 150)).items():
        value = record.get(name)
        if type(value) not in (int, float) or not low <= value <= high or not math.isfinite(value):
            return f"invalid_weather_{name}"
    observation, fetched = record.get("observation_time_utc"), record.get("fetched_at")
    if not isinstance(observation, datetime) or not isinstance(fetched, datetime):
        return "invalid_weather_timestamp"
    # Spark TimestampType returns naive datetime values in the configured UTC
    # session. Also accept timezone-aware records at the ingestion boundary.
    observation = observation.replace(tzinfo=timezone.utc) if observation.tzinfo is None else observation.astimezone(timezone.utc)
    fetched = fetched.replace(tzinfo=timezone.utc) if fetched.tzinfo is None else fetched.astimezone(timezone.utc)
    delta = (observation - fetched).total_seconds()
    if delta > MAX_FUTURE_SECONDS or delta < -MAX_OBSERVATION_AGE_SECONDS:
        return "weather_observation_stale_or_future"
    if observation.timestamp() <= 0:
        return "invalid_weather_timestamp"
    if record["weather_reading_id"] != f"{farm_id}-{int(observation.timestamp())}":
        return "invalid_weather_reading_id"
    return None


def flatten_record(farm, data, fetched_at):
    if not isinstance(data, dict) or str(data.get("cod", 200)) != "200":
        raise ValueError("Weather API did not return a successful observation")
    timestamp = data.get("dt")
    if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)) or not math.isfinite(timestamp) or timestamp <= 0:
        raise ValueError("Weather observation requires a valid UTC epoch timestamp")
    try:
        observation = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except (ValueError, OverflowError, OSError) as error:
        raise ValueError("Invalid weather epoch timestamp") from error
    if fetched_at.tzinfo is None:
        raise ValueError("fetched_at must have a timezone")
    if (observation - fetched_at).total_seconds() > MAX_FUTURE_SECONDS or (fetched_at - observation).total_seconds() > MAX_OBSERVATION_AGE_SECONDS:
        raise ValueError("Weather observation is stale or in the future")
    lat, lon = float(farm["latitude"]), float(farm["longitude"])
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise ValueError("Invalid farm coordinates")
    farm_id = farm["farm_id"]
    if type(farm_id) is not int or not 0 < farm_id <= 2**63 - 1:
        raise ValueError("Invalid farm identifier")
    main, wind = data.get("main") or {}, data.get("wind") or {}
    conditions = data.get("weather") or []
    condition = conditions[0] if conditions else {}
    record = {
        "weather_reading_id": f"{farm_id}-{int(timestamp)}", "farm_id": farm_id,
        "farm_name": str(farm["farm_name"]), "latitude": lat, "longitude": lon,
        "fetched_at": fetched_at.astimezone(timezone.utc).replace(tzinfo=None),
        "observation_time_utc": observation.replace(tzinfo=None),
        "weather_condition": str(condition.get("main", "Unknown")),
        "weather_description": str(condition.get("description", "")),
        "temperature": main.get("temp"), "feels_like": main.get("feels_like", main.get("temp")),
        "humidity": main.get("humidity"), "pressure": main.get("pressure"),
        "wind_speed": wind.get("speed"), "cloud_cover": (data.get("clouds") or {}).get("all", 0),
        "rain_1h_mm": (data.get("rain") or {}).get("1h", 0.0),
        "raw_response": json.dumps(data, ensure_ascii=False, allow_nan=False),
        "schema_version": CONTRACT_VERSION,
    }
    for name, (low, high) in NUMERIC_RANGES.items():
        value = record[name]
        if isinstance(value, bool) or value is None:
            raise ValueError(f"Missing or invalid weather field: {name}")
        value = float(value)
        if not math.isfinite(value) or not low <= value <= high:
            raise ValueError(f"Weather field outside contract: {name}")
        record[name] = value
    if isinstance(record["feels_like"], bool):
        raise ValueError("Invalid feels_like")
    record["feels_like"] = float(record["feels_like"])
    if not math.isfinite(record["feels_like"]) or not -100 <= record["feels_like"] <= 150:
        raise ValueError("Invalid feels_like")
    error = weather_record_error(record)
    if error is not None:
        raise ValueError(error)
    return record


def bronze_schema():
    from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType, IntegerType
    fields = {"weather_reading_id": StringType(), "farm_id": LongType(), "farm_name": StringType(),
              "latitude": DoubleType(), "longitude": DoubleType(), "fetched_at": TimestampType(),
              "observation_time_utc": TimestampType(), "weather_condition": StringType(),
              "weather_description": StringType(), "temperature": DoubleType(), "feels_like": DoubleType(),
              "humidity": DoubleType(), "pressure": DoubleType(), "wind_speed": DoubleType(),
              "cloud_cover": DoubleType(), "rain_1h_mm": DoubleType(), "raw_response": StringType(),
              "schema_version": IntegerType()}
    return StructType([StructField(name, kind, False) for name, kind in fields.items()])


def silver_schema():
    from pyspark.sql.types import StructType, StructField, DateType
    fields = [field for field in bronze_schema().fields if field.name != "raw_response"]
    return StructType(fields + [StructField("observation_date", DateType(), False)])


def daily_schema():
    from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, DoubleType, TimestampType
    fields = {"farm_id": LongType(), "farm_name": StringType(), "weather_date": DateType(),
              "avg_temperature": DoubleType(), "avg_humidity": DoubleType(), "total_rain": DoubleType(),
              "avg_wind_speed": DoubleType(), "avg_clouds": DoubleType(), "weather_condition": StringType(),
              "last_update": TimestampType()}
    return StructType([StructField(name, kind, False) for name, kind in fields.items()])


def alerts_schema():
    from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType
    fields = {"farm_id": LongType(), "farm_name": StringType(), "observation_time_utc": TimestampType(),
              "rain_1h_mm": DoubleType(), "weather_condition": StringType(), "alert_message": StringType()}
    return StructType([StructField(name, kind, False) for name, kind in fields.items()])


def gold_alert_schema():
    """Current zone decisions; expected_rain is null until a forecast source exists."""
    from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, IntegerType, TimestampType
    fields = {"farm_id": LongType(), "farm_name": StringType(), "zone_id": LongType(),
              "zone_name": StringType(), "crop_id": LongType(), "crop_name": StringType(),
              "soil_moisture": DoubleType(), "expected_rain": DoubleType(), "observed_rain_mm": DoubleType(),
              "weather_available": IntegerType(), "recommended_water_mm": DoubleType(),
              "irrigation_decision": StringType(), "weather_condition": StringType(),
              "event_time": TimestampType(), "processing_time": TimestampType()}
    nullable = {"expected_rain", "observed_rain_mm", "recommended_water_mm"}
    return StructType([StructField(name, kind, name in nullable) for name, kind in fields.items()])


def enforce_columns(frame, schema):
    """Project a deterministic field set and physical types before each write."""
    from pyspark.sql import functions as f
    return frame.select(*[f.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields])
