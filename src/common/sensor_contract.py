"""Version 1 and Version 2 sensor wire contract, also used at the Bronze admission gate."""
import json
import math
import re
from datetime import datetime, timezone

IDS = ("sensor_id", "zone_id", "farm_id", "crop_id")
NAMES = ("sensor_code", "zone_name", "farm_name", "crop_name", "reading_id")
RANGES = {"soil_moisture": (0, 100), "soil_temperature": (-10, 70),
          "air_temperature": (-10, 70), "air_humidity": (0, 100)}
RANGES_V2 = {
    "soil_ph": (3.0, 10.0),
    "nitrogen": (0, 500),
    "phosphorus": (0, 300),
    "potassium": (0, 600),
    "electrical_conductivity": (0, 30),
    "battery_pct": (0, 100),
}


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def sensor_json_error(raw, reference_time=None):
    """NULL means accepted; use a stable ingress instant when routing events.

    The standalone default supports simulator validation. Streaming admission
    must use sensor_ingress_error with Kafka's broker LogAppendTime timestamp.
    """
    try:
        data = json.loads(raw, object_pairs_hook=_unique_object)
    except (ValueError, TypeError, RecursionError):
        return "malformed_json"
    if not isinstance(data, dict):
        return "expected_object"

    version = data.get("schema_version", 1)
    if type(version) is not int or version not in (1, 2):
        return "unsupported_schema_version"

    if version == 1:
        allowed = set(IDS) | set(NAMES) | set(RANGES) | {"timestamp", "schema_version"}
        if set(data) - allowed:
            return "unexpected_fields"
    else:
        allowed = set(IDS) | set(NAMES) | set(RANGES) | set(RANGES_V2) | {"timestamp", "schema_version"}
        if set(data) - allowed:
            return "unexpected_fields"

    for field in IDS:
        if type(data.get(field)) is not int or not 0 < data[field] <= 2**63 - 1:
            return f"invalid_{field}"
    for field in NAMES:
        if not isinstance(data.get(field), str) or not data[field].strip():
            return f"invalid_{field}"
    for field, (low, high) in RANGES.items():
        value = data.get(field)
        # Range-check arbitrary JSON integers before converting them to float
        # in isfinite; a huge integer must be rejected, not crash the stream.
        if type(value) not in (int, float) or not low <= value <= high or not math.isfinite(value):
            return f"invalid_{field}"
    if version == 2:
        for field, (low, high) in RANGES_V2.items():
            value = data.get(field)
            if type(value) not in (int, float) or not low <= value <= high or not math.isfinite(value):
                return f"invalid_{field}"
    timestamp = data.get("timestamp")
    if not isinstance(timestamp, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z", timestamp):
        return "invalid_timestamp"
    try:
        observed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return "invalid_timestamp"
    if reference_time is None:
        reference_time = datetime.now(timezone.utc)
    elif not isinstance(reference_time, datetime):
        return "invalid_ingress_timestamp"
    elif reference_time.tzinfo is None:
        # Spark returns naive TimestampType values in this UTC application.
        reference_time = reference_time.replace(tzinfo=timezone.utc)
    else:
        reference_time = reference_time.astimezone(timezone.utc)
    if (observed - reference_time).total_seconds() > 300:
        return "future_timestamp"
    return None


def sensor_ingress_error(raw, ingress_time):
    """Keep accepted/quarantine routing and replay independent of wall time."""
    if ingress_time is None:
        return "missing_ingress_timestamp"
    return sensor_json_error(raw, reference_time=ingress_time)
