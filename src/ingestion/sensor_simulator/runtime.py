"""Apply only typed, bounded simulator settings from the application database."""
import math
from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeSettings:
    interval_seconds: float
    anomaly_rate: float
    enabled: bool = True
    simulate_irrigation: bool = False
    schema_version: int = 1


def runtime_settings(values, defaults):
    interval = values.get('simulator.interval_seconds', defaults.interval_seconds)
    anomaly = values.get('simulator.anomaly_rate', defaults.anomaly_rate)
    enabled = values.get('simulator.enabled', defaults.enabled)
    simulate_irrigation = values.get('simulator.simulate_irrigation', getattr(defaults, 'simulate_irrigation', False))
    schema_version = values.get('simulator.schema_version', getattr(defaults, 'schema_version', 1))
    for value, low, high in ((interval, 0.1, 3600), (anomaly, 0, 1)):
        if type(value) not in (int, float) or not math.isfinite(value) or not low <= value <= high:
            raise ValueError('Invalid simulator configuration')
    if type(enabled) is not bool:
        raise ValueError('Simulator enabled requires a boolean')
    if type(simulate_irrigation) is not bool:
        raise ValueError('Simulator simulate_irrigation requires a boolean')
    if type(schema_version) is not int or schema_version not in (1, 2):
        raise ValueError('Simulator schema_version must be 1 or 2')
    return RuntimeSettings(float(interval), float(anomaly), enabled, simulate_irrigation, schema_version)
