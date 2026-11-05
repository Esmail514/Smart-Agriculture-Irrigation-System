import math
import pytest
from src.ingestion.sensor_simulator.runtime import RuntimeSettings, runtime_settings


@pytest.mark.parametrize('values', [
    {'simulator.interval_seconds': True}, {'simulator.interval_seconds': 0},
    {'simulator.anomaly_rate': math.nan}, {'simulator.anomaly_rate': '0.2'},
    {'simulator.enabled': 'false'}, {'simulator.anomaly_rate': 1.1}])
def test_bad_runtime_settings_rejected(values):
    with pytest.raises(ValueError):
        runtime_settings(values, RuntimeSettings(5, 0.03))


def test_runtime_settings_respect_disable_and_preserve_defaults():
    defaults = RuntimeSettings(5, 0.03)
    assert runtime_settings({}, defaults) == defaults
    settings = runtime_settings({'simulator.enabled': False, 'simulator.interval_seconds': 30,
                                 'simulator.anomaly_rate': 0.2}, defaults)
    assert settings == RuntimeSettings(30, 0.2, False)
