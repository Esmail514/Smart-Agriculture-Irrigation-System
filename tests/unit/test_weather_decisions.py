from src.processing.weather.decisions import observed_weather_decision


def test_missing_weather_never_becomes_a_zero_rain_decision():
    assert observed_weather_decision("Dry", None, 10.0).startswith("Review")
    assert observed_weather_decision("Optimal", None, 40.0).startswith("Review")


def test_missing_crop_rule_never_becomes_maintain():
    assert observed_weather_decision("Unknown_NoRule", 0.0, 30.0).startswith("Review")
    assert observed_weather_decision(None, 0.0, 30.0).startswith("Review")


def test_real_observed_zero_rain_uses_crop_soil_status():
    assert observed_weather_decision("Dry", 0.0, 10.0).startswith("Irrigate")
    assert observed_weather_decision("Optimal", 0.0, 40.0).startswith("Maintain")


def test_observed_rain_is_explicitly_not_a_forecast():
    text = observed_weather_decision("Dry", 3.5, 10.0)
    assert text.startswith("Review")
    assert "observed rainfall 3.5" in text
    assert "not a forecast" in text
