#!/usr/bin/env python3
"""Irrigation decision text for the weather_irrigation_alerts table.

Pure functions so they can be unit-tested on the host.  The `Postpone` /
`Irrigate` / `Reduce` / `Maintain` keywords also match the Grafana dashboard
filters (e.g. the postpone KPI uses `LIKE '%Postpone%'`).
"""


def irrigation_decision(
    water_status, expected_rain, soil_moisture, recommended_water_mm=None
):
    """Compute a human-readable irrigation decision string."""
    if expected_rain and expected_rain > 0:
        rain = float(expected_rain)
        return f"Postpone irrigation - rain expected {rain:.1f} mm"

    moisture = float(soil_moisture)
    if water_status == "Dry":
        return f"Irrigate now - soil moisture {moisture:.1f}% below optimum"
    if water_status == "Overwatered":
        return f"Reduce irrigation - soil moisture {moisture:.1f}% above optimum"
    return "Maintain irrigation schedule"
