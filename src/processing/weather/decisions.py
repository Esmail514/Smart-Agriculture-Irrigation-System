"""Decision text from observed rain, explicitly separate from unavailable forecasts."""
import math
from common.irrigation_decisions import irrigation_decision


def observed_weather_decision(status, observed_rain, soil_moisture):
    if status not in {"Dry", "Optimal", "Overwatered"}:
        return "Review irrigation - crop rule unavailable"
    if observed_rain is None or not math.isfinite(float(observed_rain)):
        return "Review irrigation - recent weather unavailable"
    if soil_moisture is None or not math.isfinite(float(soil_moisture)):
        return "Review irrigation - soil observation unavailable"
    if float(observed_rain) > 0:
        return f"Review irrigation - observed rainfall {float(observed_rain):.1f} mm (not a forecast)"
    return irrigation_decision(status, 0.0, soil_moisture)
