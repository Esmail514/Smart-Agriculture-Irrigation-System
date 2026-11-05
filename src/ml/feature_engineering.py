#!/usr/bin/env python3
"""Feature engineering for the irrigation ML pipeline.

Pure-pandas module (no Spark / sklearn imports) so it can be unit-tested on
the host. In the Spark container it runs inside the driver via toPandas().
"""

import pandas as pd
import numpy as np

try:
    from common.agronomy import CROP_KC_CATALOG
except ImportError:
    try:
        from src.common.agronomy import CROP_KC_CATALOG
    except ImportError:
        CROP_KC_CATALOG = {}

SOIL_WATER_PROPERTIES = {
    "Sandy": {"fc": 15.0, "pwp": 6.0, "mad": 0.50},
    "Loam": {"fc": 27.0, "pwp": 12.0, "mad": 0.50},
    "Silty": {"fc": 31.0, "pwp": 14.0, "mad": 0.50},
    "Clay": {"fc": 38.0, "pwp": 22.0, "mad": 0.50},
}

CLIP_BOUNDS = {
    "avg_soil_moisture": (0.0, 100.0),
    "avg_soil_temperature": (-10.0, 70.0),
    "avg_air_temperature": (-20.0, 60.0),
    "avg_air_humidity": (0.0, 100.0),
    "avg_temperature": (-20.0, 60.0),
    "avg_humidity": (0.0, 100.0),
    "total_rain": (0.0, 200.0),
    "avg_wind_speed": (0.0, 60.0),
    "vpd_kpa": (0.0, 15.0),
    "moisture_depletion_rate_hourly": (-15.0, 15.0),
    "et0_est": (0.0, 15.0),
    "etc_est": (0.0, 25.0),
}


def merge_weather(zone_pdf, weather_pdf=None):
    """Attach the latest same-day aggregate no newer than each observation.

    Missing weather is represented by weather_available=0 and zero placeholders.
    last_update only proves event-time ordering; without a fetched-at archive it
    does not prove point-in-time availability. Training therefore continues to
    exclude these weather aggregates. An optional available_at tightens the join
    when an actual publication timestamp is supplied.
    """
    df = zone_pdf.copy()
    required = {"farm_id", "weather_date", "last_update", "total_rain", "avg_wind_speed"}
    if weather_pdf is None or weather_pdf.empty or not required.issubset(weather_pdf.columns):
        df["total_rain"] = 0.0
        df["avg_wind_speed"] = 0.0
        df["weather_available"] = 0
        return df

    weather = weather_pdf.copy()
    weather["__reading_date"] = pd.to_datetime(weather["weather_date"], utc=True, errors="coerce").dt.normalize()
    weather["__weather_available_at"] = pd.to_datetime(weather["last_update"], utc=True, errors="coerce")
    if "available_at" in weather:
        published = pd.to_datetime(weather["available_at"], utc=True, errors="coerce")
        # Never use a record before either its observed or publication instant.
        weather["__weather_available_at"] = published.where(published >= weather["__weather_available_at"], weather["__weather_available_at"])
        weather.loc[published.isna(), "__weather_available_at"] = pd.NaT
    weather = weather.dropna(subset=["__reading_date", "__weather_available_at"])
    weather_features = [c for c in ["total_rain", "avg_wind_speed", "avg_temperature", "avg_humidity"] if c in weather]
    bounds = {"total_rain": (0, float("inf")), "avg_wind_speed": (0, 60),
              "avg_temperature": (-20, 55), "avg_humidity": (0, 100)}
    valid = pd.Series(True, index=weather.index)
    for name in weather_features:
        values = pd.to_numeric(weather[name], errors="coerce")
        weather[name] = values
        valid &= np.isfinite(values.to_numpy(dtype=float, na_value=np.nan)) & values.between(*bounds[name])
    weather = weather.loc[valid]

    df["__reading_time"] = pd.to_datetime(df["last_update"], utc=True, errors="raise")
    if df["__reading_time"].isna().any():
        raise ValueError("Zone observations require last_update")
    df["__reading_date"] = df["__reading_time"].dt.normalize()
    df["__row_position"] = np.arange(len(df))
    # asof keeps an older eligible snapshot when a newer one belongs to the
    # future. Deduplicating a full day before the join would lose that snapshot.
    result = pd.merge_asof(
        df.sort_values("__reading_time", kind="stable"),
        weather[["farm_id", "__reading_date", "__weather_available_at"] + weather_features]
        .sort_values("__weather_available_at", kind="stable"),
        left_on="__reading_time", right_on="__weather_available_at",
        by=["farm_id", "__reading_date"], direction="backward",
    ).sort_values("__row_position", kind="stable")
    result["weather_available"] = result["__weather_available_at"].notna().astype(int)
    result = result.drop(columns=["__reading_time", "__reading_date", "__weather_available_at", "__row_position"])
    result.index = zone_pdf.index
    for name in ["total_rain", "avg_wind_speed"]:
        result[name] = result[name].fillna(0.0)
    return result


def engineer_features(zone_pdf, weather_pdf=None):
    if zone_pdf.empty:
        return zone_pdf.copy()
    df = merge_weather(zone_pdf, weather_pdf)
    # Never turn a broken observation into a plausible zero-valued sensor.
    # Unknown crop-rule thresholds remain nullable and are handled by the
    # explicit Unknown_NoRule label, but measured fields must be finite.
    for name in ["avg_soil_moisture", "avg_soil_temperature", "avg_air_temperature", "avg_air_humidity"]:
        values = pd.to_numeric(df[name], errors="coerce")
        if not np.isfinite(values.to_numpy(dtype=float, na_value=np.nan)).all():
            raise ValueError(f"Invalid observed sensor feature: {name}")
        df[name] = values
    if "water_status" in df:
        df["needs_irrigation"] = df["water_status"].map({"Dry": 1, "Optimal": 0, "Overwatered": 0}).astype("Int64")

    if "moisture_min" in df.columns:
        df["moisture_deficit"] = df["moisture_min"] - df["avg_soil_moisture"]
    else:
        df["moisture_deficit"] = 0.0
    if "moisture_max" in df.columns:
        df["moisture_surplus"] = df["avg_soil_moisture"] - df["moisture_max"]
    else:
        df["moisture_surplus"] = 0.0

    dt = pd.to_datetime(df["last_update"], utc=True, errors="raise")
    if dt.isna().any():
        raise ValueError("Zone observations require last_update")
    df["last_update"] = dt
    df["month"] = dt.dt.month
    df["day_of_week"] = dt.dt.dayofweek
    df["season"] = dt.dt.month.map(
        {
            12: 3,
            1: 3,
            2: 3,
            3: 0,
            4: 0,
            5: 0,
            6: 1,
            7: 1,
            8: 1,
            9: 2,
            10: 2,
            11: 2,
        }
    )

    keys = [c for c in ["farm_id", "zone_id"] if c in df]
    if len(df) > 1 and keys:
        order = keys + ["last_update"] + (["snapshot_id"] if "snapshot_id" in df else [])
        df = df.sort_values(order, kind="stable")
        df["moisture_rolling_mean_3"] = df.groupby(keys)[
            "avg_soil_moisture"
        ].transform(lambda x: x.rolling(3, min_periods=1).mean())
        df["moisture_trend"] = df["avg_soil_moisture"] - df["moisture_rolling_mean_3"]

        # Physical moisture kinetics: hourly depletion rate dtheta/dt
        prev_m = df.groupby(keys)["avg_soil_moisture"].shift(1)
        prev_t = df.groupby(keys)["last_update"].shift(1)
        dt_hrs = (df["last_update"] - prev_t).dt.total_seconds() / 3600.0
        # Only compute depletion rate when elapsed time is positive and within a plausible observation window (<= 48h)
        valid_dt = (dt_hrs > 0.0) & (dt_hrs <= 48.0)
        hourly_depletion = pd.Series(0.0, index=df.index)
        hourly_depletion.loc[valid_dt] = (
            (prev_m.loc[valid_dt] - df.loc[valid_dt, "avg_soil_moisture"])
            / dt_hrs.loc[valid_dt].clip(lower=0.25)
        )
        df["moisture_depletion_rate_hourly"] = hourly_depletion.fillna(0.0).clip(lower=-15.0, upper=15.0).round(4)
    else:
        df["moisture_rolling_mean_3"] = df["avg_soil_moisture"]
        df["moisture_trend"] = 0.0
        df["moisture_depletion_rate_hourly"] = 0.0

    for weather_col, sensor_col in [("avg_temperature", "avg_air_temperature"), ("avg_humidity", "avg_air_humidity")]:
        if weather_col in df:
            df[weather_col] = df[weather_col].fillna(df[sensor_col])
        else:
            df[weather_col] = df[sensor_col]

    # Clip bounded sensor fields to prevent corrupted outliers from skewing ML
    for col, bounds in CLIP_BOUNDS.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").clip(lower=bounds[0], upper=bounds[1])

    # Soil physical properties (USDA NRCS / FAO standards)
    soil_series = df["soil_type"] if "soil_type" in df.columns else pd.Series(["Loam"] * len(df), index=df.index)
    fc_vals, pwp_vals, raw_vals = [], [], []
    for st in soil_series.fillna("Loam"):
        clean = str(st).strip().title()
        if clean in ("Sand", "Sandy"):
            props = SOIL_WATER_PROPERTIES["Sandy"]
        elif clean in ("Silt", "Silty", "Silt Loam", "Silty Loam"):
            props = SOIL_WATER_PROPERTIES["Silty"]
        elif clean in ("Clay", "Clay Loam"):
            props = SOIL_WATER_PROPERTIES["Clay"]
        else:
            props = SOIL_WATER_PROPERTIES.get(clean, SOIL_WATER_PROPERTIES["Loam"])
        fc_vals.append(props["fc"])
        pwp_vals.append(props["pwp"])
        raw_vals.append(round((props["fc"] - props["pwp"]) * props["mad"], 3))
    df["field_capacity"] = fc_vals
    df["wilting_point"] = pwp_vals
    df["raw_water"] = raw_vals

    # Atmospheric VPD (Vapor Pressure Deficit in kPa) - FAO-56 eq. 11/17/19
    t_air = df["avg_air_temperature"]
    rh_air = df["avg_air_humidity"].clip(lower=1.0, upper=100.0)
    es = 0.6108 * np.exp((17.27 * t_air) / (t_air + 237.3))
    ea = es * (rh_air / 100.0)
    df["vpd_kpa"] = (es - ea).clip(lower=0.0, upper=15.0).round(4)

    # Evapotranspiration estimation (FAO-56 Hargreaves-Samani eq. 52 / Radiation baseline)
    doy = df["last_update"].dt.dayofyear
    phi = np.radians(df["latitude"]) if "latitude" in df.columns else 0.26  # Sub-tropical ~15°N baseline
    delta = 0.409 * np.sin((2.0 * np.pi * doy / 365.0) - 1.39)
    dr = 1.0 + 0.033 * np.cos(2.0 * np.pi * doy / 365.0)
    ws = np.arccos(np.clip(-np.tan(phi) * np.tan(delta), -1.0, 1.0))
    ra = (24.0 * 60.0 / np.pi) * 0.0820 * dr * (ws * np.sin(phi) * np.sin(delta) + np.cos(phi) * np.cos(delta) * np.sin(ws))
    t_range = (df["avg_soil_temperature"] - df["avg_air_temperature"]).abs().clip(lower=3.0, upper=25.0)
    et0_val = (0.0023 * (t_air + 17.8).clip(lower=0.0) * np.sqrt(t_range) * (ra * 0.408)).clip(lower=0.0, upper=15.0)
    df["et0_est"] = et0_val.round(3)

    # Crop Kc mapping for ETc (FAO-56 Table 12)
    crop_series = df["crop_name"].astype(str) if "crop_name" in df.columns else pd.Series([""] * len(df), index=df.index)
    kc_vals = []
    for c_name in crop_series:
        norm_name = str(c_name).strip().lower().replace(" ", "_").replace("-", "_")
        kc_info = CROP_KC_CATALOG.get(norm_name, {"mid": 1.0})
        kc_vals.append(kc_info.get("mid", 1.0))
    df["etc_est"] = (df["et0_est"] * np.array(kc_vals)).clip(lower=0.0, upper=25.0).round(3)

    # Net Irrigation Requirement in mm: NIR = max(0, moisture_deficit * active_root_zone_factor)
    if "moisture_deficit" in df.columns:
        df["nir_mm"] = (df["moisture_deficit"].clip(lower=0.0) * 0.40).round(2)

    for col in [
        "avg_soil_moisture",
        "avg_soil_temperature",
        "avg_air_temperature",
        "avg_air_humidity",
        "avg_temperature",
        "avg_humidity",
        "moisture_deficit",
        "moisture_surplus",
        "moisture_rolling_mean_3",
        "moisture_trend",
        "moisture_depletion_rate_hourly",
        "vpd_kpa",
        "field_capacity",
        "wilting_point",
        "raw_water",
        "et0_est",
        "etc_est",
        "nir_mm",
        "total_rain",
        "avg_wind_speed",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df
