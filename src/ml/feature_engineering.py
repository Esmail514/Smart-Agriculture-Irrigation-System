#!/usr/bin/env python3
"""Feature engineering for the irrigation ML pipeline.

Pure-pandas module (no Spark / sklearn imports) so it can be unit-tested on
the host.  In the Spark container it runs inside the driver via toPandas().
"""

import pandas as pd


def merge_weather(zone_pdf, weather_pdf=None):
    """Left-join daily weather (rain, wind) onto zone readings by (farm_id, date).

    Falls back to 0.0 when no weather row exists for that farm/day.
    """
    df = zone_pdf.copy()
    weather = (
        weather_pdf.copy()
        if weather_pdf is not None and not weather_pdf.empty
        else None
    )

    if weather is None:
        df["total_rain"] = 0.0
        df["avg_wind_speed"] = 0.0
        df["weather_available"] = 0
        return df

    weather = weather.copy()
    weather["__weather_date"] = pd.to_datetime(weather["weather_date"]).dt.normalize()
    # One row per (farm_id, day): if the upstream aggregation ever produced
    # duplicates, the most recent wins.
    weather = weather.sort_values("__weather_date").drop_duplicates(
        ["farm_id", "__weather_date"], keep="last"
    )

    df["__reading_date"] = pd.to_datetime(df["last_update"]).dt.normalize()
    df = df.merge(
        weather[["farm_id", "__weather_date", "total_rain", "avg_wind_speed"]].rename(
            columns={"__weather_date": "__reading_date"}
        ),
        on=["farm_id", "__reading_date"],
        how="left",
        indicator=True,
    )
    df = df.drop(columns=["__reading_date"])
    df["weather_available"] = (df["_merge"] == "both").astype(int)
    df = df.drop(columns=["_merge"])
    df["total_rain"] = pd.to_numeric(df["total_rain"], errors="coerce").fillna(0.0)
    df["avg_wind_speed"] = pd.to_numeric(df["avg_wind_speed"], errors="coerce").fillna(
        0.0
    )
    return df


def engineer_features(zone_pdf, weather_pdf=None):
    df = merge_weather(zone_pdf, weather_pdf)
    df["needs_irrigation"] = (df["water_status"] == "Dry").astype(int)

    df["moisture_deficit"] = df["moisture_min"] - df["avg_soil_moisture"]
    df["moisture_surplus"] = df["avg_soil_moisture"] - df["moisture_max"]

    dt = pd.to_datetime(df["last_update"])
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

    if len(df) > 1:
        df = df.sort_values(["zone_id", "last_update"])
        df["moisture_rolling_mean_3"] = df.groupby("zone_id")[
            "avg_soil_moisture"
        ].transform(lambda x: x.rolling(3, min_periods=1).mean())
        df["moisture_trend"] = df["avg_soil_moisture"] - df["moisture_rolling_mean_3"]
    else:
        df["moisture_rolling_mean_3"] = df["avg_soil_moisture"]
        df["moisture_trend"] = 0.0

    df["avg_temperature"] = df["avg_air_temperature"]
    df["avg_humidity"] = df["avg_air_humidity"]

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
        "total_rain",
        "avg_wind_speed",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df
