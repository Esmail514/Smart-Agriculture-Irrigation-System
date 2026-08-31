"""Tests for the ML feature engineering module (A2: weather features)."""

import pandas as pd

from src.ml.feature_engineering import engineer_features, merge_weather


def _zone_df():
    return pd.DataFrame(
        {
            "zone_id": [1, 2],
            "zone_name": ["Tomato-01", "Wheat-AJ-01"],
            "farm_id": [100, 200],
            "farm_name": ["Farm A", "Farm B"],
            "crop_id": [7, 10],
            "crop_name": ["Tomato", "Wheat"],
            "sensor_count": [4, 6],
            "avg_soil_moisture": [35.0, 22.0],
            "avg_soil_temperature": [26.0, 20.0],
            "avg_air_temperature": [30.0, 24.0],
            "avg_air_humidity": [60.0, 55.0],
            "moisture_min": [30.0, 25.0],
            "moisture_max": [45.0, 40.0],
            "recommended_water_mm": [18.0, 16.0],
            "last_update": ["2026-08-29 10:00:00", "2026-08-29 08:00:00"],
            "water_status": ["Optimal", "Dry"],
        }
    )


def test_no_weather_defaults_to_zero():
    df = engineer_features(_zone_df(), None)
    assert list(df["total_rain"]) == [0.0, 0.0]
    assert list(df["avg_wind_speed"]) == [0.0, 0.0]
    assert list(df["weather_available"]) == [0, 0]


def test_weather_merged_by_farm_and_date():
    weather = pd.DataFrame(
        {
            "farm_id": [100, 100, 200],
            "weather_date": ["2026-08-29", "2026-08-28", "2026-08-29"],
            "total_rain": [5.0, 0.0, 1.5],
            "avg_wind_speed": [12.0, 3.0, 9.0],
        }
    )
    df = engineer_features(_zone_df(), weather)
    assert list(df["total_rain"]) == [5.0, 1.5]
    assert list(df["avg_wind_speed"]) == [12.0, 9.0]
    assert list(df["weather_available"]) == [1, 1]


def test_weather_only_matches_same_date():
    weather = pd.DataFrame(
        {
            "farm_id": [100],
            "weather_date": ["2026-08-28"],  # different day than the reading
            "total_rain": [9.0],
            "avg_wind_speed": [7.0],
        }
    )
    df = engineer_features(_zone_df(), weather)
    assert list(df["total_rain"]) == [0.0, 0.0]
    assert list(df["weather_available"]) == [0, 0]


def test_duplicate_weather_deduped_keep_last():
    weather = pd.DataFrame(
        {
            "farm_id": [100, 100],
            "weather_date": ["2026-08-29", "2026-08-29"],
            "total_rain": [2.0, 9.5],
            "avg_wind_speed": [4.0, 14.0],
        }
    )
    df = merge_weather(_zone_df(), weather)
    assert float(df.loc[df["farm_id"] == 100, "total_rain"].iloc[0]) == 9.5


def test_engineer_features_derived_columns():
    df = engineer_features(_zone_df(), None)
    assert "needs_irrigation" in df.columns
    assert list(df["needs_irrigation"]) == [0, 1]
    assert "moisture_deficit" in df.columns
    assert list(df["moisture_deficit"]) == [
        -5.0,
        3.0,
    ]  # moisture_min - avg_soil_moisture
    assert "total_rain" in df.columns
    assert "avg_wind_speed" in df.columns
    assert "season" in df.columns
    assert (df["total_rain"] == 0.0).all()
