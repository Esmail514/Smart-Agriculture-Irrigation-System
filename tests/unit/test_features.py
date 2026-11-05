"""Tests for the ML feature engineering module (A2: weather features)."""

import pandas as pd
import pytest

from src.ml.feature_engineering import engineer_features, merge_weather


def _weather_frame(data):
    frame = pd.DataFrame(data)
    frame["last_update"] = frame["weather_date"] + " 07:00:00"
    return frame


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
    weather = _weather_frame(
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
    weather = _weather_frame(
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
    weather = _weather_frame(
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


def test_weather_temperature_is_preserved_instead_of_sensor_average():
    weather = _weather_frame({"farm_id": [100], "weather_date": ["2026-08-29"],
                            "total_rain": [0.0], "avg_wind_speed": [2.0],
                            "avg_temperature": [18.0], "avg_humidity": [85.0]})
    df = engineer_features(_zone_df(), weather)
    assert df.loc[df.farm_id == 100, "avg_temperature"].iloc[0] == 18.0
    assert df.loc[df.farm_id == 100, "avg_humidity"].iloc[0] == 85.0
    assert df.loc[df.farm_id == 200, "avg_temperature"].iloc[0] == 24.0


def test_rolling_moisture_never_crosses_farm_boundary():
    zones = _zone_df()
    zones["zone_id"] = 1
    df = engineer_features(zones)
    assert list(df["moisture_rolling_mean_3"]) == [35.0, 22.0]


def test_pipeline_and_spark_feature_paths_share_weather_contract():
    from src.ml.pipelines.engineer import engineer_features as engineer_loaded
    weather = _weather_frame({"farm_id": [100], "weather_date": ["2026-08-29"],
                            "total_rain": [7.0], "avg_wind_speed": [3.0]})
    spark_path = engineer_features(_zone_df(), weather)
    loaded_path = engineer_loaded(merge_weather(_zone_df(), weather))
    for feature in ["total_rain", "avg_wind_speed", "moisture_deficit", "weather_available"]:
        assert list(spark_path[feature]) == list(loaded_path[feature])


def test_unknown_crop_rule_is_not_labeled_as_healthy():
    frame = _zone_df()
    frame.loc[0, "water_status"] = "Unknown_NoRule"
    result = engineer_features(frame)
    assert pd.isna(result.iloc[0]["needs_irrigation"])


@pytest.mark.parametrize("value", [None, float("nan"), float("inf"), "broken"])
def test_invalid_measured_feature_does_not_become_zero(value):
    frame = _zone_df()
    frame["avg_soil_moisture"] = frame["avg_soil_moisture"].astype(object)
    frame.loc[0, "avg_soil_moisture"] = value
    with pytest.raises(ValueError, match="avg_soil_moisture"):
        engineer_features(frame)


def test_rolling_features_respect_gold_snapshot_sequence_for_equal_event_times():
    frame = pd.concat([_zone_df().iloc[:1]] * 2, ignore_index=True)
    frame["snapshot_id"] = [8, 7]
    frame["avg_soil_moisture"] = [10.0, 40.0]
    result = engineer_features(frame)
    assert list(result.snapshot_id) == [7, 8]
    assert list(result.moisture_rolling_mean_3) == [40.0, 25.0]


def test_daily_weather_from_later_that_day_cannot_enter_earlier_features():
    weather = _weather_frame({"farm_id": [100], "weather_date": ["2026-08-29"],
                              "total_rain": [9.0], "avg_wind_speed": [4.0]})
    weather["last_update"] = "2026-08-29 18:00:00"
    result = engineer_features(_zone_df(), weather)
    assert list(result["weather_available"]) == [0, 0]
    assert list(result["total_rain"]) == [0.0, 0.0]


def test_asof_join_keeps_older_eligible_weather_when_newer_snapshot_is_in_future():
    weather = _weather_frame({"farm_id": [100, 100], "weather_date": ["2026-08-29"] * 2,
                              "total_rain": [2.0, 9.0], "avg_wind_speed": [4.0, 5.0]})
    weather["last_update"] = ["2026-08-29 07:00:00", "2026-08-29 18:00:00"]
    zones = _zone_df()
    zones.index = [99, 4]
    result = merge_weather(zones, weather.iloc[::-1])
    assert list(result.index) == [99, 4]
    assert list(result["total_rain"]) == [2.0, 0.0]
    assert len(result) == len(zones)


def test_weather_requires_timestamp_provenance_and_complete_rain_wind_fields():
    weather = pd.DataFrame({"farm_id": [100], "weather_date": ["2026-08-29"],
                            "total_rain": [9.0], "avg_wind_speed": [4.0]})
    assert not merge_weather(_zone_df(), weather)["weather_available"].any()
    weather["last_update"] = "2026-08-29 07:00:00"
    assert not merge_weather(_zone_df(), weather.drop(columns=["avg_wind_speed"]))["weather_available"].any()


@pytest.mark.parametrize("value", [float("inf"), float("nan"), -1.0])
def test_invalid_weather_is_unavailable_instead_of_an_observed_zero(value):
    weather = _weather_frame({"farm_id": [100], "weather_date": ["2026-08-29"],
                              "total_rain": [value], "avg_wind_speed": [4.0]})
    result = merge_weather(_zone_df(), weather)
    assert list(result["weather_available"]) == [0, 0]
    assert list(result["total_rain"]) == [0.0, 0.0]


def test_known_publication_time_prevents_backdated_weather_availability():
    weather = _weather_frame({"farm_id": [100], "weather_date": ["2026-08-29"],
                              "total_rain": [2.0], "avg_wind_speed": [4.0]})
    weather["available_at"] = "2026-08-29 11:00:00"
    result = merge_weather(_zone_df(), weather)
    assert not result["weather_available"].any()
