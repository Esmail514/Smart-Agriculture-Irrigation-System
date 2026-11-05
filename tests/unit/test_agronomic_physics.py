"""Comprehensive verification of agronomic physics and precision irrigation standards.

Tests:
1. USDA Soil Physical Properties (FC, PWP, RAW) per soil type and robustness to case/alias.
2. Atmospheric Vapor Pressure Deficit (VPD in kPa) mathematical accuracy and bounds.
3. FAO-56 Hargreaves-Samani Reference ET (ET0) and Crop ET (ETc) with crop Kc mapping.
4. Hourly soil moisture depletion kinetics (dtheta/dt) over elapsed time vs fixed rows.
5. Net Irrigation Requirement (NIR mm) and actuator run duration capping (<= 3600s).
"""
import json
import numpy as np
import pandas as pd
import pytest

from src.ml.feature_engineering import SOIL_WATER_PROPERTIES, CLIP_BOUNDS, engineer_features
from src.ml.inference import build_predictions


def test_usda_soil_physical_properties_mapping():
    """Verify FC, PWP, and RAW are accurately computed for USDA soil textures."""
    df = pd.DataFrame({
        "farm_id": [1, 2, 3, 4, 5, 6],
        "zone_id": [10, 20, 30, 40, 50, 60],
        "crop_name": ["Tomato", "Wheat", "Maize", "Olive", "Date Palm", "Potato"],
        "soil_type": ["Sandy", "sandy", "Loam", "Silty", "Clay", "sand"],
        "last_update": ["2026-07-01 10:00:00"] * 6,
        "avg_soil_moisture": [12.0, 14.0, 25.0, 28.0, 35.0, 10.0],
        "avg_soil_temperature": [28.0] * 6,
        "avg_air_temperature": [32.0] * 6,
        "avg_air_humidity": [45.0] * 6,
        "moisture_min": [20.0] * 6,
        "moisture_max": [35.0] * 6,
    })
    feat = engineer_features(df)

    # Sandy soil (15% FC, 6% PWP, RAW = (15-6)*0.5 = 4.5%)
    assert feat.loc[0, "field_capacity"] == 15.0
    assert feat.loc[0, "wilting_point"] == 6.0
    assert feat.loc[0, "raw_water"] == 4.5
    # Lowercase 'sandy' and alias 'sand' must map to Sandy
    assert feat.loc[1, "field_capacity"] == 15.0
    assert feat.loc[1, "wilting_point"] == 6.0
    assert feat.loc[5, "field_capacity"] == 15.0
    assert feat.loc[5, "wilting_point"] == 6.0

    # Loam (27% FC, 12% PWP, RAW = (27-12)*0.5 = 7.5%)
    assert feat.loc[2, "field_capacity"] == 27.0
    assert feat.loc[2, "wilting_point"] == 12.0
    assert feat.loc[2, "raw_water"] == 7.5

    # Silty (31% FC, 14% PWP, RAW = (31-14)*0.5 = 8.5%)
    assert feat.loc[3, "field_capacity"] == 31.0
    assert feat.loc[3, "wilting_point"] == 14.0
    assert feat.loc[3, "raw_water"] == 8.5

    # Clay (38% FC, 22% PWP, RAW = (38-22)*0.5 = 8.0%)
    assert feat.loc[4, "field_capacity"] == 38.0
    assert feat.loc[4, "wilting_point"] == 22.0
    assert feat.loc[4, "raw_water"] == 8.0


def test_vpd_computations_sound_and_bounded():
    """Verify Tetens FAO-56 VPD formulation across extreme weather conditions."""
    df = pd.DataFrame({
        "farm_id": [1, 1, 1, 1],
        "zone_id": [1, 2, 3, 4],
        "crop_name": ["Tomato"] * 4,
        "last_update": ["2026-07-01 12:00:00"] * 4,
        "avg_soil_moisture": [25.0] * 4,
        "avg_soil_temperature": [25.0, 30.0, 45.0, 10.0],
        # 100% RH (saturated), 50% RH (moderate), 10% RH at 45C (extreme arid), 0% RH
        "avg_air_temperature": [20.0, 30.0, 45.0, 5.0],
        "avg_air_humidity": [100.0, 50.0, 10.0, 0.0],
        "moisture_min": [20.0] * 4,
        "moisture_max": [35.0] * 4,
    })
    feat = engineer_features(df)

    # At 100% RH, VPD must be exactly 0.0 kPa
    assert feat.loc[0, "vpd_kpa"] == 0.0

    # At 30C, 50% RH: es = 0.6108*exp(17.27*30/267.3) = 4.246 kPa; ea = 2.123; VPD ~ 2.123 kPa
    assert 2.0 <= feat.loc[1, "vpd_kpa"] <= 2.3

    # At 45C, 10% RH: es = 9.59 kPa; ea = 0.96; VPD ~ 8.63 kPa (strictly bounded <= 15.0)
    assert 8.0 <= feat.loc[2, "vpd_kpa"] <= 9.0
    assert feat["vpd_kpa"].max() <= 15.0
    assert feat["vpd_kpa"].min() >= 0.0


def test_fao56_et0_and_etc_computations():
    """Verify Hargreaves-Samani ET0 and crop Kc ETc scaling."""
    df = pd.DataFrame({
        "farm_id": [1, 1],
        "zone_id": [1, 2],
        "crop_name": ["Date Palm", "Tomato"],
        "last_update": ["2026-06-21 12:00:00", "2026-06-21 12:00:00"],  # Summer solstice
        "avg_soil_moisture": [20.0, 22.0],
        "avg_soil_temperature": [35.0, 35.0],
        "avg_air_temperature": [38.0, 38.0],
        "avg_air_humidity": [30.0, 30.0],
        "moisture_min": [25.0, 28.0],
        "moisture_max": [40.0, 40.0],
    })
    feat = engineer_features(df)

    # In summer sub-tropics at 38C, ET0 should be in realistic range ~ 3.0 - 9.0 mm/day
    et0 = feat.loc[0, "et0_est"]
    assert 3.0 <= et0 <= 10.0

    # Date Palm mid-season Kc is 0.95 (from CROP_KC_CATALOG)
    # Even with spaces/casing, Date Palm must resolve correctly
    etc_palm = feat.loc[0, "etc_est"]
    assert np.isclose(etc_palm, round(et0 * 0.95, 3), atol=0.01)

    # Tomato mid-season Kc is 1.15
    etc_tomato = feat.loc[1, "etc_est"]
    assert np.isclose(etc_tomato, round(et0 * 1.15, 3), atol=0.01)


def test_hourly_moisture_depletion_kinetics_over_elapsed_time():
    """Verify dtheta/dt uses true elapsed delta-t, handles zero delta-t, and clips properly."""
    df = pd.DataFrame({
        "farm_id": [1, 1, 1, 1],
        "zone_id": [10, 10, 10, 10],
        "snapshot_id": [1, 2, 3, 4],
        "last_update": [
            "2026-07-01 08:00:00",
            "2026-07-01 10:00:00",  # 2 hours elapsed, moisture dropped by 3% (1.5%/hr)
            "2026-07-01 10:00:00",  # Simultaneous snapshot (dt = 0), rate must be 0.0
            "2026-07-01 14:00:00",  # 4 hours elapsed, moisture increased by 10% (irrigation recharge)
        ],
        "avg_soil_moisture": [30.0, 27.0, 27.0, 37.0],
        "avg_soil_temperature": [28.0] * 4,
        "avg_air_temperature": [32.0] * 4,
        "avg_air_humidity": [50.0] * 4,
        "moisture_min": [25.0] * 4,
        "moisture_max": [38.0] * 4,
    })
    feat = engineer_features(df)

    # Row 0: Initial point, depletion rate = 0.0
    assert feat.loc[0, "moisture_depletion_rate_hourly"] == 0.0

    # Row 1: Dropped 3.0% over 2.0 hours -> 1.5 %/hr
    assert np.isclose(feat.loc[1, "moisture_depletion_rate_hourly"], 1.5, atol=0.01)

    # Row 2: Same timestamp (0 elapsed seconds) -> must NOT divide by 0 or produce fake spikes
    assert feat.loc[2, "moisture_depletion_rate_hourly"] == 0.0

    # Row 3: Recharge of 10% over 4 hours -> negative rate (approx -2.5 %/hr)
    assert feat.loc[3, "moisture_depletion_rate_hourly"] < 0.0
    assert np.isclose(feat.loc[3, "moisture_depletion_rate_hourly"], -2.5, atol=0.01)


def test_net_irrigation_requirement_and_actuator_run_capping():
    """Verify NIR mm calculation and actuator run duration safety capping (<= 3600 seconds)."""
    df = pd.DataFrame({
        "farm_id": [1, 1, 1],
        "zone_id": [1, 2, 3],
        "last_update": ["2026-07-01 12:00:00"] * 3,
        "avg_soil_moisture": [15.0, 28.0, 10.0],
        "moisture_min": [25.0, 25.0, 30.0],
        "moisture_max": [35.0, 35.0, 40.0],
        "moisture_deficit": [10.0, -3.0, 20.0],  # deficits: 10% (NIR=4.0mm), 0%, 20% (NIR=8.0mm)
        "vpd_kpa": [2.5, 0.8, 3.0],
        "moisture_depletion_rate_hourly": [1.8, -0.2, 2.2],
        "total_rain": [0.0, 5.0, 0.0],
    })
    # Probabilities: 0.90 (high dry), 0.10 (wet), 0.95 (extreme dry)
    preds = build_predictions(df, [0.90, 0.10, 0.95], "test-model-v2", ["avg_soil_moisture", "moisture_deficit"])
    assert len(preds) == 3

    snap1 = json.loads(preds[0]["feature_snapshot"])
    snap2 = json.loads(preds[1]["feature_snapshot"])
    snap3 = json.loads(preds[2]["feature_snapshot"])

    # Zone 1: deficit 10% -> NIR = 4.0 mm.
    # At 1200 s/mm, 4.0 * 1200 = 4800s, but MUST BE CAPPED at 3600s (1 hour limit)
    assert snap1["net_irrigation_requirement_mm"] == 4.0
    assert snap1["recommended_duration_seconds"] == 3600

    # Decision drivers must include moisture_deficit, high_vpd, and rapid_depletion
    driver_factors = [d["factor"] for d in snap1["primary_drivers"]]
    assert "moisture_deficit" in driver_factors
    assert "high_vpd" in driver_factors
    assert "rapid_depletion" in driver_factors

    # Zone 2: no deficit, low probability -> NIR = 0.0, duration = 0
    assert snap2["net_irrigation_requirement_mm"] == 0.0
    assert snap2["recommended_duration_seconds"] == 0
    driver2_factors = [d["factor"] for d in snap2["primary_drivers"]]
    assert "recent_rain" in driver2_factors

    # Zone 3: deficit 20% -> NIR = 8.0 mm.
    # Duration must also be capped at 3600s
    assert snap3["net_irrigation_requirement_mm"] == 8.0
    assert snap3["recommended_duration_seconds"] == 3600
