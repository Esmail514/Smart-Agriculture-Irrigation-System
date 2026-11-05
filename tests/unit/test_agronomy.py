"""Unit tests for the Agronomy & FAO-56 Penman-Monteith Evapotranspiration Engine."""
import math
import pytest
from src.common.agronomy import (
    atmospheric_pressure,
    psychrometric_constant,
    saturation_vapor_pressure,
    slope_vapor_pressure_curve,
    mean_saturation_vapor_pressure,
    actual_vapor_pressure_from_rh,
    actual_vapor_pressure_from_mean_rh,
    vapor_pressure_deficit,
    extraterrestrial_radiation,
    clear_sky_radiation,
    estimate_solar_radiation,
    net_solar_radiation,
    net_longwave_radiation,
    net_radiation,
    fao56_penman_monteith_daily,
    fao56_penman_monteith_hourly,
    calculate_daily_et0,
    get_crop_kc,
    calculate_etc,
    calculate_effective_rain,
    net_irrigation_requirement,
    irrigation_volume_m3,
    CROP_KC_CATALOG,
)


def test_atmospheric_pressure_and_psychrometric_constant():
    # Sea level (0m)
    p_sea = atmospheric_pressure(0.0)
    assert math.isclose(p_sea, 101.3, rel_tol=1e-3)
    gamma_sea = psychrometric_constant(p_sea)
    # FAO-56 standard: gamma at sea level is ~0.067 kPa/°C
    assert math.isclose(gamma_sea, 0.0674, rel_tol=1e-2)

    # Elevation 1800m (e.g. Sana'a highland plateau)
    p_high = atmospheric_pressure(1800.0)
    assert p_high < p_sea
    assert math.isclose(p_high, 81.8, rel_tol=2e-2)
    gamma_high = psychrometric_constant(p_high)
    assert gamma_high < gamma_sea


def test_saturation_vapor_pressure_fao56_reference_values():
    # FAO-56 Table 2.1 reference values:
    # At 0 °C: e0(0) = 0.611 kPa
    assert math.isclose(saturation_vapor_pressure(0.0), 0.6108, rel_tol=1e-3)

    # At 20 °C: e0(20) = 2.338 kPa
    assert math.isclose(saturation_vapor_pressure(20.0), 2.338, rel_tol=1e-2)

    # At 25 °C: e0(25) = 3.167 kPa
    assert math.isclose(saturation_vapor_pressure(25.0), 3.167, rel_tol=1e-2)

    # At 35 °C: e0(35) = 5.622 kPa
    assert math.isclose(saturation_vapor_pressure(35.0), 5.622, rel_tol=1e-2)


def test_slope_vapor_pressure_curve_fao56():
    # At 20 °C, FAO-56 Table 2.2: Delta ~ 0.145 kPa/°C
    delta_20 = slope_vapor_pressure_curve(20.0)
    assert math.isclose(delta_20, 0.145, rel_tol=2e-2)

    # At 30 °C, Delta ~ 0.244 kPa/°C
    delta_30 = slope_vapor_pressure_curve(30.0)
    assert math.isclose(delta_30, 0.244, rel_tol=2e-2)


def test_mean_saturation_and_actual_vapor_pressure():
    # Daily Tmax = 30°C, Tmin = 15°C
    es = mean_saturation_vapor_pressure(30.0, 15.0)
    e0_30 = saturation_vapor_pressure(30.0)
    e0_15 = saturation_vapor_pressure(15.0)
    assert math.isclose(es, (e0_30 + e0_15) / 2.0)

    # RHmax = 80%, RHmin = 40%
    ea = actual_vapor_pressure_from_rh(30.0, 15.0, rh_max=80.0, rh_min=40.0)
    expected_ea = (e0_15 * 0.8 + e0_30 * 0.4) / 2.0
    assert math.isclose(ea, expected_ea)

    vpd = vapor_pressure_deficit(es, ea)
    assert vpd == max(0.0, es - ea)
    assert vpd > 0.0


def test_extraterrestrial_radiation_fao56():
    # For latitude 30°N (Cairo/Giza region) on day 100 (mid-April):
    # FAO-56 Table 2.6: Ra is ~36.1 - 38.1 MJ / m^2 / day
    ra_30n_100 = extraterrestrial_radiation(30.0, 100)
    assert 34.0 <= ra_30n_100 <= 38.0

    # Day of year 15 in winter for 30°N should be lower than summer (day 180)
    ra_winter = extraterrestrial_radiation(30.0, 15)
    ra_summer = extraterrestrial_radiation(30.0, 180)
    assert ra_summer > ra_winter
    # In winter (day 15), Table 2.6 is ~21.4
    assert 20.0 <= ra_winter <= 23.0


def test_radiation_components():
    ra = 34.0
    rso = clear_sky_radiation(ra, elevation_m=100.0)
    assert rso > 0.75 * ra

    rs = estimate_solar_radiation(ra, temp_max_c=32.0, temp_min_c=18.0)
    assert 0 < rs < rso

    rns = net_solar_radiation(rs)
    assert math.isclose(rns, (1 - 0.23) * rs)

    ea = 1.8
    rnl = net_longwave_radiation(32.0, 18.0, ea, rs, rso)
    assert rnl > 0

    rn = net_radiation(rns, rnl)
    assert rn < rns


def test_fao56_penman_monteith_daily_standard_example():
    # FAO-56 Chapter 4 benchmark conditions:
    # Tmean = 20.0 °C, u2 = 2.0 m/s, Rn = 12.0 MJ/m^2/day
    # es = 2.34 kPa, ea = 1.40 kPa
    et0 = fao56_penman_monteith_daily(
        temp_mean_c=20.0,
        wind_speed_2m_ms=2.0,
        net_radiation_mj=12.0,
        actual_vapor_pressure_kpa=1.40,
        saturation_vapor_pressure_kpa=2.34,
        elevation_m=0.0,
    )
    # Under these warm temperate/subtropical conditions, ET0 is typically between 3.5 and 4.5 mm/day
    assert 3.5 <= et0 <= 4.5


def test_fao56_penman_monteith_hourly():
    # Daytime hourly ET0
    et0_day = fao56_penman_monteith_hourly(
        temp_c=28.0,
        rh_pct=45.0,
        wind_speed_2m_ms=2.5,
        net_radiation_mj=2.0,
        elevation_m=100.0,
        is_daytime=True,
    )
    assert 0.2 <= et0_day <= 0.8

    # Nighttime hourly ET0 with negative or zero radiation
    et0_night = fao56_penman_monteith_hourly(
        temp_c=18.0,
        rh_pct=85.0,
        wind_speed_2m_ms=1.0,
        net_radiation_mj=-0.2,
        elevation_m=100.0,
        is_daytime=False,
    )
    assert et0_night < et0_day
    assert et0_night >= 0.0


def test_calculate_daily_et0_high_level():
    # Comprehensive high-level calculation with solar radiation
    et0 = calculate_daily_et0(
        temp_max_c=32.0,
        temp_min_c=18.0,
        rh_mean=50.0,
        wind_speed_2m_ms=2.0,
        solar_radiation_mj=22.0,
        latitude_deg=15.0,
        day_of_year=120,
        elevation_m=500.0,
    )
    assert 4.0 <= et0 <= 7.0

    # Auto-estimated solar radiation from latitude and DOY
    et0_estimated = calculate_daily_et0(
        temp_max_c=32.0,
        temp_min_c=18.0,
        rh_mean=50.0,
        wind_speed_2m_ms=2.0,
        latitude_deg=15.0,
        day_of_year=120,
    )
    assert 3.5 <= et0_estimated <= 7.5


def test_crop_kc_catalog_and_etc():
    # Required crops verification
    required_crops = ["wheat", "olive", "tomato", "date_palm", "citrus", "maize"]
    for crop in required_crops:
        assert crop in CROP_KC_CATALOG
        for stage in ("initial", "mid", "late"):
            kc = get_crop_kc(crop, stage)
            assert 0.2 <= kc <= 1.4

    # Test case sensitivity and whitespace tolerance
    assert get_crop_kc("Date Palm", "mid") == CROP_KC_CATALOG["date_palm"]["mid"]
    assert get_crop_kc("Tomato", "initial") == CROP_KC_CATALOG["tomato"]["initial"]
    assert get_crop_kc("Olive", "late") == CROP_KC_CATALOG["olive"]["late"]

    # Crop water requirement ETc = Kc * ET0
    et0 = 5.0
    etc_tomato_mid = calculate_etc(et0, "tomato", "mid")
    assert math.isclose(etc_tomato_mid, 5.0 * 1.15, rel_tol=1e-2)

    etc_wheat_ini = calculate_etc(et0, "wheat", "initial")
    assert math.isclose(etc_wheat_ini, 5.0 * 0.35, rel_tol=1e-2)

    # Custom Kc override
    etc_custom = calculate_etc(et0, "wheat", custom_kc=1.0)
    assert etc_custom == 5.0


def test_net_irrigation_requirement_and_effective_rain():
    # Rain <= 5mm produces 0 effective rain
    assert calculate_effective_rain(3.0) == 0.0
    assert calculate_effective_rain(5.0) == 0.0

    # Rain > 5mm produces (P - 5) * 0.8
    assert calculate_effective_rain(15.0) == (15.0 - 5.0) * 0.8  # 8.0 mm
    assert calculate_effective_rain(25.0) == (25.0 - 5.0) * 0.8  # 16.0 mm

    # NIR = max(0, ETc - effective_rain)
    etc = 6.0
    # No rain
    assert net_irrigation_requirement(etc, 0.0) == 6.0
    # Partial rain
    assert net_irrigation_requirement(etc, 2.5) == 3.5
    # Heavy rain exceeds ETc -> NIR is 0
    assert net_irrigation_requirement(etc, 10.0) == 0.0


def test_irrigation_volume_m3():
    # 1 mm over 1 ha = 10 m³
    # 5 mm over 2 ha = 5 * 2 * 10 = 100 m³ (at 100% efficiency)
    vol_100 = irrigation_volume_m3(nir_mm=5.0, area_hectares=2.0, irrigation_efficiency=1.0)
    assert vol_100 == 100.0

    # With drip irrigation efficiency = 0.90 (90%)
    vol_drip = irrigation_volume_m3(nir_mm=5.0, area_hectares=2.0, irrigation_efficiency=0.90)
    assert math.isclose(vol_drip, 100.0 / 0.90, rel_tol=1e-2)

    # Zero NIR or zero area
    assert irrigation_volume_m3(0.0, 10.0) == 0.0
    assert irrigation_volume_m3(5.0, 0.0) == 0.0

    # Invalid efficiency
    with pytest.raises(ValueError):
        irrigation_volume_m3(5.0, 2.0, irrigation_efficiency=0.0)
    with pytest.raises(ValueError):
        irrigation_volume_m3(5.0, 2.0, irrigation_efficiency=1.2)
