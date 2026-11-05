"""Agronomy & FAO-56 Penman-Monteith Evapotranspiration Engine.

Implements FAO Irrigation and Drainage Paper No. 56:
- Reference evapotranspiration (ET0) using the Penman-Monteith equation (daily & hourly).
- Crop evapotranspiration under standard conditions: ETc = Kc * ET0.
- Catalog of crop Kc coefficients across growth stages (Initial, Mid, Late).
- Dynamic Net Irrigation Requirement (NIR = max(0, ETc - effective_rain)).
- Volumetric irrigation water calculations in cubic meters (m³) based on field area in hectares.
"""
import math
from typing import Literal

# Solar Constant G_sc in MJ * m^-2 * min^-1
G_SC = 0.0820

# Stefan-Boltzmann constant in MJ * K^-4 * m^-2 * day^-1
STEFAN_BOLTZMANN = 4.903e-9

# Reference grass albedo
ALBEDO_GRASS = 0.23

# FAO-56 Crop Kc Catalog across growth stages: Initial, Mid, Late
# Values sourced from FAO Irrigation and Drainage Paper No. 56, Table 12
CROP_KC_CATALOG: dict[str, dict[str, float]] = {
    "wheat": {"initial": 0.35, "mid": 1.15, "late": 0.25},
    "olive": {"initial": 0.65, "mid": 0.70, "late": 0.65},
    "tomato": {"initial": 0.60, "mid": 1.15, "late": 0.80},
    "date_palm": {"initial": 0.90, "mid": 0.95, "late": 0.95},
    "citrus": {"initial": 0.70, "mid": 0.65, "late": 0.70},
    "maize": {"initial": 0.30, "mid": 1.20, "late": 0.35},
    "corn": {"initial": 0.30, "mid": 1.20, "late": 0.35},
    "potato": {"initial": 0.50, "mid": 1.15, "late": 0.75},
    "alfalfa": {"initial": 0.40, "mid": 1.20, "late": 1.15},
    "grape": {"initial": 0.30, "mid": 0.85, "late": 0.45},
    "barley": {"initial": 0.30, "mid": 1.15, "late": 0.25},
    "cotton": {"initial": 0.35, "mid": 1.20, "late": 0.50},
    "onion": {"initial": 0.70, "mid": 1.05, "late": 0.75},
}


def atmospheric_pressure(elevation_m: float = 0.0) -> float:
    """Atmospheric pressure P (kPa) at elevation z (m) above sea level (FAO-56 eq. 7)."""
    return 101.3 * ((293.0 - 0.0065 * elevation_m) / 293.0) ** 5.26


def psychrometric_constant(pressure_kpa: float) -> float:
    """Psychrometric constant gamma (kPa / °C) from atmospheric pressure (FAO-56 eq. 8)."""
    return 0.000665 * pressure_kpa


def saturation_vapor_pressure(temp_c: float) -> float:
    """Saturation vapor pressure e0(T) (kPa) at air temperature T (°C) (FAO-56 eq. 11)."""
    return 0.6108 * math.exp((17.27 * temp_c) / (temp_c + 237.3))


def slope_vapor_pressure_curve(temp_c: float) -> float:
    """Slope of saturation vapor pressure curve Delta (kPa / °C) at temperature T (°C) (FAO-56 eq. 13)."""
    e0 = saturation_vapor_pressure(temp_c)
    return (4098.0 * e0) / ((temp_c + 237.3) ** 2)


def mean_saturation_vapor_pressure(temp_max_c: float, temp_min_c: float) -> float:
    """Mean saturation vapor pressure es (kPa) from daily Tmax and Tmin (FAO-56 eq. 12)."""
    return (saturation_vapor_pressure(temp_max_c) + saturation_vapor_pressure(temp_min_c)) / 2.0


def actual_vapor_pressure_from_rh(
    temp_max_c: float, temp_min_c: float, rh_max: float, rh_min: float
) -> float:
    """Actual vapor pressure ea (kPa) from RHmax (%) and RHmin (%) (FAO-56 eq. 17)."""
    e0_tmin = saturation_vapor_pressure(temp_min_c)
    e0_tmax = saturation_vapor_pressure(temp_max_c)
    return (e0_tmin * (rh_max / 100.0) + e0_tmax * (rh_min / 100.0)) / 2.0


def actual_vapor_pressure_from_mean_rh(temp_c: float, rh_mean: float) -> float:
    """Actual vapor pressure ea (kPa) from mean relative humidity and temperature (FAO-56 eq. 19)."""
    return (rh_mean / 100.0) * saturation_vapor_pressure(temp_c)


def vapor_pressure_deficit(es: float, ea: float) -> float:
    """Vapor pressure deficit (es - ea) in kPa."""
    return max(0.0, es - ea)


def extraterrestrial_radiation(latitude_deg: float, day_of_year: int) -> float:
    """Extraterrestrial radiation Ra (MJ * m^-2 * day^-1) for daily periods (FAO-56 eq. 21).

    Args:
        latitude_deg: Latitude in decimal degrees (positive for Northern hemisphere, negative for Southern).
        day_of_year: Julian day of year J (1 to 365 or 366).
    """
    phi = math.radians(latitude_deg)
    # Inverse relative distance Earth-Sun (eq. 23)
    dr = 1.0 + 0.033 * math.cos(2.0 * math.pi * day_of_year / 365.0)
    # Solar declination delta (eq. 24)
    delta = 0.409 * math.sin((2.0 * math.pi * day_of_year / 365.0) - 1.39)
    # Sunset hour angle omega_s (eq. 25)
    cos_omega_s = -math.tan(phi) * math.tan(delta)
    # Clamp to [-1.0, 1.0] for polar daylight or polar night
    cos_omega_s = max(-1.0, min(1.0, cos_omega_s))
    omega_s = math.acos(cos_omega_s)

    # Ra in MJ * m^-2 * day^-1
    term1 = omega_s * math.sin(phi) * math.sin(delta)
    term2 = math.cos(phi) * math.cos(delta) * math.sin(omega_s)
    ra = (24.0 * 60.0 / math.pi) * G_SC * dr * (term1 + term2)
    return max(0.0, ra)


def clear_sky_radiation(ra: float, elevation_m: float = 0.0) -> float:
    """Clear-sky solar radiation Rso (MJ * m^-2 * day^-1) (FAO-56 eq. 37)."""
    return (0.75 + 2e-5 * elevation_m) * ra


def estimate_solar_radiation(
    ra: float, temp_max_c: float, temp_min_c: float, coastal: bool = False
) -> float:
    """Hargreaves radiation formula for solar radiation Rs from temperature range (FAO-56 eq. 50)."""
    k_rs = 0.19 if coastal else 0.16
    temp_diff = max(0.0, temp_max_c - temp_min_c)
    return k_rs * math.sqrt(temp_diff) * ra


def net_solar_radiation(rs: float, albedo: float = ALBEDO_GRASS) -> float:
    """Net solar (shortwave) radiation Rns (MJ * m^-2 * day^-1) (FAO-56 eq. 38)."""
    return (1.0 - albedo) * rs


def net_longwave_radiation(
    temp_max_c: float,
    temp_min_c: float,
    ea: float,
    rs: float,
    rso: float,
) -> float:
    """Net longwave radiation Rnl (MJ * m^-2 * day^-1) (FAO-56 eq. 39)."""
    t_max_k = temp_max_c + 273.16
    t_min_k = temp_min_c + 273.16
    temp_factor = STEFAN_BOLTZMANN * ((t_max_k**4 + t_min_k**4) / 2.0)
    humidity_factor = 0.34 - 0.14 * math.sqrt(max(0.0, ea))
    rs_ratio = rs / rso if rso > 0 else 0.7
    rs_ratio = max(0.0, min(1.0, rs_ratio))
    cloud_factor = 1.35 * rs_ratio - 0.35
    cloud_factor = max(0.05, cloud_factor)
    return temp_factor * humidity_factor * cloud_factor


def net_radiation(rns: float, rnl: float) -> float:
    """Net radiation Rn = Rns - Rnl (MJ * m^-2 * day^-1) (FAO-56 eq. 40)."""
    return rns - rnl


def fao56_penman_monteith_daily(
    temp_mean_c: float,
    wind_speed_2m_ms: float,
    net_radiation_mj: float,
    actual_vapor_pressure_kpa: float,
    saturation_vapor_pressure_kpa: float,
    elevation_m: float = 0.0,
    soil_heat_flux_mj: float = 0.0,
) -> float:
    """Calculate daily reference evapotranspiration ET0 (mm/day) using FAO-56 Penman-Monteith (eq. 6).

    Args:
        temp_mean_c: Mean daily air temperature at 2 m height (°C).
        wind_speed_2m_ms: Wind speed at 2 m height (m/s).
        net_radiation_mj: Net radiation at crop surface (MJ * m^-2 * day^-1).
        actual_vapor_pressure_kpa: Actual vapor pressure ea (kPa).
        saturation_vapor_pressure_kpa: Saturation vapor pressure es (kPa).
        elevation_m: Station elevation above sea level (m).
        soil_heat_flux_mj: Soil heat flux density G (MJ * m^-2 * day^-1), ~0 for daily.

    Returns:
        ET0 in mm/day (clamped to non-negative).
    """
    p = atmospheric_pressure(elevation_m)
    gamma = psychrometric_constant(p)
    delta = slope_vapor_pressure_curve(temp_mean_c)
    vpd = vapor_pressure_deficit(saturation_vapor_pressure_kpa, actual_vapor_pressure_kpa)
    u2 = max(0.0, wind_speed_2m_ms)

    rn_g = net_radiation_mj - soil_heat_flux_mj
    radiation_term = 0.408 * delta * rn_g
    aerodynamic_term = gamma * (900.0 / (temp_mean_c + 273.0)) * u2 * vpd
    denominator = delta + gamma * (1.0 + 0.34 * u2)

    if denominator <= 0:
        return 0.0
    et0 = (radiation_term + aerodynamic_term) / denominator
    return max(0.0, round(et0, 2))


def fao56_penman_monteith_hourly(
    temp_c: float,
    rh_pct: float,
    wind_speed_2m_ms: float,
    net_radiation_mj: float,
    elevation_m: float = 0.0,
    is_daytime: bool = True,
) -> float:
    """Calculate hourly reference evapotranspiration ET0 (mm/hour) using FAO-56 Penman-Monteith (eq. 53).

    Args:
        temp_c: Mean hourly air temperature (°C).
        rh_pct: Mean hourly relative humidity (%).
        wind_speed_2m_ms: Wind speed at 2 m height (m/s).
        net_radiation_mj: Net radiation during the hour (MJ * m^-2 * hour^-1).
        elevation_m: Elevation above sea level (m).
        is_daytime: True for daytime (G = 0.1 * Rn), False for nighttime (G = 0.5 * Rn).

    Returns:
        ET0 in mm/hour.
    """
    p = atmospheric_pressure(elevation_m)
    gamma = psychrometric_constant(p)
    delta = slope_vapor_pressure_curve(temp_c)
    es = saturation_vapor_pressure(temp_c)
    ea = (rh_pct / 100.0) * es
    vpd = vapor_pressure_deficit(es, ea)
    u2 = max(0.0, wind_speed_2m_ms)

    # Soil heat flux G for hourly periods (eq. 45 & 46)
    g = 0.1 * net_radiation_mj if is_daytime else 0.5 * net_radiation_mj
    rn_g = net_radiation_mj - g

    radiation_term = 0.408 * delta * rn_g
    aerodynamic_term = gamma * (37.0 / (temp_c + 273.0)) * u2 * vpd
    denominator = delta + gamma * (1.0 + 0.34 * u2)

    if denominator <= 0:
        return 0.0
    et0 = (radiation_term + aerodynamic_term) / denominator
    return max(0.0, round(et0, 3))


def calculate_daily_et0(
    temp_max_c: float,
    temp_min_c: float,
    rh_mean: float,
    wind_speed_2m_ms: float,
    solar_radiation_mj: float | None = None,
    latitude_deg: float | None = None,
    day_of_year: int | None = None,
    elevation_m: float = 0.0,
    net_radiation_mj: float | None = None,
) -> float:
    """High-level daily reference evapotranspiration (ET0) calculator.

    Estimates radiation components automatically when not directly provided.
    """
    temp_mean = (temp_max_c + temp_min_c) / 2.0
    es = mean_saturation_vapor_pressure(temp_max_c, temp_min_c)
    ea = actual_vapor_pressure_from_mean_rh(temp_mean, rh_mean)

    if net_radiation_mj is None:
        if solar_radiation_mj is None:
            if latitude_deg is None or day_of_year is None:
                raise ValueError("Must provide either net_radiation_mj, solar_radiation_mj, or (latitude_deg and day_of_year)")
            ra = extraterrestrial_radiation(latitude_deg, day_of_year)
            rso = clear_sky_radiation(ra, elevation_m)
            rs = estimate_solar_radiation(ra, temp_max_c, temp_min_c)
        else:
            rs = solar_radiation_mj
            if latitude_deg is not None and day_of_year is not None:
                ra = extraterrestrial_radiation(latitude_deg, day_of_year)
                rso = clear_sky_radiation(ra, elevation_m)
            else:
                rso = rs / 0.75

        rns = net_solar_radiation(rs)
        rnl = net_longwave_radiation(temp_max_c, temp_min_c, ea, rs, rso)
        rn = net_radiation(rns, rnl)
    else:
        rn = net_radiation_mj

    return fao56_penman_monteith_daily(
        temp_mean_c=temp_mean,
        wind_speed_2m_ms=wind_speed_2m_ms,
        net_radiation_mj=rn,
        actual_vapor_pressure_kpa=ea,
        saturation_vapor_pressure_kpa=es,
        elevation_m=elevation_m,
    )


def normalize_crop_name(crop_name: str) -> str:
    """Normalize crop name for catalog lookup."""
    clean = crop_name.strip().lower().replace(" ", "_").replace("-", "_")
    return clean


def get_crop_kc(
    crop_name: str,
    growth_stage: Literal["initial", "mid", "late", "ini", "end"] = "mid",
) -> float:
    """Get FAO-56 crop coefficient Kc from catalog.

    Args:
        crop_name: Name of crop (e.g. 'Wheat', 'Olive', 'Tomato', 'Date Palm', 'Citrus', 'Maize').
        growth_stage: Crop growth stage ('initial', 'mid', 'late').

    Returns:
        Kc coefficient value.
    """
    key = normalize_crop_name(crop_name)
    stage_key = growth_stage.lower()
    if stage_key in ("ini", "start"):
        stage_key = "initial"
    elif stage_key in ("end",):
        stage_key = "late"

    crop_data = CROP_KC_CATALOG.get(key)
    if not crop_data:
        # Default fallback for unknown crops
        return 1.0

    return crop_data.get(stage_key, crop_data.get("mid", 1.0))


def calculate_etc(
    et0: float,
    crop_name: str,
    growth_stage: Literal["initial", "mid", "late"] = "mid",
    custom_kc: float | None = None,
) -> float:
    """Calculate crop water requirement ETc = Kc * ET0 (mm/day).

    Args:
        et0: Reference evapotranspiration in mm.
        crop_name: Crop name.
        growth_stage: Growth stage ('initial', 'mid', 'late').
        custom_kc: Optional override for Kc coefficient.

    Returns:
        Crop evapotranspiration ETc in mm/day.
    """
    kc = custom_kc if custom_kc is not None else get_crop_kc(crop_name, growth_stage)
    etc = kc * max(0.0, et0)
    return round(etc, 2)


def calculate_effective_rain(total_rain_mm: float, method: str = "fao") -> float:
    """Calculate effective rainfall from total rainfall (mm).

    FAO USDA-SCS standard daily estimate:
    - Rain <= 5.0 mm is lost to canopy interception and surface evaporation.
    - Rain > 5.0 mm has ~80% effective infiltration into the root zone.
    """
    if total_rain_mm <= 5.0:
        return 0.0
    effective = (total_rain_mm - 5.0) * 0.8
    return round(max(0.0, effective), 2)


def net_irrigation_requirement(etc_mm: float, effective_rain_mm: float = 0.0) -> float:
    """Calculate dynamic Net Irrigation Requirement (NIR).

    NIR = max(0, ETc - effective_rain) in mm.
    """
    nir = max(0.0, etc_mm - effective_rain_mm)
    return round(nir, 2)


def irrigation_volume_m3(
    nir_mm: float,
    area_hectares: float,
    irrigation_efficiency: float = 1.0,
) -> float:
    """Calculate irrigation water volume in cubic meters (m³).

    1 mm of water depth over 1 hectare (10,000 m²) equals 10 m³ of volume.
    Volume = (NIR_mm * area_ha * 10.0) / efficiency.

    Args:
        nir_mm: Net irrigation requirement in mm.
        area_hectares: Field area in hectares (ha).
        irrigation_efficiency: System application efficiency (0.0 < eta <= 1.0).
            Drip: ~0.90, Sprinkler: ~0.75-0.80, Surface: ~0.60.

    Returns:
        Volume in cubic meters (m³).
    """
    if area_hectares <= 0 or nir_mm <= 0:
        return 0.0
    if not 0.0 < irrigation_efficiency <= 1.0:
        raise ValueError("Irrigation efficiency must be in range (0.0, 1.0]")
    
    # 1 mm * 1 ha = 10 m^3
    gross_nir = nir_mm / irrigation_efficiency
    volume = gross_nir * area_hectares * 10.0
    return round(volume, 2)
