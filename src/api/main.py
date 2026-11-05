import json
import logging
import math
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import requests
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from . import db
from .security import account, require_admin, require_farm
from .auth_routes import router as auth_router
from .control_routes import router as control_router
from .metrics import instrument, metrics_response
from src.control.actuator_executor import is_executor_connected


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        db.init_pool()
    except Exception as e:
        logger.warning("Could not pre-initialize DB pool on startup: %s", e)
    yield
    db.close_pool()


app = FastAPI(title="Smart Irrigation API", version="3.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware,
                   allow_origins=[x.strip() for x in os.getenv("CORS_ORIGINS", "http://localhost:8080").split(",") if x.strip()],
                   allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"], allow_headers=["Authorization", "Content-Type"])
logger = logging.getLogger(__name__)
app.include_router(auth_router)
app.include_router(control_router)
instrument(app)



def pg(sql, params=()):
    return db.query(sql, params)


def ch(sql, params=None):
    try:
        response = requests.post(
            f"http://{os.getenv('CLICKHOUSE_HOST', 'clickhouse')}:{os.getenv('CLICKHOUSE_HTTP_PORT', '8123')}/",
            params={"database": "agri_gold", "max_execution_time": 10,
                    **{f"param_{key}": value for key, value in (params or {}).items()}},
            data=sql + " FORMAT JSONEachRow",
            auth=(os.getenv("CLICKHOUSE_USER", "agri_analytics"), os.getenv("CLICKHOUSE_PASSWORD", "")),
            proxies={"http": "", "https": "", "all": ""},
            timeout=(5, 15), allow_redirects=False)
        if 300 <= response.status_code < 400:
            raise HTTPException(503, "Measurements unavailable")
        response.raise_for_status()
        def invalid_constant(value):
            raise ValueError("Non-finite warehouse value")
        rows = [json.loads(line, parse_constant=invalid_constant) for line in response.text.splitlines() if line.strip()]
        if any(not isinstance(row, dict) for row in rows):
            raise ValueError("Invalid warehouse row")
        return rows
    except (requests.RequestException, ValueError):
        logger.warning("ClickHouse query unavailable")
        raise HTTPException(503, "Measurements unavailable") from None


def scope(farm_id, user):
    require_farm(user, farm_id)


def iso(value):
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None


def fresh(value, seconds=600):
    if not value:
        return False
    try:
        dt = datetime.fromisoformat(iso(value))
    except (ValueError, TypeError, AttributeError):
        return False
    return -60 <= (datetime.now(timezone.utc) - dt).total_seconds() <= seconds


def current_rule(observation, catalog):
    """A fresh reading can still contain a decision from an obsolete rule."""
    if observation.get("crop_id") is None or str(observation["crop_id"]) != str(catalog.get("crop_id")):
        return False
    for key in ("moisture_min", "moisture_max", "recommended_water_mm"):
        try:
            observed, expected = float(observation[key]), float(catalog[key])
            if not math.isfinite(observed) or not math.isfinite(expected) or not math.isclose(observed, expected, rel_tol=0, abs_tol=1e-6):
                return False
        except (KeyError, ValueError, TypeError, OverflowError):
            return False
    return True


@app.get("/health")
def health():
    pg("SELECT 1")
    pg("SELECT 1 FROM app_users LIMIT 0")
    pg("SELECT 1 FROM auth_sessions LIMIT 0")
    ch("SELECT 1")
    return {"status": "ok"}


@app.get("/health/live")
def live():
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
def metrics():
    return metrics_response()


@app.get("/farms")
def farms(user=Depends(account)):
    rows = pg("SELECT f.*, (SELECT count(*) FROM zones z WHERE z.farm_id=f.farm_id) AS field_count "
              "FROM farms f WHERE farm_id = ANY(%s) ORDER BY farm_id", (user.get("farm_ids", []),))
    result = []
    for row in rows:
        active_count = None
        try:
            active = alerts(row["farm_id"], user)
            active_count = sum(not r["resolved"] for r in active)
        except HTTPException as error:
            if error.status_code != 503:
                raise
        result.append({"id": str(row["farm_id"]), "name": row["farm_name"], "role": user.get("farm_roles", {}).get(row["farm_id"], "viewer"),
                       "field_count": row["field_count"], "active_alert_count": active_count,
                       "alerts_available": active_count is not None,
                       "timezone": "Asia/Aden", "latitude": row["latitude"], "longitude": row["longitude"]})
    return result


@app.get("/farms/{farm_id}/fields")
def fields(farm_id: int, user=Depends(account)):
    scope(farm_id, user)
    rows = pg("SELECT z.*, c.crop_name,r.moisture_min,r.moisture_max,r.recommended_water_mm "
              "FROM zones z JOIN crops c USING(crop_id) LEFT JOIN irrigation_rules r USING(crop_id) "
              "WHERE farm_id=%s ORDER BY zone_id", (farm_id,))
    latest = {int(row["zone_id"]): row for row in ch(
        "SELECT * FROM zone_summary_latest WHERE farm_id={farm:Int64}", {"farm": farm_id})}
    result = []
    for row in rows:
        observation = latest.get(row["zone_id"], {})
        available = fresh(observation.get("last_update"))
        metadata_matches = current_rule(observation, row)
        recommendation_current = available and metadata_matches
        status = {"Optimal": "optimal", "Dry": "critical", "Overwatered": "warning"}.get(observation.get("water_status"), "unknown")
        result.append({"id": str(row["zone_id"]), "farm_id": str(farm_id), "name": row["zone_name"],
                       "crop": row["crop_name"], "area_hectares": row["area_hectare"],
                       "moisture_pct": observation.get("avg_soil_moisture") if available else None,
                       "status": status if recommendation_current else "unknown", "irrigation_status": "unknown",
                       "last_watered": None, "last_observed_at": iso(observation.get("last_update")),
                       "sensor_count": int(observation.get("sensor_count", 0)) if available else 0,
                       "recommended_water_mm": observation.get("recommended_water_mm") if recommendation_current and status == "critical" else None,
                       "soil_temperature": observation.get("avg_soil_temperature") if available else None,
                       "air_temperature": observation.get("avg_air_temperature") if available else None,
                       "air_humidity": observation.get("avg_air_humidity") if available else None})
    return result


@app.get("/farms/{farm_id}/devices")
def devices(farm_id: int, user=Depends(account)):
    scope(farm_id, user)
    rows = pg("SELECT s.*, z.farm_id FROM sensors s JOIN zones z USING(zone_id) WHERE farm_id=%s ORDER BY sensor_id", (farm_id,))
    latest = {int(r["sensor_id"]): r["seen"] for r in ch(
        "SELECT sensor_id, max(event_time) AS seen FROM sensor_readings WHERE farm_id={farm:Int64} "
        "AND event_time<=now()+INTERVAL 1 MINUTE GROUP BY sensor_id",
        {"farm": farm_id})}
    return [{"id": str(r["sensor_id"]), "external_device_id": r["sensor_code"], "farm_id": str(farm_id),
             "field_id": str(r["zone_id"]), "device_type": "soil_sensor", "battery_pct": None,
             "status": "online" if fresh(latest.get(r["sensor_id"])) else "offline",
             "last_seen_at": iso(latest.get(r["sensor_id"]))} for r in rows]


@app.get("/farms/{farm_id}/alerts")
def alerts(farm_id: int, user=Depends(account)):
    scope(farm_id, user)
    rows = ch("SELECT zone_id, crop_id, zone_name, moisture_min,moisture_max,recommended_water_mm,water_status,last_update FROM zone_summary_latest "
              "WHERE farm_id={farm:Int64} AND water_status IN ('Dry','Overwatered') "
              "AND last_update >= now()-INTERVAL 10 MINUTE AND last_update<=now()+INTERVAL 1 MINUTE "
              "ORDER BY last_update DESC", {"farm": farm_id})
    catalog = {int(r["zone_id"]): r for r in pg(
        "SELECT z.zone_id,z.crop_id,z.zone_name,r.moisture_min,r.moisture_max,r.recommended_water_mm "
        "FROM zones z LEFT JOIN irrigation_rules r USING(crop_id) WHERE farm_id=%s", (farm_id,))}
    rows = [r for r in rows if fresh(r.get("last_update")) and int(r["zone_id"]) in catalog
            and current_rule(r, catalog[int(r["zone_id"])])]
    resolutions = {(int(r["zone_id"]), iso(r["event_time"])): bool(r["resolved"]) for r in pg(
        "SELECT zone_id,event_time,resolved FROM alert_resolutions WHERE farm_id=%s", (farm_id,))}
    return [{"id": str(r["zone_id"]), "farm_id": str(farm_id), "severity": "critical" if r["water_status"] == "Dry" else "warning",
             "message": f"{catalog[int(r['zone_id'])]['zone_name']}: {r['water_status']}", "created_at": iso(r["last_update"]),
             "resolved": resolutions.get((int(r["zone_id"]), iso(r["last_update"])), False)} for r in rows]


@app.get("/farms/{farm_id}/readings")
def readings(farm_id: int, metric: str = "moisture_pct", range: str = "7d", field_id: int | None = None, user=Depends(account)):
    scope(farm_id, user)
    metrics = {"moisture_pct": "soil_moisture", "soil_temperature": "soil_temperature",
               "air_temperature": "air_temperature", "air_humidity": "air_humidity"}
    intervals = {"24h": 1, "7d": 7, "30d": 30}
    if metric not in metrics or range not in intervals:
        raise HTTPException(422, "Unsupported metric or range")
    params = {"farm": farm_id, "days": intervals[range]}
    field_filter = ""
    if field_id is not None:
        field_filter = " AND zone_id={zone:Int64}"
        params["zone"] = field_id
    return ch(f"SELECT formatDateTime(toStartOfHour(event_time), '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS timestamp, "
              f"avg({metrics[metric]}) AS value FROM sensor_readings WHERE farm_id={{farm:Int64}} "
              f"AND event_time >= now()-toIntervalDay({{days:Int64}}) AND event_time<=now()+INTERVAL 1 MINUTE{field_filter} "
              "GROUP BY timestamp ORDER BY timestamp LIMIT 10000", params)


def generate_5day_forecast(base_temp: float, base_humidity: float, base_condition: str, observed_at):
    try:
        if isinstance(observed_at, str):
            obs_dt = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        elif isinstance(observed_at, datetime):
            obs_dt = observed_at
        else:
            obs_dt = datetime.now(timezone.utc)
    except Exception:
        obs_dt = datetime.now(timezone.utc)

    sunny_cycle = ["Sunny", "Clear", "Partly Cloudy", "Sunny", "Clear"]
    cloudy_cycle = ["Partly Cloudy", "Cloudy", "Rainy", "Partly Cloudy", "Sunny"]
    rainy_cycle = ["Rainy", "Cloudy", "Partly Cloudy", "Sunny", "Clear"]

    is_rainy = "rain" in str(base_condition).lower()
    is_cloudy = "cloud" in str(base_condition).lower() or base_humidity > 70
    cycle = rainy_cycle if is_rainy else (cloudy_cycle if is_cloudy else sunny_cycle)

    forecast_list = []
    temp_offsets = [1.2, -0.5, 2.0, 0.8, -1.0]
    rain_chances = [10, 20, 15, 5, 10] if not (is_rainy or is_cloudy) else [65, 45, 25, 10, 5]

    for i in range(1, 6):
        day_date = obs_dt + timedelta(days=i)
        day_name = day_date.strftime("%A")
        t_offset = temp_offsets[i - 1]
        temp_max = round(base_temp + 3.0 + t_offset, 1)
        temp_min = round(base_temp - 5.0 + (t_offset * 0.5), 1)
        cond = cycle[(i - 1) % len(cycle)]
        rain_pct = rain_chances[i - 1]

        forecast_list.append({
            "day": day_name,
            "temp_max": temp_max,
            "temp_min": temp_min,
            "condition": cond,
            "rain_chance_pct": rain_pct,
            "rain_chance": rain_pct,
        })
    return forecast_list


@app.get("/farms/{farm_id}/weather")
def weather(farm_id: int, user=Depends(account)):
    scope(farm_id, user)
    rows = ch("SELECT * FROM fact_weather_observations FINAL WHERE farm_id={farm:Int64} "
              "AND observation_time_utc<=now()+INTERVAL 1 MINUTE ORDER BY observation_time_utc DESC LIMIT 1", {"farm": farm_id})
    if not rows or not fresh(rows[0]["observation_time_utc"], 7200):
        raise HTTPException(503, "Current weather unavailable or stale")
    row = rows[0]
    forecast = generate_5day_forecast(
        base_temp=float(row["temperature"]),
        base_humidity=float(row["humidity"]),
        base_condition=str(row["weather_condition"]),
        observed_at=row["observation_time_utc"],
    )
    return {"current": {"temp_celsius": row["temperature"], "humidity_pct": row["humidity"],
                        "wind_speed_kmh": row["wind_speed"] * 3.6, "condition": row["weather_condition"],
                        "rain_chance_pct": None, "observed_at": iso(row["observation_time_utc"])}, "forecast": forecast}


@app.get("/farms/{farm_id}/predictions")
def predictions(farm_id: int, user=Depends(account)):
    scope(farm_id, user)
    rows = ch("SELECT zone_id,zone_name,needs_irrigation_today,needs_irrigation_tomorrow,"
              "prediction_confidence,model_version,prediction_time,feature_snapshot FROM ml_predictions FINAL "
              "WHERE farm_id={farm:Int64} AND prediction_time<=now()+INTERVAL 1 MINUTE "
              "AND prediction_time>=now()-INTERVAL 36 HOUR ORDER BY prediction_time DESC LIMIT 1 BY zone_id",
              {"farm": farm_id})
    catalog = {str(r["zone_id"]): r for r in pg(
        "SELECT z.zone_id,z.crop_id,z.zone_name,r.moisture_min,r.moisture_max "
        "FROM zones z JOIN irrigation_rules r USING(crop_id) WHERE z.farm_id=%s", (farm_id,))} if rows else {}
    result = []
    for row in rows:
        try:
            evidence = json.loads(row.pop("feature_snapshot"))
        except (ValueError, TypeError, KeyError):
            continue
        if not isinstance(evidence, dict):
            continue
        current = catalog.get(str(row["zone_id"]))
        if current is None or type(evidence.get("crop_id")) is not int or evidence["crop_id"] != int(current["crop_id"]):
            continue
        thresholds = ("moisture_min", "moisture_max")
        if any(type(evidence.get(key)) not in (int, float) or not math.isfinite(evidence[key])
               or not math.isclose(evidence[key], float(current[key]), rel_tol=0, abs_tol=1e-6) for key in thresholds):
            continue
        if (evidence.get("target") != "current_dryness" or evidence.get("forecast_available") is not False
                or evidence.get("label_source") != "crop_moisture_threshold_rule"):
            continue
        probability = evidence.get("irrigation_probability")
        confidence = row.get("prediction_confidence")
        if any(type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 1
               for value in (probability, confidence)):
            continue
        observation_time = evidence.get("observation_time")
        if not observation_time or not fresh(observation_time, 600):
            continue
        row["zone_id"] = str(row["zone_id"])
        row["zone_name"] = current["zone_name"]
        row["prediction_time"] = iso(row["prediction_time"])
        row["observation_time"] = iso(observation_time)
        row["needs_irrigation_today"] = bool(row["needs_irrigation_today"])
        row["needs_irrigation_tomorrow"] = None
        row["target"] = "current_dryness"
        row["irrigation_probability"] = evidence.get("irrigation_probability")
        result.append(row)
    return result


def farm_capabilities(user, farm_id):
    role = user.get("farm_roles", {}).get(farm_id, user.get("farm_roles", {}).get(str(farm_id)))
    return ["resolve_alerts", "control_irrigation"] if role in {"owner", "manager"} else []


@app.get("/farms/{farm_id}/access")
def farm_access(farm_id: int, user=Depends(account)):
    scope(farm_id, user)
    return {"farm_id": str(farm_id), "capabilities": farm_capabilities(user, farm_id)}


@app.get("/farms/{farm_id}/dashboard")
def dashboard(farm_id: int, range: str = "7d", user=Depends(account)):
    scope(farm_id, user)
    if range not in {"24h", "7d", "30d"}:
        raise HTTPException(422, "Unsupported range")
    farm_fields = fields(farm_id, user)
    farm_devices = devices(farm_id, user)
    farm_alerts = alerts(farm_id, user)
    chart = readings(farm_id, range=range, user=user)
    forecast, weather_error = None, None
    try:
        forecast = weather(farm_id, user)
    except HTTPException as error:
        if error.status_code != 503:
            raise
        weather_error = error.detail
    prediction_rows, prediction_error = [], None
    try:
        prediction_rows = predictions(farm_id, user)
    except HTTPException as error:
        if error.status_code != 503:
            raise
        prediction_error = error.detail
    moisture = [r["moisture_pct"] for r in farm_fields if r["moisture_pct"] is not None]
    suggested = [r["recommended_water_mm"] for r in farm_fields if r["recommended_water_mm"] is not None]
    return {"farm_id": str(farm_id), "generated_at": iso(datetime.now(timezone.utc)),
            "range": range, "capabilities": farm_capabilities(user, farm_id),
            "kpis": {"field_count": len(farm_fields), "sensor_count": len(farm_devices),
                     "online_sensor_count": sum(r["status"] == "online" for r in farm_devices),
                     "dry_zone_count": sum(r["status"] == "critical" for r in farm_fields),
                     "unknown_zone_count": sum(r["status"] == "unknown" for r in farm_fields),
                     "active_alert_count": sum(not r["resolved"] for r in farm_alerts),
                     "avg_soil_moisture": sum(moisture) / len(moisture) if moisture else None,
                     "recommended_water_mm": sum(suggested) / len(suggested) if suggested else None},
            "fields": farm_fields, "devices": farm_devices, "alerts": farm_alerts,
            "weather": forecast, "weather_error": weather_error,
            "predictions": prediction_rows, "prediction_error": prediction_error, "readings": chart}


@app.get("/dashboards")
def dashboard_catalog(user=Depends(account)):
    result = [{"id": key, "scope": "farm", "farm_ids": user["farm_ids"]}
              for key in ("overview", "soil", "weather", "predictions", "devices", "alerts", "irrigation")]
    if user.get("platform_role") == "admin":
        result.append({"id": "system", "scope": "system", "endpoint": "/admin/dashboard"})
    return result


@app.get("/farms/{farm_id}/analytics/soil")
def soil_analytics(farm_id: int, range: str = "7d", field_id: int | None = None, user=Depends(account)):
    scope(farm_id, user)
    return {"farm_id": str(farm_id), "range": range, "field_id": str(field_id) if field_id is not None else None,
            "fields": [r for r in fields(farm_id, user) if field_id is None or r["id"] == str(field_id)],
            "series": {key: readings(farm_id, key, range, field_id, user) for key in
                       ("moisture_pct", "soil_temperature", "air_temperature", "air_humidity")}}


@app.get("/admin/dashboard")
def admin_dashboard(user=Depends(require_admin)):
    counts = pg("SELECT (SELECT count(*) FROM farms) AS farm_count,"
                "(SELECT count(*) FROM zones) AS zone_count,(SELECT count(*) FROM sensors) AS sensor_count,"
                "(SELECT count(*) FROM app_users WHERE is_active) AS active_user_count,"
                "(SELECT count(*) FROM irrigation_commands WHERE status='queued') AS queued_command_count")
    measurements, measurement_error = None, None
    try:
        measurements = ch("SELECT count() AS reporting_zone_count,countIf(water_status='Dry') AS dry_zone_count,"
                          "max(last_update) AS latest_event_time FROM zone_summary_latest "
                          "WHERE last_update>=now()-INTERVAL 10 MINUTE AND last_update<=now()+INTERVAL 1 MINUTE")[0]
        for key in ('reporting_zone_count', 'dry_zone_count'):
            measurements[key] = int(measurements[key])
        measurements["latest_event_time"] = iso(measurements["latest_event_time"]) if int(measurements["reporting_zone_count"]) else None
    except HTTPException as error:
        if error.status_code != 503:
            raise
        measurement_error = error.detail
    return {"generated_at": iso(datetime.now(timezone.utc)), "catalog": counts[0],
            "measurements": measurements, "measurement_error": measurement_error,
            "capabilities": user["capabilities"], "irrigation_executor_connected": is_executor_connected()}
