"""Transactional catalog management, farm commands and private-agent operations."""
import json
import logging
import math
import uuid
from datetime import datetime, timezone
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator, model_validator
from . import db
from .security import current_account, require_admin, require_farm, hash_password
from .control_client import agent_configuration, call_agent, validate_service_action, validate_dag

router = APIRouter()
logger = logging.getLogger(__name__)
USER_COLUMNS = "id,email,full_name,platform_role,is_active,created_at,updated_at"
Password = Annotated[str, StringConstraints(min_length=12, max_length=1024, strip_whitespace=False)]
SETTING_APPLICATION = {
    "simulator.interval_seconds": {"mode": "consumer_refresh", "refresh_seconds": 30},
    "simulator.anomaly_rate": {"mode": "consumer_refresh", "refresh_seconds": 30},
    "simulator.enabled": {"mode": "consumer_refresh", "refresh_seconds": 30},
    "irrigation.max_duration_seconds": {"mode": "new_commands"},
}


class Input(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, allow_inf_nan=False)

    @field_validator("*", mode="before")
    @classmethod
    def strict_numeric_fields(cls, value, info):
        fields = {"latitude", "longitude", "total_area_hectare", "area_hectare", "moisture_min", "moisture_max",
                  "temperature_min", "temperature_max", "recommended_water_mm"}
        if value is not None and info.field_name in fields and type(value) not in {int, float}:
            raise ValueError("Numeric fields require JSON numbers")
        return value


class UserCreate(Input):
    email: str = Field(min_length=3, max_length=254)
    password: Password
    full_name: str = Field(min_length=1, max_length=150)
    platform_role: Literal["admin", "user"] = "user"
    is_active: bool = Field(default=True, strict=True)


class UserPatch(Input):
    email: str | None = Field(default=None, min_length=3, max_length=254)
    password: Password | None = None
    full_name: str | None = Field(default=None, min_length=1, max_length=150)
    platform_role: Literal["admin", "user"] | None = None
    is_active: bool | None = Field(default=None, strict=True)


class Membership(Input):
    role: Literal["owner", "manager", "viewer"]


class FarmCreate(Input):
    farm_name: str = Field(min_length=1, max_length=100)
    city: str | None = Field(default=None, max_length=100)
    district: str | None = Field(default=None, max_length=100)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    total_area_hectare: float = Field(gt=0, le=1_000_000)


class FarmPatch(Input):
    farm_name: str | None = Field(default=None, min_length=1, max_length=100)
    city: str | None = Field(default=None, max_length=100)
    district: str | None = Field(default=None, max_length=100)
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)
    total_area_hectare: float | None = Field(default=None, gt=0, le=1_000_000)


class ZoneCreate(Input):
    crop_id: int = Field(gt=0, strict=True)
    zone_name: str = Field(min_length=1, max_length=100)
    area_hectare: float = Field(gt=0, le=1_000_000)
    soil_type: Literal["Loam", "Sandy", "Clay", "Silty"] = "Loam"


class ZonePatch(Input):
    crop_id: int | None = Field(default=None, gt=0, strict=True)
    zone_name: str | None = Field(default=None, min_length=1, max_length=100)
    area_hectare: float | None = Field(default=None, gt=0, le=1_000_000)
    soil_type: Literal["Loam", "Sandy", "Clay", "Silty"] | None = None


class SensorCreate(Input):
    zone_id: int = Field(gt=0, strict=True)
    sensor_code: str = Field(min_length=1, max_length=100)


class SensorPatch(Input):
    zone_id: int | None = Field(default=None, gt=0, strict=True)
    sensor_code: str | None = Field(default=None, min_length=1, max_length=100)


class DeviceCreate(Input):
    device_id: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_-]+$")
    zone_id: int | None = Field(default=None, gt=0, strict=True)
    name: str = Field(min_length=1, max_length=100)
    enabled: bool = Field(default=True, strict=True)


class DevicePatch(Input):
    zone_id: int | None = Field(default=None, gt=0, strict=True)
    name: str | None = Field(default=None, min_length=1, max_length=100)
    enabled: bool | None = Field(default=None, strict=True)


class RuleInput(Input):
    crop_id: int = Field(gt=0, strict=True)
    moisture_min: float = Field(ge=0, le=100)
    moisture_max: float = Field(ge=0, le=100)
    temperature_min: float = Field(ge=-40, le=85)
    temperature_max: float = Field(ge=-40, le=85)
    recommended_water_mm: float = Field(ge=0, le=200)

    @model_validator(mode="after")
    def ordered_ranges(self):
        if self.moisture_min >= self.moisture_max or self.temperature_min >= self.temperature_max:
            raise ValueError("Minimum thresholds must be below maximum thresholds")
        return self


class SettingsInput(Input):
    values: dict[str, object]

    @model_validator(mode="after")
    def supported_settings(self):
        bounds = {"simulator.interval_seconds": (0.1, 3600), "simulator.anomaly_rate": (0, 1),
                  "irrigation.max_duration_seconds": (1, 3600)}
        if not self.values:
            raise ValueError("No settings supplied")
        for key, value in self.values.items():
            if key == "simulator.enabled":
                if type(value) is not bool:
                    raise ValueError("simulator.enabled requires a boolean")
            elif key in bounds:
                if type(value) not in {int, float} or not math.isfinite(value) or not bounds[key][0] <= value <= bounds[key][1]:
                    raise ValueError(f"Invalid numeric setting: {key}")
                if key == "irrigation.max_duration_seconds" and type(value) is not int:
                    raise ValueError(f"Setting requires an integer: {key}")
            else:
                raise ValueError(f"Unsupported setting: {key}")
        return self


class PipelineState(Input):
    is_paused: bool = Field(strict=True)


class IrrigationCommand(Input):
    device_id: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_-]+$")
    duration_seconds: int = Field(ge=1, le=3600, strict=True)
    idempotency_key: str = Field(min_length=8, max_length=120, pattern=r"^[A-Za-z0-9_-]+$")


class Resolution(Input):
    resolved: bool = Field(strict=True)
    event_time: datetime

    @model_validator(mode="after")
    def utc_time(self):
        if self.resolved is not True:
            raise ValueError("Only resolving an alert is supported")
        if self.event_time.tzinfo is None:
            raise ValueError("event_time requires a timezone")
        self.event_time = self.event_time.astimezone(timezone.utc)
        return self


def exists(conn, table, key, value, extra_sql="", extra=()):
    # All identifiers are literals chosen by this module, never client input.
    rows = db.query(f"SELECT * FROM {table} WHERE {key}=%s {extra_sql}", (value, *extra), conn=conn)
    if not rows:
        raise HTTPException(404, "Requested record not found")
    return rows[0]


def patch_values(payload, nullable=()):
    values = payload.model_dump(exclude_unset=True)
    if not values or any(value is None and key not in nullable for key, value in values.items()):
        raise HTTPException(422, "Patch requires non-null changed fields")
    return values


def update(conn, table, key, identifier, values, returning="*"):
    assignments = ",".join(f"{field}=%s" for field in values)
    return db.query(f"UPDATE {table} SET {assignments} WHERE {key}=%s RETURNING {returning}",
                    (*values.values(), identifier), conn=conn)[0]


def audit(conn, user, action, target_type, target_id, farm_id=None, details=None):
    return db.audit(conn, user["id"], action, farm_id=farm_id, target_type=target_type,
                    target_id=str(target_id), details=details or {})


def valid_email(email):
    value = email.lower()
    if value.count("@") != 1 or not all(value.split("@")) or "." not in value.split("@")[1] or any(character.isspace() for character in value):
        raise HTTPException(422, "Invalid email address")
    return value


def external_operation(user, action, target_type, target_id, method, path, payload=None):
    """Commit intent first: restarting PostgreSQL must not destroy its own audit."""
    agent_configuration()
    operation_id = str(uuid.uuid4())
    with db.connect() as conn:
        audit(conn, user, action + ".requested", target_type, target_id,
              details={"operation_id": operation_id, "payload": payload or {}})
    try:
        result = call_agent(method, path, payload)
    except HTTPException as error:
        try:
            with db.connect() as conn:
                audit(conn, user, action + ".failed", target_type, target_id,
                      details={"operation_id": operation_id, "outcome": "unconfirmed", "http_status": error.status_code})
        except HTTPException:
            logger.warning("External operation outcome audit unavailable operation_id=%s", operation_id)
        raise
    recorded = True
    try:
        with db.connect() as conn:
            audit(conn, user, action + ".completed", target_type, target_id,
                  details={"operation_id": operation_id, "result": result})
    except HTTPException:
        recorded = False
        logger.warning("External operation outcome audit unavailable operation_id=%s", operation_id)
    if isinstance(result, dict):
        return {**result, "operation_id": operation_id, "outcome_audit_recorded": recorded}
    return result


@router.get("/admin/users")
def users(user=Depends(require_admin), limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    return db.query(f"SELECT {USER_COLUMNS} FROM app_users ORDER BY id LIMIT %s OFFSET %s", (limit, offset))


@router.post("/admin/users", status_code=201)
def create_user(payload: UserCreate, user=Depends(require_admin)):
    with db.connect() as conn:
        rows = db.query(f"INSERT INTO app_users(email,full_name,password_hash,platform_role,is_active) VALUES(%s,%s,%s,%s,%s) RETURNING {USER_COLUMNS}",
            (valid_email(payload.email), payload.full_name, hash_password(payload.password), payload.platform_role, payload.is_active), conn=conn)
        audit(conn, user, "user.created", "user", rows[0]["id"])
        return rows[0]


@router.patch("/admin/users/{user_id}")
def change_user(user_id: int, payload: UserPatch, user=Depends(require_admin)):
    values = patch_values(payload)
    if str(user_id) == str(user["id"]) and (values.get("platform_role") == "user" or values.get("is_active") is False):
        raise HTTPException(409, "Administrators cannot disable or demote themselves")
    if "email" in values:
        values["email"] = valid_email(values["email"])
    if "password" in values:
        values["password_hash"] = hash_password(values.pop("password"))
    with db.connect() as conn:
        admins = db.query("SELECT id FROM app_users WHERE platform_role='admin' AND is_active ORDER BY id FOR UPDATE", conn=conn)
        existing = exists(conn, "app_users", "id", user_id)
        if existing["platform_role"] == "admin" and existing["is_active"] and len(admins) <= 1 and (values.get("platform_role") == "user" or values.get("is_active") is False):
            raise HTTPException(409, "At least one active administrator is required")
        values["updated_at"] = datetime.now(timezone.utc)
        row = update(conn, "app_users", "id", user_id, values, USER_COLUMNS)
        if "password_hash" in values or values.get("is_active") is False:
            db.query("UPDATE auth_sessions SET revoked_at=now() WHERE user_id=%s AND revoked_at IS NULL RETURNING id", (user_id,), conn=conn)
        audit(conn, user, "user.updated", "user", user_id, details={"fields": [key for key in values if key != "password_hash"], "password_changed": "password_hash" in values})
        return row


@router.get("/admin/users/{user_id}/farms")
def memberships(user_id: int, user=Depends(require_admin)):
    return db.query("SELECT fm.*,f.farm_name FROM farm_memberships fm JOIN farms f USING(farm_id) WHERE user_id=%s ORDER BY farm_id", (user_id,))


@router.put("/admin/users/{user_id}/farms/{farm_id}")
def set_membership(user_id: int, farm_id: int, payload: Membership, user=Depends(require_admin)):
    with db.connect() as conn:
        exists(conn, "app_users", "id", user_id)
        exists(conn, "farms", "farm_id", farm_id)
        row = db.query("INSERT INTO farm_memberships(user_id,farm_id,role) VALUES(%s,%s,%s) ON CONFLICT(user_id,farm_id) DO UPDATE SET role=excluded.role RETURNING *", (user_id, farm_id, payload.role), conn=conn)[0]
        audit(conn, user, "membership.updated", "user", user_id, farm_id, {"role": payload.role})
        return row


@router.delete("/admin/users/{user_id}/farms/{farm_id}")
def remove_membership(user_id: int, farm_id: int, user=Depends(require_admin)):
    with db.connect() as conn:
        rows = db.query("DELETE FROM farm_memberships WHERE user_id=%s AND farm_id=%s RETURNING user_id", (user_id, farm_id), conn=conn)
        if not rows:
            raise HTTPException(404, "Membership not found")
        audit(conn, user, "membership.deleted", "user", user_id, farm_id)
        return {"removed": True}


@router.get("/admin/farms")
def admin_farms(user=Depends(require_admin)):
    return db.query("SELECT * FROM farms ORDER BY farm_id")


@router.post("/admin/farms", status_code=201)
def create_farm(payload: FarmCreate, user=Depends(require_admin)):
    values = payload.model_dump()
    with db.connect() as conn:
        row = db.query(f"INSERT INTO farms({','.join(values)}) VALUES({','.join(['%s'] * len(values))}) RETURNING *", tuple(values.values()), conn=conn)[0]
        audit(conn, user, "farm.created", "farm", row["farm_id"], row["farm_id"])
        return row


@router.patch("/admin/farms/{farm_id}")
def change_farm(farm_id: int, payload: FarmPatch, user=Depends(require_admin)):
    values = patch_values(payload, nullable=("city", "district"))
    with db.connect() as conn:
        exists(conn, "farms", "farm_id", farm_id)
        row = update(conn, "farms", "farm_id", farm_id, values)
        audit(conn, user, "farm.updated", "farm", farm_id, farm_id, {"fields": list(values)})
        return row


@router.get("/admin/farms/{farm_id}/zones")
def admin_zones(farm_id: int, user=Depends(require_admin)):
    return db.query("SELECT * FROM zones WHERE farm_id=%s ORDER BY zone_id", (farm_id,))


@router.post("/admin/farms/{farm_id}/zones", status_code=201)
def create_zone(farm_id: int, payload: ZoneCreate, user=Depends(require_admin)):
    with db.connect() as conn:
        exists(conn, "farms", "farm_id", farm_id)
        exists(conn, "crops", "crop_id", payload.crop_id)
        row = db.query("INSERT INTO zones(farm_id,crop_id,zone_name,area_hectare,soil_type) VALUES(%s,%s,%s,%s,%s) RETURNING *", (farm_id, payload.crop_id, payload.zone_name, payload.area_hectare, payload.soil_type), conn=conn)[0]
        audit(conn, user, "zone.created", "zone", row["zone_id"], farm_id)
        return row


@router.patch("/admin/farms/{farm_id}/zones/{zone_id}")
def change_zone(farm_id: int, zone_id: int, payload: ZonePatch, user=Depends(require_admin)):
    values = patch_values(payload)
    with db.connect() as conn:
        exists(conn, "zones", "zone_id", zone_id, "AND farm_id=%s", (farm_id,))
        if "crop_id" in values:
            exists(conn, "crops", "crop_id", values["crop_id"])
        row = update(conn, "zones", "zone_id", zone_id, values)
        audit(conn, user, "zone.updated", "zone", zone_id, farm_id, {"fields": list(values)})
        return row


@router.get("/admin/farms/{farm_id}/sensors")
def admin_sensors(farm_id: int, user=Depends(require_admin)):
    return db.query("SELECT s.* FROM sensors s JOIN zones z USING(zone_id) WHERE z.farm_id=%s ORDER BY sensor_id", (farm_id,))


@router.post("/admin/farms/{farm_id}/sensors", status_code=201)
def create_sensor(farm_id: int, payload: SensorCreate, user=Depends(require_admin)):
    with db.connect() as conn:
        exists(conn, "zones", "zone_id", payload.zone_id, "AND farm_id=%s", (farm_id,))
        row = db.query("INSERT INTO sensors(zone_id,sensor_code) VALUES(%s,%s) RETURNING *", (payload.zone_id, payload.sensor_code), conn=conn)[0]
        audit(conn, user, "sensor.created", "sensor", row["sensor_id"], farm_id)
        return row


@router.patch("/admin/farms/{farm_id}/sensors/{sensor_id}")
def change_sensor(farm_id: int, sensor_id: int, payload: SensorPatch, user=Depends(require_admin)):
    values = patch_values(payload)
    with db.connect() as conn:
        rows = db.query("SELECT s.sensor_id FROM sensors s JOIN zones z USING(zone_id) WHERE s.sensor_id=%s AND z.farm_id=%s", (sensor_id, farm_id), conn=conn)
        if not rows:
            raise HTTPException(404, "Sensor not found")
        if "zone_id" in values:
            exists(conn, "zones", "zone_id", values["zone_id"], "AND farm_id=%s", (farm_id,))
        row = update(conn, "sensors", "sensor_id", sensor_id, values)
        audit(conn, user, "sensor.updated", "sensor", sensor_id, farm_id, {"fields": list(values)})
        return row


@router.get("/admin/irrigation-rules")
def rules(user=Depends(require_admin)):
    return db.query("SELECT r.*,c.crop_name FROM irrigation_rules r JOIN crops c USING(crop_id) ORDER BY crop_id")


@router.get("/admin/crops")
def crops(user=Depends(require_admin)):
    return db.query("SELECT crop_id,crop_name FROM crops ORDER BY crop_name")


@router.put("/admin/irrigation-rules")
def set_rule(payload: RuleInput, user=Depends(require_admin)):
    with db.connect() as conn:
        exists(conn, "crops", "crop_id", payload.crop_id)
        row = db.query("INSERT INTO irrigation_rules(crop_id,moisture_min,moisture_max,temperature_min,temperature_max,recommended_water_mm) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(crop_id) DO UPDATE SET moisture_min=excluded.moisture_min,moisture_max=excluded.moisture_max,temperature_min=excluded.temperature_min,temperature_max=excluded.temperature_max,recommended_water_mm=excluded.recommended_water_mm RETURNING *", tuple(payload.model_dump().values()), conn=conn)[0]
        audit(conn, user, "rule.updated", "crop", payload.crop_id, details=payload.model_dump())
        return row


@router.get("/admin/settings")
def settings(user=Depends(require_admin)):
    return {"values": {row["key"]: row["value"] for row in db.query("SELECT key,value FROM system_settings WHERE key=ANY(%s) ORDER BY key", (list(SETTING_APPLICATION),))},
            "application_status": "saved_configuration", "apply_info": SETTING_APPLICATION}


@router.patch("/admin/settings")
def change_settings(payload: SettingsInput, user=Depends(require_admin)):
    with db.connect() as conn:
        for key, value in payload.values.items():
            db.query("INSERT INTO system_settings(key,value,updated_by) VALUES(%s,%s::jsonb,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_by=excluded.updated_by,updated_at=now() RETURNING key", (key, json.dumps(value), user["id"]), conn=conn)
        audit(conn, user, "settings.updated", "settings", "system", details=payload.values)
        return {"values": payload.values, "application_status": "saved_configuration",
                "apply_info": {key: SETTING_APPLICATION[key] for key in payload.values}}


@router.get("/admin/audit")
def audit_events(user=Depends(require_admin), limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
    rows = db.query("SELECT * FROM audit_events ORDER BY id DESC LIMIT %s OFFSET %s", (limit, offset))
    total = db.query("SELECT count(*) AS total FROM audit_events")[0]["total"]
    return {"items": rows, "total": total, "limit": limit, "offset": offset}


@router.get("/admin/system/services")
def services(user=Depends(require_admin)):
    return call_agent("GET", "/services")


@router.post("/admin/system/services/{service}/{action}")
def service_action(service: str, action: str, user=Depends(require_admin)):
    validate_service_action(service, action)
    return external_operation(user, "service." + action, "service", service,
                              "POST", f"/services/{service}/{action}")


@router.get("/admin/pipelines")
def pipelines(user=Depends(require_admin)):
    return call_agent("GET", "/pipelines")


@router.post("/admin/pipelines/{dag}/trigger")
def trigger_pipeline(dag: str, user=Depends(require_admin)):
    validate_dag(dag)
    return external_operation(user, "pipeline.trigger", "pipeline", dag,
                              "POST", f"/pipelines/{dag}/trigger")


@router.patch("/admin/pipelines/{dag}/state")
def pipeline_state(dag: str, payload: PipelineState, user=Depends(require_admin)):
    validate_dag(dag)
    return external_operation(user, "pipeline.state", "pipeline", dag,
                              "PATCH", f"/pipelines/{dag}/state", payload.model_dump())


@router.get("/farms/{farm_id}/irrigation-devices")
def irrigation_devices(farm_id: int, user=Depends(current_account)):
    require_farm(user, farm_id)
    return db.query("SELECT * FROM api_irrigation_devices WHERE farm_id=%s ORDER BY device_id", (farm_id,))


@router.post("/admin/farms/{farm_id}/irrigation-devices", status_code=201)
def create_device(farm_id: int, payload: DeviceCreate, user=Depends(require_admin)):
    with db.connect() as conn:
        exists(conn, "farms", "farm_id", farm_id)
        if payload.zone_id is not None:
            exists(conn, "zones", "zone_id", payload.zone_id, "AND farm_id=%s", (farm_id,))
        row = db.query("INSERT INTO api_irrigation_devices(device_id,farm_id,zone_id,name,enabled) VALUES(%s,%s,%s,%s,%s) RETURNING *", (payload.device_id, farm_id, payload.zone_id, payload.name, payload.enabled), conn=conn)[0]
        audit(conn, user, "irrigation_device.created", "device", payload.device_id, farm_id)
        return row


@router.patch("/admin/farms/{farm_id}/irrigation-devices/{device_id}")
def change_device(farm_id: int, device_id: str, payload: DevicePatch, user=Depends(require_admin)):
    values = patch_values(payload, nullable=("zone_id",))
    with db.connect() as conn:
        exists(conn, "api_irrigation_devices", "device_id", device_id, "AND farm_id=%s", (farm_id,))
        if values.get("zone_id") is not None:
            exists(conn, "zones", "zone_id", values["zone_id"], "AND farm_id=%s", (farm_id,))
        values["updated_at"] = datetime.now(timezone.utc)
        row = update(conn, "api_irrigation_devices", "device_id", device_id, values)
        audit(conn, user, "irrigation_device.updated", "device", device_id, farm_id, {"fields": list(values)})
        return row


@router.get("/farms/{farm_id}/irrigation-commands")
def commands(farm_id: int, user=Depends(current_account), limit: int = Query(100, ge=1, le=500)):
    require_farm(user, farm_id)
    return db.query("SELECT * FROM irrigation_commands WHERE farm_id=%s ORDER BY created_at DESC LIMIT %s", (farm_id, limit))


@router.post("/farms/{farm_id}/irrigation-commands", status_code=202)
def queue_command(farm_id: int, payload: IrrigationCommand, user=Depends(current_account)):
    require_farm(user, farm_id, roles=("owner", "manager"))
    with db.connect() as conn:
        prior = db.query("SELECT * FROM irrigation_commands WHERE farm_id=%s AND requested_by=%s AND idempotency_key=%s", (farm_id, user["id"], payload.idempotency_key), conn=conn)
        if prior:
            if prior[0]["device_id"] != payload.device_id or prior[0]["duration_seconds"] != payload.duration_seconds:
                raise HTTPException(409, "Idempotency key already used with another command")
            return prior[0]
        exists(conn, "api_irrigation_devices", "device_id", payload.device_id, "AND farm_id=%s AND enabled FOR UPDATE", (farm_id,))
        configured = db.query("SELECT value FROM system_settings WHERE key='irrigation.max_duration_seconds'", conn=conn)
        if configured and payload.duration_seconds > int(configured[0]["value"]):
            raise HTTPException(422, "Duration exceeds the configured irrigation limit")
        identifier = str(uuid.uuid4())
        inserted = db.query("INSERT INTO irrigation_commands(id,farm_id,device_id,duration_seconds,idempotency_key,requested_by,status) VALUES(%s,%s,%s,%s,%s,%s,'queued') ON CONFLICT(farm_id,requested_by,idempotency_key) DO NOTHING RETURNING *", (identifier, farm_id, payload.device_id, payload.duration_seconds, payload.idempotency_key, user["id"]), conn=conn)
        if not inserted:
            existing = db.query("SELECT * FROM irrigation_commands WHERE farm_id=%s AND requested_by=%s AND idempotency_key=%s", (farm_id, user["id"], payload.idempotency_key), conn=conn)[0]
            if existing["device_id"] != payload.device_id or existing["duration_seconds"] != payload.duration_seconds:
                raise HTTPException(409, "Idempotency key already used with another command")
            return existing
        audit(conn, user, "irrigation.queued", "irrigation_command", identifier, farm_id, {"device_id": payload.device_id, "duration_seconds": payload.duration_seconds})
        return inserted[0]


@router.post("/farms/{farm_id}/irrigation-commands/{command_id}/cancel")
def cancel_command(farm_id: int, command_id: uuid.UUID, user=Depends(current_account)):
    require_farm(user, farm_id, roles=("owner", "manager"))
    with db.connect() as conn:
        rows = db.query("UPDATE irrigation_commands SET status='cancelled',updated_at=now() WHERE id=%s AND farm_id=%s AND status='queued' RETURNING *", (str(command_id), farm_id), conn=conn)
        if not rows:
            exists(conn, "irrigation_commands", "id", str(command_id), "AND farm_id=%s", (farm_id,))
            raise HTTPException(409, "Only queued commands can be cancelled")
        audit(conn, user, "irrigation.cancelled", "irrigation_command", command_id, farm_id)
        return rows[0]


@router.patch("/farms/{farm_id}/alerts/{zone_id}")
def resolve_alert(farm_id: int, zone_id: int, payload: Resolution, user=Depends(current_account)):
    require_farm(user, farm_id, roles=("owner", "manager"))
    from .main import ch, fresh, current_rule, iso
    with db.connect() as conn:
        zone = exists(conn, "zones", "zone_id", zone_id, "AND farm_id=%s", (farm_id,))
        rows = ch("SELECT last_update,crop_id,moisture_min,moisture_max,recommended_water_mm FROM zone_summary_latest WHERE farm_id={farm:Int64} AND zone_id={zone:Int64} AND water_status IN ('Dry','Overwatered') AND last_update >= now()-INTERVAL 10 MINUTE AND last_update<=now()+INTERVAL 1 MINUTE", {"farm": farm_id, "zone": zone_id})
        if not rows:
            raise HTTPException(409, "Alert is no longer active")
        timestamp = iso(rows[0].get("last_update"))
        if timestamp is None:
            raise HTTPException(409, "Alert observation is invalid; refresh before resolving")
        current_time = datetime.fromisoformat(timestamp)
        if current_time != payload.event_time:
            raise HTTPException(409, "Alert changed; refresh before resolving")
        if not fresh(current_time) or str(rows[0].get("crop_id")) != str(zone.get("crop_id")):
            raise HTTPException(409, "Alert metadata changed or observation expired; refresh before resolving")
        rules = db.query("SELECT moisture_min,moisture_max,recommended_water_mm FROM irrigation_rules WHERE crop_id=%s FOR SHARE", (zone["crop_id"],), conn=conn)
        if not rules or not current_rule(rows[0], {**zone, **rules[0]}):
            raise HTTPException(409, "Alert rule changed; refresh after new measurements are processed")
        row = db.query("INSERT INTO alert_resolutions(farm_id,zone_id,event_time,resolved,resolved_by) VALUES(%s,%s,%s,true,%s) ON CONFLICT(farm_id,zone_id,event_time) DO UPDATE SET resolved=true,resolved_by=excluded.resolved_by,updated_at=now() RETURNING *", (farm_id, zone_id, payload.event_time, user["id"]), conn=conn)[0]
        audit(conn, user, "alert.resolved", "zone", zone_id, farm_id, {"event_time": payload.event_time.isoformat()})
        return row
