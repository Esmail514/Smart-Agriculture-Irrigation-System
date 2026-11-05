# Management and control API

`src/api/control_routes.py` is included in the main FastAPI application. All endpoints require a database-backed session. Legacy tokens remain read-only. Platform administrators manage the global catalog; farm owners and managers can queue irrigation commands and resolve alerts only in their assigned farms. The server checks permissions on every request.

Apply `sql/postgres/init/02-api-schema.sql` using the API schema migration script before using these endpoints. Existing PostgreSQL volumes do not automatically rerun Docker initialization scripts. Bootstrap the first administrator using the separately documented administrator bootstrap command. The public API never accepts SQL, shell commands, file paths, Docker container IDs or an arbitrary service name.

## Management

- `GET/POST /admin/users`, `PATCH /admin/users/{id}`: `email`, `full_name`, `platform_role` (`user` or `admin`), `is_active`; creation additionally requires a password of 12–1024 characters. Password whitespace is preserved, hashes are never returned, and resetting a password or disabling an account revokes its sessions. The last active administrator cannot be removed, and administrators cannot disable or demote their own account.
- `GET /admin/users/{id}/farms`, `PUT/DELETE /admin/users/{id}/farms/{farm_id}`: PUT accepts `{ "role": "owner|manager|viewer" }`.
- `GET/POST /admin/farms`, `PATCH /admin/farms/{farm_id}`: `farm_name`, `city`, `district`, `latitude`, `longitude`, `total_area_hectare`. Creation requires the name, coordinates and a positive area.
- `GET/POST /admin/farms/{farm_id}/zones`, `PATCH /admin/farms/{farm_id}/zones/{zone_id}`: `crop_id`, `zone_name`, `area_hectare`, `soil_type` (`Loam`, `Sandy`, `Clay`, `Silty`).
- `GET/POST /admin/farms/{farm_id}/sensors`, `PATCH /admin/farms/{farm_id}/sensors/{sensor_id}`: `zone_id`, `sensor_code`. A sensor cannot be moved to another farm through this endpoint.
- `GET /admin/crops` returns crop identifiers and names for selection controls.
- `GET/PUT /admin/irrigation-rules`: PUT accepts `crop_id`, `moisture_min`, `moisture_max`, `temperature_min`, `temperature_max`, `recommended_water_mm`. Thresholds must be finite JSON numbers in their supported range, with minimum below maximum.
- `GET /admin/audit?limit=50&offset=0` returns `{items,total,limit,offset}`. Every catalog, setting, membership, command and resolution change writes an audit event in the same PostgreSQL transaction.

Catalog and irrigation-rule changes are saved immediately. Running Spark streams cache metadata; restart the appropriate stream to load catalog/rule changes. Saving these records does not claim the running stream already applied them.

## Settings

`GET/PATCH /admin/settings` uses `{ "values": { "setting.key": value } }`. PATCH supports only:

| Key | Value | Application |
|---|---|---|
| `simulator.interval_seconds` | number 0.1–3600 | Running simulator refreshes configuration every 30 seconds |
| `simulator.anomaly_rate` | number 0–1 | Running simulator refreshes configuration every 30 seconds |
| `simulator.enabled` | JSON boolean | Controls simulated telemetry generation |
| `irrigation.max_duration_seconds` | integer 1–3600 | Checked when a new irrigation command is queued |

Responses include `application_status: "saved_configuration"` and `apply_info` describing consumer refresh or application to new commands. Saved simulator settings are applied only while the simulator is running. Weather polling is configured by the Airflow DAG schedule; there is no unsupported weather-polling setting here.

## Irrigation commands

Register real/logical irrigation actuators separately from soil sensors:

- `POST /admin/farms/{farm_id}/irrigation-devices`: `{device_id,name,zone_id?,enabled}`.
- `PATCH /admin/farms/{farm_id}/irrigation-devices/{device_id}` updates the name, zone or enabled flag.
- `GET /farms/{farm_id}/irrigation-devices` lists registered actuators.

`POST /farms/{farm_id}/irrigation-commands` requires `{device_id,duration_seconds,idempotency_key}`. Duration is an integer between 1 and 3600 and cannot exceed the saved system limit. Device IDs and idempotency keys use letters, digits, underscore or hyphen; keys contain 8–120 characters. The actuator must be enabled and registered in that farm.

A successful response is HTTP 202 with **status `queued`**. It records a command in PostgreSQL and does not indicate a running pump or successful hardware delivery. A separate authenticated actuator worker must claim and acknowledge commands before `running` or `completed` can be asserted. No such acknowledgement is fabricated by this API.

Retries with the same user/farm/idempotency key return the original command, including its latest status. Reusing a key with different command data returns 409. `GET /farms/{farm_id}/irrigation-commands` returns command history. `POST /farms/{farm_id}/irrigation-commands/{command_id}/cancel` cancels only a queued command; a concurrent worker transition prevents cancellation through an atomic SQL status condition.

## Alert resolution

`PATCH /farms/{farm_id}/alerts/{zone_id}` requires `{ "resolved": true, "event_time": "UTC ISO timestamp" }`. Send the alert's exact observation timestamp. The API checks farm membership, zone ownership, active alert freshness and the current event timestamp. A changed or stale alert returns 409. The resolution applies only to that event; a later alert in the same zone remains unresolved.

## Private system control

Configure `CONTROL_AGENT_URL` and `CONTROL_AGENT_TOKEN` in the API. Without them, control endpoints return 503. The private agent is reached only over authenticated HTTP; the public API has no Docker socket or shell access.

- `GET /admin/system/services` lists service states and supported actions.
- `POST /admin/system/services/{service}/{action}` supports an allowlisted service and `start`, `stop` or `restart`. PostgreSQL, ClickHouse and MinIO support restart only. The API/control agent itself is never a selectable service.
- `GET /admin/pipelines`, `POST /admin/pipelines/{dag}/trigger`, `PATCH /admin/pipelines/{dag}/state` with `{ "is_paused": true|false }`.
- Allowed DAGs: `sensor_pipeline`, `weather_api_pipeline`, `ml_pipeline`.

External actions first commit a durable `*.requested` audit event, then call the agent outside that database transaction. A second transaction records `*.completed` or `*.failed` when possible. This prevents PostgreSQL restart from destroying the request's own audit trail. Successful responses include `operation_id` and `outcome_audit_recorded`; accepted operations remain accepted when PostgreSQL is temporarily unavailable for the outcome audit.

An upstream timeout can leave the external action's outcome unconfirmed even though it executed. The failed audit records this ambiguity. Inspect the service or DAG state before retrying; external actions do not promise distributed exactly-once execution.
