# Application backend and administration

The Flutter mobile application and website share one FastAPI backend. PostgreSQL stores accounts, per-farm permissions, sessions, registered irrigation actuators, commands, alert acknowledgements, settings and the audit trail. ClickHouse serves analytical measurements. Clients never connect directly to either database.

## Start and create the first administrator

Run from `Pipeline`. Configure the existing PostgreSQL, ClickHouse and Airflow credentials in the untracked `.env`. Generate a private service-control token with `python -c "import secrets; print(secrets.token_urlsafe(48))"` and set `CONTROL_AGENT_TOKEN` there. The Compose configuration supplies the same token to the API and private agent; it is never included in the Flutter build.

```powershell
docker compose up -d --build
docker compose exec -e BOOTSTRAP_ADMIN_EMAIL=you@example.com -e BOOTSTRAP_ADMIN_NAME=Administrator api python scripts/migrate_api_schema.py --init-schema --bootstrap-admin
```

The second command asks for a password of at least 12 characters. Replace the email and name with the intended administrator's details. It refuses to create another initial administrator if one already exists. Existing database volumes require this explicit schema migration; no volume deletion is required. Later accounts and farm assignments are managed through the application's administration screen.

For a host-side migration, set the PostgreSQL connection environment variables to the reachable database address and run `venv/Scripts/python.exe scripts/migrate_api_schema.py --init-schema --bootstrap-admin`. Container hostnames such as `postgres` are resolved inside Compose, not by a normal Windows process.

```powershell
# Optional simulator: provision it for management without starting it.
docker compose --profile simulation build sensor-simulator
docker compose --profile simulation create sensor-simulator
# Optional operator dashboards and troubleshooting tools.
docker compose --profile monitoring up -d
docker compose --profile tools create kafka-ui spark-history
```

The application can start already provisioned optional services. A service that has not been created is reported as `not_created`; the private agent does not provision arbitrary containers. Simulator settings refresh within about 30 seconds plus any current send/database operation. Sensor metadata refreshes at batch boundaries. Bronze registry and Silver rules are cached by their streaming processes, so catalog/rule edits require those processes to restart before their new definitions apply.

## Website and mobile client

From `App`:

```powershell
flutter pub get
flutter run -d chrome --web-port=8080 --dart-define=API_BASE_URL=http://localhost:8000
flutter build web --dart-define=API_BASE_URL=https://your-api-host
```

The local origin matches the default `CORS_ORIGINS=http://localhost:8080`. For deployment, configure the actual website origin and reachable API URL. A phone's `localhost` refers to the phone. Sign in with the email/password created above. Legacy configured API tokens are an optional advanced viewer login and cannot administer the system.

## Dashboard and permission contract

`GET /dashboards` lists the available dashboard categories. `GET /farms` lists only permitted farm metadata, even if the measurement store is temporarily unavailable. An unavailable alert count is null. `GET /farms/{id}/dashboard?range=24h|7d|30d` returns fields, registered sensors, alerts, weather, model predictions, a soil-moisture series and KPI counts in one response. Weather and ML failures are reported as partial availability; current measurements require the measurement store. Separate field charts, soil analytics, weather, devices and alerts endpoints remain available.

An absent or stale measurement stays null/unknown. Sensor online status means an observation was received within ten minutes. Recommended water in millimetres is a suggestion, not measured consumption. Current weather has a two-hour freshness limit. Model output is shown only when its evidence identifies the current-dryness target and is at most ten minutes old; no tomorrow forecast is invented.

`GET /farms/{id}/access` returns the authenticated account's capabilities for that farm without querying ClickHouse. The irrigation screen uses this endpoint so a measurement-store outage does not hide PostgreSQL-backed command history and controls. Device enablement, current permissions and duration limits are still rechecked by each write. Refreshing the screen clears selections for disabled/deleted actuators and ignores results from superseded refreshes.

Active alerts use current PostgreSQL zone names and are suppressed when their crop identity, moisture thresholds or recommended dose no longer match the catalog, their event timestamp is malformed/stale, or their event is over one minute ahead of server time. Field recommendations use the same rule checks while preserving observed measurements. Alert acknowledgement rechecks the current rule inside its database transaction before recording a resolution. Historical reading and platform freshness queries also bound future observations.

Catalog PATCH requests can explicitly clear optional farm city/district fields and an actuator's zone assignment with JSON null. Required fields still reject null. Assigning an actuator to no zone makes it farm-wide; it does not change farm permissions.

Prediction evidence now carries `crop_id`, `moisture_min` and `moisture_max`. The API only displays a prediction when these match the current PostgreSQL zone/rule; changing a crop or moisture rule invalidates an older prediction immediately. Legacy prediction evidence lacking these fields is hidden. Restart the relevant streams to load updated catalog/rules, then rerun the ML pipeline to generate compatible evidence.

Platform administrators manage users, memberships, farms, zones, sensors, irrigation devices, crop rules, settings, audit records, service states and the three allowlisted Airflow DAGs. Owners/managers can acknowledge alerts and queue/cancel irrigation commands in their own farms; viewers can read. Per-farm dashboard capabilities drive buttons, and every write rechecks server permissions. The global admin dashboard summarizes catalog counts and recent measurements.

The supported controls and payloads are documented in [API_CONTROL.md](API_CONTROL.md). The full generated contract is [api-openapi.json](api-openapi.json); interactive API documentation is served at `/docs` after startup.

## Control and monitoring behavior

The public API has no Docker socket. A private control agent, without a published port, validates a fixed service/DAG allowlist. Only the API, agent and Airflow webserver share the internal control network. It never accepts commands, SQL, arbitrary container identities or file paths. PostgreSQL, ClickHouse and MinIO can be restarted, while stopping them is excluded from application controls. A durable requested audit is committed before external actions; outcome audit is recorded separately, so a PostgreSQL restart cannot erase the requested action. An upstream timeout can leave an executed action unconfirmed; refresh system state before retrying.

Irrigation commands remain `queued` until an authenticated actuator executor is implemented and connected. The API records intent and supports cancellation; it does not claim physical pumping. Soil sensors cannot be used as actuator IDs.

Prometheus scrapes `/metrics` for HTTP request rates, status counts and duration histograms. Labels contain route templates, never user/farm IDs, raw query strings or tokens. The Grafana operations dashboard includes API rate, server errors and p95 latency along with infrastructure availability. Alert rules cover API errors/latency and existing infrastructure conditions. See [observability-deployment.md](observability-deployment.md) for exporter and dashboard setup.
