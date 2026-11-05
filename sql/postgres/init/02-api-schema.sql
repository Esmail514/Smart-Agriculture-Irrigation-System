-- Application identities and controls. Apply explicitly to existing databases;
-- docker-entrypoint init scripts run only when the PostgreSQL volume is empty.
CREATE TABLE IF NOT EXISTS app_users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE CHECK (email = lower(btrim(email))),
    full_name TEXT NOT NULL CHECK (length(btrim(full_name)) BETWEEN 1 AND 200),
    password_hash TEXT NOT NULL,
    platform_role TEXT NOT NULL DEFAULT 'user' CHECK (platform_role IN ('admin','user')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS farm_memberships (
    user_id BIGINT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    farm_id BIGINT NOT NULL REFERENCES farms(farm_id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('owner','manager','viewer')),
    PRIMARY KEY (user_id,farm_id)
);
CREATE TABLE IF NOT EXISTS auth_sessions (
    id UUID PRIMARY KEY,
    family_id UUID NOT NULL,
    user_id BIGINT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    access_hash CHAR(64) NOT NULL UNIQUE,
    refresh_hash CHAR(64) NOT NULL UNIQUE,
    access_expires_at TIMESTAMPTZ NOT NULL,
    refresh_expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (refresh_expires_at >= access_expires_at)
);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_family ON auth_sessions(family_id);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id);
CREATE TABLE IF NOT EXISTS auth_login_attempts (
    email_hash CHAR(64) PRIMARY KEY,
    failed_count INTEGER NOT NULL DEFAULT 0,
    last_failed_at TIMESTAMPTZ,
    blocked_until TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_api_zone_farm ON zones(farm_id,zone_id);
CREATE TABLE IF NOT EXISTS api_irrigation_devices (
    device_id TEXT PRIMARY KEY,
    farm_id BIGINT NOT NULL REFERENCES farms(farm_id),
    zone_id BIGINT REFERENCES zones(zone_id),
    name TEXT NOT NULL CHECK (length(btrim(name)) BETWEEN 1 AND 200),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (farm_id,device_id),
    FOREIGN KEY (farm_id,zone_id) REFERENCES zones(farm_id,zone_id)
);
CREATE TABLE IF NOT EXISTS irrigation_commands (
    id UUID PRIMARY KEY,
    farm_id BIGINT NOT NULL REFERENCES farms(farm_id),
    device_id TEXT NOT NULL,
    duration_seconds INTEGER NOT NULL CHECK (duration_seconds BETWEEN 1 AND 3600),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    requested_by BIGINT NOT NULL REFERENCES app_users(id),
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','cancelled','running','completed','failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failure_reason TEXT,
    UNIQUE (farm_id,requested_by,idempotency_key),
    FOREIGN KEY (farm_id,device_id) REFERENCES api_irrigation_devices(farm_id,device_id)
);
CREATE INDEX IF NOT EXISTS idx_irrigation_commands_farm_time ON irrigation_commands(farm_id,created_at DESC);
CREATE TABLE IF NOT EXISTS alert_resolutions (
    farm_id BIGINT NOT NULL REFERENCES farms(farm_id),
    zone_id BIGINT NOT NULL REFERENCES zones(zone_id),
    event_time TIMESTAMPTZ NOT NULL,
    resolved BOOLEAN NOT NULL DEFAULT TRUE,
    resolved_by BIGINT REFERENCES app_users(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (farm_id,zone_id,event_time),
    FOREIGN KEY (farm_id,zone_id) REFERENCES zones(farm_id,zone_id)
);
CREATE TABLE IF NOT EXISTS audit_events (
    id BIGSERIAL PRIMARY KEY,
    actor_id BIGINT REFERENCES app_users(id) ON DELETE SET NULL,
    farm_id BIGINT REFERENCES farms(farm_id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    target_type TEXT,
    target_id TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_events_created ON audit_events(created_at DESC);
CREATE TABLE IF NOT EXISTS system_settings (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_by BIGINT REFERENCES app_users(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Schema migrations for irrigation command execution lifecycle
ALTER TABLE irrigation_commands ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;
ALTER TABLE irrigation_commands ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE irrigation_commands ADD COLUMN IF NOT EXISTS failure_reason TEXT;

