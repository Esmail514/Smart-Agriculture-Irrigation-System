-- ============================================================
-- Smart Agriculture Irrigation System
-- PostgreSQL Database Initialization
-- ============================================================
-- Creates all required databases and schemas.
-- This script runs automatically when the container starts.
-- ============================================================

-- Create Airflow metadata database
CREATE DATABASE airflow;

-- Create crop data database (same as main DB but with proper schemas)
-- The main DB (smart_agriculture) is already created via POSTGRES_DB env var

-- Connect to the main database and create schemas
\connect smart_agriculture;

-- ============================================================
-- SCHEMA: crop_data
-- Purpose: Crop type definitions, growth stages, requirements
-- ============================================================
CREATE SCHEMA IF NOT EXISTS crop_data;

CREATE TABLE IF NOT EXISTS crop_data.crops (
    crop_id         SERIAL PRIMARY KEY,
    crop_name       VARCHAR(100) NOT NULL UNIQUE,
    scientific_name VARCHAR(150),
    crop_type       VARCHAR(50) NOT NULL,  -- cereal, vegetable, fruit, etc.
    growth_days     INT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS crop_data.irrigation_requirements (
    req_id              SERIAL PRIMARY KEY,
    crop_id             INT REFERENCES crop_data.crops(crop_id),
    growth_stage        VARCHAR(50) NOT NULL,  -- seedling, vegetative, flowering, harvest
    min_soil_moisture   FLOAT NOT NULL,        -- percentage (0-100)
    max_soil_moisture   FLOAT NOT NULL,
    optimal_moisture    FLOAT NOT NULL,
    irrigation_freq_hrs INT,                   -- how often to check in hours
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- SCHEMA: field_data
-- Purpose: Physical field/plot metadata
-- ============================================================
CREATE SCHEMA IF NOT EXISTS field_data;

CREATE TABLE IF NOT EXISTS field_data.fields (
    field_id        SERIAL PRIMARY KEY,
    field_name      VARCHAR(100) NOT NULL,
    location_lat    DECIMAL(9, 6),
    location_lon    DECIMAL(9, 6),
    area_hectares   DECIMAL(10, 4),
    soil_type       VARCHAR(50),    -- clay, loam, sandy, silt
    current_crop_id INT REFERENCES crop_data.crops(crop_id),
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS field_data.sensors (
    sensor_id       VARCHAR(50) PRIMARY KEY,  -- e.g., "FIELD-01-SENSOR-A"
    field_id        INT REFERENCES field_data.fields(field_id),
    sensor_type     VARCHAR(50) NOT NULL,     -- soil_moisture, temperature, etc.
    depth_cm        INT,                       -- sensor depth in soil
    installed_at    DATE,
    last_seen       TIMESTAMPTZ,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- SCHEMA: pipeline_meta
-- Purpose: Track pipeline runs, data quality, lineage
-- ============================================================
CREATE SCHEMA IF NOT EXISTS pipeline_meta;

CREATE TABLE IF NOT EXISTS pipeline_meta.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    run_date        DATE NOT NULL,
    status          VARCHAR(20),    -- running, success, failed
    records_in      BIGINT,
    records_out     BIGINT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_meta.data_quality_checks (
    check_id        SERIAL PRIMARY KEY,
    run_id          INT REFERENCES pipeline_meta.pipeline_runs(run_id),
    layer           VARCHAR(20) NOT NULL,  -- bronze, silver, gold
    check_name      VARCHAR(100),
    passed          BOOLEAN,
    records_checked BIGINT,
    failures        BIGINT,
    checked_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- SEED DATA: Sample crops for Smart Agriculture
-- ============================================================
INSERT INTO crop_data.crops (crop_name, scientific_name, crop_type, growth_days) VALUES
    ('Wheat',    'Triticum aestivum',    'cereal',    110),
    ('Corn',     'Zea mays',             'cereal',    100),
    ('Tomato',   'Solanum lycopersicum', 'vegetable',  75),
    ('Potato',   'Solanum tuberosum',    'vegetable',  90),
    ('Soybean',  'Glycine max',          'legume',    110),
    ('Sunflower','Helianthus annuus',    'oilseed',   120),
    ('Cotton',   'Gossypium hirsutum',   'fiber',     160),
    ('Rice',     'Oryza sativa',         'cereal',    120)
ON CONFLICT (crop_name) DO NOTHING;

-- Irrigation requirements for Wheat (all growth stages)
INSERT INTO crop_data.irrigation_requirements
    (crop_id, growth_stage, min_soil_moisture, max_soil_moisture, optimal_moisture, irrigation_freq_hrs)
SELECT c.crop_id, stage, min_m, max_m, opt_m, freq
FROM crop_data.crops c,
(VALUES
    ('seedling',   35.0, 65.0, 50.0, 12),
    ('vegetative', 40.0, 70.0, 55.0, 24),
    ('flowering',  50.0, 75.0, 62.0, 12),
    ('grain_fill', 45.0, 72.0, 58.0, 24),
    ('harvest',    25.0, 50.0, 35.0, 48)
) AS s(stage, min_m, max_m, opt_m, freq)
WHERE c.crop_name = 'Wheat';

-- Sample fields
INSERT INTO field_data.fields (field_name, location_lat, location_lon, area_hectares, soil_type, current_crop_id)
SELECT 'North Field A', 31.2001, 29.9187, 12.5, 'loam', crop_id FROM crop_data.crops WHERE crop_name = 'Wheat';
INSERT INTO field_data.fields (field_name, location_lat, location_lon, area_hectares, soil_type, current_crop_id)
SELECT 'South Field B', 31.1987, 29.9201, 8.2, 'clay', crop_id FROM crop_data.crops WHERE crop_name = 'Corn';
INSERT INTO field_data.fields (field_name, location_lat, location_lon, area_hectares, soil_type, current_crop_id)
SELECT 'East Plot C',   31.2015, 29.9215, 5.0, 'sandy', crop_id FROM crop_data.crops WHERE crop_name = 'Tomato';

-- Sample sensors
INSERT INTO field_data.sensors (sensor_id, field_id, sensor_type, depth_cm, installed_at, active)
VALUES
    ('FIELD-01-SM-01', 1, 'soil_moisture', 15, '2024-01-15', TRUE),
    ('FIELD-01-SM-02', 1, 'soil_moisture', 30, '2024-01-15', TRUE),
    ('FIELD-01-TMP-01',1, 'temperature',   5,  '2024-01-15', TRUE),
    ('FIELD-02-SM-01', 2, 'soil_moisture', 15, '2024-02-01', TRUE),
    ('FIELD-02-SM-02', 2, 'soil_moisture', 30, '2024-02-01', TRUE),
    ('FIELD-03-SM-01', 3, 'soil_moisture', 15, '2024-03-10', TRUE);
