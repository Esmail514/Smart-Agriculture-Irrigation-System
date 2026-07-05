-- ============================================================
-- ClickHouse Database & Table Initialization
-- Smart Agriculture Irrigation System - Gold Layer
-- ============================================================
-- This script runs when ClickHouse container starts for the
-- first time. Creates the Gold layer schema for analytics.
-- ============================================================

-- Create the Gold Layer analytics database
CREATE DATABASE IF NOT EXISTS agri_gold;

-- ============================================================
-- TABLE: agri_gold.sensor_readings_hourly
-- Purpose: Aggregated hourly sensor data (Gold Layer)
-- Optimized for Power BI and time-series queries
-- ============================================================
CREATE TABLE IF NOT EXISTS agri_gold.sensor_readings_hourly
(
    reading_hour        DateTime,
    sensor_id           String,
    field_id            Int32,
    field_name          String,
    crop_name           String,
    soil_moisture_avg   Float32,
    soil_moisture_min   Float32,
    soil_moisture_max   Float32,
    temperature_avg     Float32,
    humidity_avg        Float32,
    reading_count       Int32,
    requires_irrigation UInt8,
    alert_count         Int32,
    ingestion_ts        DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(reading_hour)
ORDER BY (field_id, sensor_id, reading_hour)
TTL toDateTime(reading_hour) + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- ============================================================
-- TABLE: agri_gold.irrigation_events
-- Purpose: Irrigation trigger and duration log
-- ============================================================
CREATE TABLE IF NOT EXISTS agri_gold.irrigation_events
(
    event_id            String,
    field_id            Int32,
    field_name          String,
    sensor_id           String,
    event_type          String,   -- triggered, completed, cancelled
    trigger_reason      String,   -- low_moisture, scheduled, manual
    soil_moisture_at_trigger Float32,
    duration_minutes    Float32,
    water_volume_liters Float32,
    event_ts            DateTime,
    ingestion_ts        DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (field_id, event_ts)
TTL toDateTime(event_ts) + INTERVAL 5 YEAR
SETTINGS index_granularity = 8192;

-- ============================================================
-- TABLE: agri_gold.weather_daily
-- Purpose: Daily weather summary per field location
-- ============================================================
CREATE TABLE IF NOT EXISTS agri_gold.weather_daily
(
    report_date         Date,
    field_id            Int32,
    location_lat        Float32,
    location_lon        Float32,
    temp_min_c          Float32,
    temp_max_c          Float32,
    temp_avg_c          Float32,
    humidity_avg_pct    Float32,
    precipitation_mm    Float32,
    wind_speed_avg_ms   Float32,
    evapotranspiration  Float32,   -- mm/day - estimated crop water need
    weather_source      String,
    ingestion_ts        DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(report_date)
ORDER BY (field_id, report_date)
TTL toDateTime(report_date) + INTERVAL 3 YEAR
SETTINGS index_granularity = 8192;

-- ============================================================
-- TABLE: agri_gold.pipeline_metrics
-- Purpose: Data pipeline operational metrics
-- ============================================================
CREATE TABLE IF NOT EXISTS agri_gold.pipeline_metrics
(
    metric_ts           DateTime,
    pipeline_name       String,
    layer               String,   -- bronze, silver, gold
    records_processed   Int64,
    processing_time_ms  Int64,
    errors_count        Int32,
    status              String,
    ingestion_ts        DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(metric_ts)
ORDER BY (pipeline_name, metric_ts)
TTL toDateTime(metric_ts) + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- ============================================================
-- MATERIALIZED VIEW: Real-time dashboard view
-- Purpose: Live aggregation for Grafana dashboards
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS agri_gold.mv_field_status
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(reading_hour)
ORDER BY (field_id, reading_hour)
AS
SELECT
    field_id,
    field_name,
    crop_name,
    toStartOfHour(reading_hour) AS reading_hour,
    avgState(soil_moisture_avg) AS moisture_avg_state,
    maxState(soil_moisture_max) AS moisture_max_state,
    minState(soil_moisture_min) AS moisture_min_state,
    sumState(requires_irrigation) AS irrigation_needed_state
FROM agri_gold.sensor_readings_hourly
GROUP BY field_id, field_name, crop_name, toStartOfHour(reading_hour);

-- ============================================================
-- Verify tables were created
-- ============================================================
SHOW TABLES IN agri_gold;
