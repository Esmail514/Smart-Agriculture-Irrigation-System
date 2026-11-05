-- ============================================================
-- ClickHouse Database & Table Initialization
-- Smart Agriculture Irrigation System - Gold Layer
-- ============================================================

CREATE DATABASE IF NOT EXISTS agri_gold;

-- TABLE: zone_summary_latest (full refresh from Gold zone_summary)
CREATE TABLE IF NOT EXISTS agri_gold.zone_summary_latest
(
    zone_id               Int64,
    zone_name             String,
    farm_id               Int64,
    farm_name             String,
    crop_id               Int64,
    crop_name             String,
    sensor_count          Int64,
    avg_soil_moisture     Float64,
    avg_soil_temperature  Float64,
    avg_air_temperature   Float64,
    avg_air_humidity      Float64,
    moisture_min          Nullable(Float64),
    moisture_max          Nullable(Float64),
    recommended_water_mm  Nullable(Float64),
    last_update           DateTime64(6, 'UTC'),
    water_status          String,
    snapshot_id Int64 DEFAULT 0
)
ENGINE = MergeTree()
ORDER BY (farm_id, zone_id)
SETTINGS index_granularity = 8192;

-- TABLE: zone_summary_history (incremental)
CREATE TABLE IF NOT EXISTS agri_gold.zone_summary_history
(
    zone_id               Int64,
    zone_name             String,
    farm_id               Int64,
    farm_name             String,
    crop_id               Int64,
    crop_name             String,
    sensor_count          Int64,
    avg_soil_moisture     Float64,
    avg_soil_temperature  Float64,
    avg_air_temperature   Float64,
    avg_air_humidity      Float64,
    moisture_min          Nullable(Float64),
    moisture_max          Nullable(Float64),
    recommended_water_mm  Nullable(Float64),
    last_update           DateTime64(6, 'UTC'),
    water_status          String,
    snapshot_id Int64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(snapshot_id)
PARTITION BY toYYYYMM(last_update)
ORDER BY (farm_id, zone_id, last_update)
SETTINGS index_granularity = 8192;

-- TABLE: farm_summary_latest (full refresh)
CREATE TABLE IF NOT EXISTS agri_gold.farm_summary_latest
(
    farm_id               Int64,
    farm_name             String,
    zones                 Int64,
    sensors               Int64,
    avg_soil_moisture     Float64,
    avg_air_temperature   Float64,
    avg_air_humidity      Float64,
    dry_readings          Int64,
    healthy_readings      Int64,
    overwatered_readings  Int64,
    last_update           DateTime64(6, 'UTC'),
    snapshot_id Int64 DEFAULT 0
)
ENGINE = MergeTree()
ORDER BY (farm_id)
SETTINGS index_granularity = 8192;

-- TABLE: farm_summary_history (incremental)
CREATE TABLE IF NOT EXISTS agri_gold.farm_summary_history
(
    farm_id               Int64,
    farm_name             String,
    zones                 Int64,
    sensors               Int64,
    avg_soil_moisture     Float64,
    avg_air_temperature   Float64,
    avg_air_humidity      Float64,
    dry_readings          Int64,
    healthy_readings      Int64,
    overwatered_readings  Int64,
    last_update           DateTime64(6, 'UTC'),
    snapshot_id Int64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(snapshot_id)
PARTITION BY toYYYYMM(last_update)
ORDER BY (farm_id, last_update)
SETTINGS index_granularity = 8192;

-- TABLE: irrigation_alerts (incremental)
CREATE TABLE IF NOT EXISTS agri_gold.irrigation_alerts
(
    reading_id            String,
    farm_name             String,
    zone_name             String,
    crop_name             String,
    soil_moisture         Float64,
    water_status          String,
    recommended_water_mm  Nullable(Float64),
    event_time            DateTime64(6, 'UTC'),
    processing_time       DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(event_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, reading_id)
SETTINGS index_granularity = 8192;

-- TABLE: farm_weather_daily
CREATE TABLE IF NOT EXISTS agri_gold.farm_weather_daily
(
    farm_id Int64,
    farm_name String,
    weather_date Date,
    avg_temperature Float64,
    avg_humidity Float64,
    total_rain Float64,
    avg_wind_speed Float64,
    avg_clouds Float64,
    weather_condition String,
    last_update DateTime64(6)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(weather_date)
ORDER BY (farm_id, weather_date);

-- TABLE: weather_irrigation_alerts
CREATE TABLE IF NOT EXISTS agri_gold.weather_irrigation_alerts
(
    farm_id Int64,
    farm_name String,
    zone_id Int64,
    zone_name String,
    crop_id Int64,
    crop_name String,
    soil_moisture Float64,
    expected_rain Nullable(Float64),
    observed_rain_mm Nullable(Float64),
    weather_available UInt8,
    recommended_water_mm Nullable(Float64),
    irrigation_decision String,
    weather_condition String,
    event_time DateTime64(6),
    processing_time DateTime64(6)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(processing_time)
ORDER BY (zone_id, processing_time);

-- TABLE: ml_predictions
CREATE TABLE IF NOT EXISTS agri_gold.ml_predictions
(
    prediction_id           String,
    zone_id                 Int64,
    zone_name               String,
    farm_id                 Int64,
    farm_name               String,
    crop_name               String,
    needs_irrigation_today  UInt8,
    needs_irrigation_tomorrow Nullable(UInt8),
    prediction_confidence   Float64,
    model_version           String,
    prediction_date         Date,
    prediction_time         DateTime64(6),
    feature_snapshot        String
)
ENGINE = ReplacingMergeTree(prediction_time)
PARTITION BY toYYYYMM(prediction_date)
ORDER BY (zone_id, prediction_date)
SETTINGS index_granularity = 8192;

SHOW TABLES IN agri_gold;
