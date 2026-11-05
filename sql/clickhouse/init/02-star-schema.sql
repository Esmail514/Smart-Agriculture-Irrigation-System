-- Canonical analytical grain: one accepted reading per sensor + event_time.
-- Dimensions are authoritative PostgreSQL snapshots (SCD Type 1).
CREATE TABLE IF NOT EXISTS agri_gold.dim_farm (
    farm_id Int64, farm_name String, city Nullable(String), district Nullable(String),
    latitude Nullable(Decimal(9,6)), longitude Nullable(Decimal(9,6)),
    total_area_hectare Nullable(Decimal(10,2))
) ENGINE = MergeTree ORDER BY farm_id;
CREATE TABLE IF NOT EXISTS agri_gold.dim_zone (
    zone_id Int64, farm_id Int64, crop_id Int64, zone_name String,
    area_hectare Nullable(Decimal(10,2)), soil_type Nullable(String)
) ENGINE = MergeTree ORDER BY zone_id;
CREATE TABLE IF NOT EXISTS agri_gold.dim_crop (
    crop_id Int64, crop_name String, optimal_moisture_min Decimal(5,2),
    optimal_moisture_max Decimal(5,2)
) ENGINE = MergeTree ORDER BY crop_id;
CREATE TABLE IF NOT EXISTS agri_gold.dim_sensor (
    sensor_id Int64, zone_id Int64, sensor_code String
) ENGINE = MergeTree ORDER BY sensor_id;
CREATE TABLE IF NOT EXISTS agri_gold.fact_sensor_readings (
    reading_id String, sensor_id Int64, zone_id Int64, farm_id Int64, crop_id Int64,
    soil_moisture Float64, soil_temperature Float64,
    air_temperature Float64, air_humidity Float64,
    event_time DateTime64(6, 'UTC'), processing_time DateTime64(6, 'UTC'),
    water_status LowCardinality(String)
) ENGINE = ReplacingMergeTree(processing_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (farm_id, zone_id, sensor_id, event_time, reading_id);

CREATE VIEW IF NOT EXISTS agri_gold.sensor_readings AS
SELECT * FROM agri_gold.fact_sensor_readings FINAL;
CREATE TABLE IF NOT EXISTS agri_gold.fact_weather_observations (
    weather_reading_id String, farm_id Int64,
    observation_time_utc DateTime64(6, 'UTC'), fetched_at DateTime64(6, 'UTC'),
    temperature Float64, feels_like Float64, humidity Float64, pressure Float64,
    wind_speed Float64, cloud_cover Float64, rain_1h_mm Float64,
    weather_condition String, weather_description String, schema_version UInt16
) ENGINE = ReplacingMergeTree(fetched_at)
PARTITION BY toYYYYMM(observation_time_utc)
ORDER BY (farm_id, observation_time_utc, weather_reading_id);
CREATE VIEW IF NOT EXISTS agri_gold.fact_zone_summary AS
SELECT zone_id, farm_id, crop_id, sensor_count, avg_soil_moisture,
       avg_soil_temperature, avg_air_temperature, avg_air_humidity,
       moisture_min, moisture_max, recommended_water_mm, last_update, water_status
FROM agri_gold.zone_summary_history FINAL;
CREATE VIEW IF NOT EXISTS agri_gold.fact_farm_weather_daily AS
SELECT farm_id, weather_date, avg_temperature, avg_humidity, total_rain,
       avg_wind_speed, avg_clouds, weather_condition, last_update
FROM agri_gold.farm_weather_daily;
