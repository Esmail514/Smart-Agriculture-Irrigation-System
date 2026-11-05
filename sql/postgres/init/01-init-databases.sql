CREATE TABLE IF NOT EXISTS crops (
    crop_id                  BIGSERIAL PRIMARY KEY,
    crop_name                VARCHAR(100) NOT NULL UNIQUE,
    optimal_moisture_min     DECIMAL(5,2) NOT NULL,
    optimal_moisture_max     DECIMAL(5,2) NOT NULL,
    optimal_temperature_min  DECIMAL(5,2),
    optimal_temperature_max  DECIMAL(5,2),
    water_requirement_level  VARCHAR(20) CHECK (water_requirement_level IN ('High', 'Medium', 'Low')),
    growth_duration_days     INTEGER,
    critical_moisture_low    DECIMAL(5,2),
    critical_moisture_high   DECIMAL(5,2),
    soil_temp_min            DECIMAL(5,2),
    soil_temp_max            DECIMAL(5,2),
    air_temp_min             DECIMAL(5,2),
    air_temp_max             DECIMAL(5,2),
    humidity_min              DECIMAL(5,2),
    humidity_max              DECIMAL(5,2),
    created_at                TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS farms (
    farm_id             BIGSERIAL PRIMARY KEY,
    farm_name           VARCHAR(100) NOT NULL UNIQUE,
    city                VARCHAR(100),
    district            VARCHAR(100),
    latitude            DECIMAL(9,6),
    longitude           DECIMAL(9,6),
    total_area_hectare  DECIMAL(10,2),
    created_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS zones (
    zone_id       BIGSERIAL PRIMARY KEY,
    farm_id       BIGINT NOT NULL,
    crop_id       BIGINT NOT NULL,
    zone_name     VARCHAR(100) NOT NULL,
    area_hectare  DECIMAL(10,2),
    soil_type     VARCHAR(50) CHECK (soil_type IN ('Loam', 'Sandy', 'Clay', 'Silty')),
    created_at    TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_zone_farm FOREIGN KEY (farm_id) REFERENCES farms(farm_id) ON DELETE CASCADE,
    CONSTRAINT fk_zone_crop FOREIGN KEY (crop_id) REFERENCES crops(crop_id) ON DELETE CASCADE,
    CONSTRAINT uq_farm_zone_name UNIQUE (farm_id, zone_name)
);

CREATE TABLE IF NOT EXISTS sensors (
    sensor_id    BIGSERIAL PRIMARY KEY,
    zone_id      BIGINT NOT NULL,
    sensor_code  VARCHAR(100) NOT NULL UNIQUE,
    created_at   TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_sensor_zone FOREIGN KEY (zone_id) REFERENCES zones(zone_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS irrigation_rules (
    rule_id                BIGSERIAL PRIMARY KEY,
    crop_id                BIGINT NOT NULL,
    moisture_min           DECIMAL(5,2),
    moisture_max           DECIMAL(5,2),
    temperature_min        DECIMAL(5,2),
    temperature_max        DECIMAL(5,2),
    recommended_water_mm   DECIMAL(6,2),
    created_at             TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_rule_crop FOREIGN KEY (crop_id) REFERENCES crops(crop_id) ON DELETE CASCADE,
    CONSTRAINT uq_rule_crop UNIQUE (crop_id)
);


CREATE INDEX IF NOT EXISTS idx_zone_crop  ON zones(crop_id);
CREATE INDEX IF NOT EXISTS idx_zone_farm  ON zones(farm_id);
CREATE INDEX IF NOT EXISTS idx_sensor_zone ON sensors(zone_id);
CREATE INDEX IF NOT EXISTS idx_rule_crop   ON irrigation_rules(crop_id);


INSERT INTO crops (
    crop_name, optimal_moisture_min, optimal_moisture_max,
    optimal_temperature_min, optimal_temperature_max,
    water_requirement_level, growth_duration_days,
    critical_moisture_low, critical_moisture_high,
    soil_temp_min, soil_temp_max, air_temp_min, air_temp_max,
    humidity_min, humidity_max
) VALUES
    ('Tomato',     30, 45, 20, 30, 'High',   120, 20, 60, 22, 30, 24, 38, 55, 85),
    ('Watermelon', 35, 50, 22, 35, 'High',   100, 25, 65, 24, 34, 26, 40, 45, 75),
    ('Mango',      40, 60, 24, 35, 'Medium', 180, 30, 75, 24, 33, 24, 38, 50, 85),
    ('Sesame',     20, 35, 25, 38, 'Low',     90, 15, 50, 25, 38, 25, 40, 40, 70),
    ('Millet',     15, 30, 25, 40, 'Low',    100, 10, 45, 25, 40, 26, 42, 35, 65),
    ('Sorghum',    20, 35, 24, 38, 'Low',    110, 15, 50, 24, 38, 25, 40, 40, 70),
    ('Cotton',     25, 40, 22, 35, 'Medium', 165, 18, 55, 22, 35, 24, 38, 45, 75),
    ('Banana',     45, 65, 20, 32, 'High',   300, 35, 80, 20, 32, 22, 35, 60, 90),
    ('Date Palm',  20, 40, 18, 42, 'Medium', 365, 12, 60, 18, 42, 20, 48, 20, 60),
    ('Wheat',      25, 40, 10, 25, 'Medium', 120, 15, 55, 10, 25, 12, 28, 50, 75),
    ('Barley',     20, 35,  8, 24, 'Low',    110, 12, 50,  8, 24, 10, 26, 45, 70),
    ('Maize',      30, 50, 20, 35, 'High',   100, 22, 65, 20, 35, 22, 38, 50, 80)
ON CONFLICT (crop_name) DO NOTHING;


INSERT INTO farms (farm_name, city, district, latitude, longitude, total_area_hectare)
VALUES
    ('Tihama Farm - Bayt Al Faqih',      'Al Hudaydah', 'Bayt Al Faqih',      14.516,  43.324, 18),
    ('Tihama Farm - Al Marawi''ah',      'Al Hudaydah', 'Al Marawi''ah',      14.830,  43.383, 21),
    ('Tihama Farm - Al Mansuriyah',      'Al Hudaydah', 'Al Mansuriyah',      14.690,  43.286, 20),
    ('Al-Jawf Farm - Al Hazm',           'Al Jawf',     'Al Hazm',            16.1591, 44.7825, 25),
    ('Al-Jawf Farm - Al Matammah',       'Al Jawf',     'Al Matammah',        15.9333, 45.3833, 30),
    ('Al-Jawf Farm - Khab wa Ash Shaaf', 'Al Jawf',     'Khab wa Ash Shaaf',  16.7500, 45.7000, 22)
ON CONFLICT (farm_name) DO NOTHING;


INSERT INTO irrigation_rules (crop_id, moisture_min, moisture_max, temperature_min, temperature_max, recommended_water_mm)
SELECT c.crop_id, v.moisture_min, v.moisture_max, v.temperature_min, v.temperature_max, v.recommended_water_mm
FROM (VALUES
    ('Tomato',     30, 45, 20, 30, 18),
    ('Watermelon', 35, 50, 22, 35, 25),  
    ('Mango',      40, 60, 24, 35, 20),
    ('Sesame',     20, 35, 25, 38, 10),
    ('Millet',     15, 30, 25, 40, 10),
    ('Sorghum',    20, 35, 24, 38, 10),
    ('Cotton',     25, 40, 22, 35, 16),
    ('Banana',     45, 65, 20, 32, 24),
    ('Date Palm',  20, 40, 18, 42, 16),
    ('Wheat',      25, 40, 10, 25, 16),
    ('Barley',     20, 35,  8, 24, 10),
    ('Maize',      30, 50, 20, 35, 24)
) AS v(crop_name, moisture_min, moisture_max, temperature_min, temperature_max, recommended_water_mm)
JOIN crops c ON c.crop_name = v.crop_name
WHERE NOT EXISTS (SELECT 1 FROM irrigation_rules r WHERE r.crop_id = c.crop_id);


INSERT INTO zones (farm_id, crop_id, zone_name, area_hectare, soil_type)
SELECT f.farm_id, c.crop_id, v.zone_name, v.area_hectare, v.soil_type
FROM (VALUES
    ('Tihama Farm - Bayt Al Faqih', 'Tomato',     'Tomato-01',     1.0, 'Loam'),
    ('Tihama Farm - Bayt Al Faqih', 'Watermelon', 'Watermelon-01', 2.5, 'Sandy'),
    ('Tihama Farm - Bayt Al Faqih', 'Mango',      'Mango-01',      4.5, 'Clay'),
    ('Tihama Farm - Al Marawi''ah', 'Tomato',     'Tomato-02',     1.5, 'Loam'),
    ('Tihama Farm - Al Marawi''ah', 'Watermelon', 'Watermelon-02', 3.0, 'Sandy'),
    ('Tihama Farm - Al Marawi''ah', 'Mango',      'Mango-02',      5.5, 'Clay'),
    ('Tihama Farm - Al Mansuriyah', 'Tomato',     'Tomato-03',     2.0, 'Loam'),
    ('Tihama Farm - Al Mansuriyah', 'Watermelon', 'Watermelon-03', 3.5, 'Sandy'),
    ('Tihama Farm - Al Mansuriyah', 'Mango',      'Mango-03',      6.5, 'Clay'),
    ('Tihama Farm - Bayt Al Faqih', 'Sesame',     'Sesame-01',     1.2, 'Sandy'),
    ('Tihama Farm - Al Marawi''ah', 'Sesame',     'Sesame-02',     1.6, 'Sandy'),
    ('Tihama Farm - Al Mansuriyah', 'Sesame',     'Sesame-03',     2.0, 'Sandy'),
    ('Tihama Farm - Bayt Al Faqih', 'Cotton',     'Cotton-01',     2.0, 'Loam'),
    ('Tihama Farm - Al Marawi''ah', 'Cotton',     'Cotton-02',     2.4, 'Loam'),
    ('Tihama Farm - Al Mansuriyah', 'Cotton',     'Cotton-03',     2.8, 'Loam'),
    ('Tihama Farm - Bayt Al Faqih', 'Millet',     'Millet-01',     1.8, 'Sandy'),
    ('Tihama Farm - Al Marawi''ah', 'Millet',     'Millet-02',     2.2, 'Sandy'),
    ('Tihama Farm - Al Mansuriyah', 'Millet',     'Millet-03',     2.6, 'Sandy'),
    ('Tihama Farm - Bayt Al Faqih', 'Banana',     'Banana-01',     2.5, 'Loam'),
    ('Tihama Farm - Al Marawi''ah', 'Banana',     'Banana-02',     3.0, 'Loam'),
    ('Tihama Farm - Al Mansuriyah', 'Banana',     'Banana-03',     3.5, 'Loam'),
    ('Tihama Farm - Bayt Al Faqih', 'Sorghum',    'Sorghum-01',    2.2, 'Sandy'),
    ('Tihama Farm - Al Marawi''ah', 'Sorghum',    'Sorghum-02',    2.6, 'Sandy'),
    ('Tihama Farm - Al Mansuriyah', 'Sorghum',    'Sorghum-03',    3.0, 'Sandy'),
    ('Tihama Farm - Bayt Al Faqih', 'Date Palm',  'DatePalm-01',   3.0, 'Sandy'),
    ('Tihama Farm - Al Marawi''ah', 'Date Palm',  'DatePalm-02',   3.5, 'Sandy'),
    ('Tihama Farm - Al Mansuriyah', 'Date Palm',  'DatePalm-03',   4.0, 'Sandy'),
    ('Al-Jawf Farm - Al Hazm',           'Wheat',  'Wheat-AJ-01',  4.0, 'Silty'),
    ('Al-Jawf Farm - Al Hazm',           'Barley', 'Barley-AJ-01', 3.0, 'Silty'),
    ('Al-Jawf Farm - Al Hazm',           'Maize',  'Maize-AJ-01',  3.5, 'Loam'),
    ('Al-Jawf Farm - Al Matammah',       'Wheat',  'Wheat-AJ-02',  5.0, 'Silty'),
    ('Al-Jawf Farm - Al Matammah',       'Barley', 'Barley-AJ-02', 3.5, 'Silty'),
    ('Al-Jawf Farm - Al Matammah',       'Maize',  'Maize-AJ-02',  4.0, 'Loam'),
    ('Al-Jawf Farm - Khab wa Ash Shaaf', 'Wheat',  'Wheat-AJ-03',  3.5, 'Silty'),
    ('Al-Jawf Farm - Khab wa Ash Shaaf', 'Barley', 'Barley-AJ-03', 2.5, 'Silty'),
    ('Al-Jawf Farm - Khab wa Ash Shaaf', 'Maize',  'Maize-AJ-03',  3.0, 'Loam')
) AS v(farm_name, crop_name, zone_name, area_hectare, soil_type)
JOIN farms f ON f.farm_name = v.farm_name
JOIN crops c ON c.crop_name = v.crop_name
WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.zone_name = v.zone_name);


INSERT INTO sensors (zone_id, sensor_code)
SELECT z.zone_id, v.code_prefix || '-' || gs.n
FROM (VALUES
    ('Tomato-01',      'TM-01',      2),
    ('Tomato-02',      'TM-02',      3),
    ('Tomato-03',      'TM-03',      4),
    ('Watermelon-01',  'WM-01',      4),
    ('Watermelon-02',  'WM-02',      5),
    ('Watermelon-03',  'WM-03',      6),
    ('Mango-01',       'MG-01',      8),
    ('Mango-02',       'MG-02',      9),
    ('Mango-03',       'MG-03',     11),
    ('Sesame-01',      'SM-01',      2),
    ('Sesame-02',      'SM-02',      3),
    ('Sesame-03',      'SM-03',      4),
    ('Cotton-01',      'CT-01',      4),
    ('Cotton-02',      'CT-02',      4),
    ('Cotton-03',      'CT-03',      5),
    ('Millet-01',      'ML-01',      3),
    ('Millet-02',      'ML-02',      4),
    ('Millet-03',      'ML-03',      5),
    ('Banana-01',      'BN-01',      4),
    ('Banana-02',      'BN-02',      5),
    ('Banana-03',      'BN-03',      6),
    ('Sorghum-01',     'SG-01',      4),
    ('Sorghum-02',     'SG-02',      5),
    ('Sorghum-03',     'SG-03',      5),
    ('DatePalm-01',    'DP-01',      5),
    ('DatePalm-02',    'DP-02',      6),
    ('DatePalm-03',    'DP-03',      7),
    ('Wheat-AJ-01',    'WH-AJ-01',   7),
    ('Barley-AJ-01',   'BR-AJ-01',   5),
    ('Maize-AJ-01',    'MZ-AJ-01',   6),
    ('Wheat-AJ-02',    'WH-AJ-02',   8),
    ('Barley-AJ-02',   'BR-AJ-02',   6),
    ('Maize-AJ-02',    'MZ-AJ-02',   7),
    ('Wheat-AJ-03',    'WH-AJ-03',   6),
    ('Barley-AJ-03',   'BR-AJ-03',   4),
    ('Maize-AJ-03',    'MZ-AJ-03',   5)
) AS v(zone_name, code_prefix, sensor_count)
JOIN zones z ON z.zone_name = v.zone_name
CROSS JOIN LATERAL generate_series(1, v.sensor_count) AS gs(n)
ON CONFLICT (sensor_code) DO NOTHING;

