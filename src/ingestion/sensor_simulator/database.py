import logging
import time
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool as pg_pool

try:
    from .config import POSTGRES_CONFIG
except ImportError:
    from config import POSTGRES_CONFIG

logger = logging.getLogger("database")

CONNECT_RETRY_ATTEMPTS = 5
CONNECT_RETRY_BASE_BACKOFF_SECONDS = 2


class Database:

    def __init__(self, minconn=1, maxconn=3):
        self.minconn = minconn
        self.maxconn = maxconn
        self.pool = self._connect_with_retry(minconn, maxconn)

    def _connect_with_retry(self, minconn=None, maxconn=None):
        minconn = self.minconn if minconn is None else minconn
        maxconn = self.maxconn if maxconn is None else maxconn
        attempt = 0
        while True:
            try:
                pool = pg_pool.SimpleConnectionPool(minconn, maxconn, **POSTGRES_CONFIG)
                logger.info("postgres_pool_created minconn=%d maxconn=%d", minconn, maxconn)
                return pool
            except psycopg2.OperationalError as exc:
                attempt += 1
                if attempt > CONNECT_RETRY_ATTEMPTS:
                    logger.error("postgres_connect_failed_permanently error=%s", exc)
                    raise
                wait_seconds = CONNECT_RETRY_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "postgres_connect_failed attempt=%d/%d retrying_in=%ds error=%s",
                    attempt, CONNECT_RETRY_ATTEMPTS, wait_seconds, exc,
                )
                time.sleep(wait_seconds)

    @contextmanager
    def _cursor(self):
        try:
            if self.pool is None or getattr(self.pool, "closed", False):
                raise psycopg2.OperationalError("PostgreSQL connection pool is closed")
            conn = self.pool.getconn()
        except (psycopg2.OperationalError, pg_pool.PoolError) as exc:
            logger.warning("postgres_getconn_failed, re-initializing pool: %s", exc)
            self.pool = self._connect_with_retry(self.minconn, self.maxconn)
            conn = self.pool.getconn()

        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            try:
                self.pool.putconn(conn, close=bool(conn.closed))
            except Exception as exc:
                logger.warning("postgres_putconn_failed: %s", exc)

    def load_sensors(self):
        query = """
        SELECT
            s.sensor_id,
            s.sensor_code,
            z.zone_id,
            z.zone_name,
            z.soil_type,
            c.crop_id,
            c.crop_name,
            f.farm_id,
            f.farm_name,
            f.city
        FROM sensors s
        JOIN zones z ON s.zone_id = z.zone_id
        JOIN crops c ON z.crop_id = c.crop_id
        JOIN farms f ON z.farm_id = f.farm_id
        ORDER BY s.sensor_id;
        """
        with self._cursor() as cursor:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    def load_crop_config(self):
        query = """
        SELECT
            crop_id,
            crop_name,
            optimal_moisture_min,
            optimal_moisture_max,
            critical_moisture_low,
            critical_moisture_high,
            soil_temp_min,
            soil_temp_max,
            air_temp_min,
            air_temp_max,
            humidity_min,
            humidity_max
        FROM crops
        ORDER BY crop_id;
        """
        with self._cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        crop_config = {}
        incomplete_crops = []

        for row in rows:
            (crop_id, name, moisture_min, moisture_max, crit_low, crit_high,
             soil_min, soil_max, air_min, air_max, hum_min, hum_max) = row

            required_fields = [moisture_min, moisture_max, crit_low, crit_high,
                                soil_min, soil_max, air_min, air_max, hum_min, hum_max]

            if any(field is None for field in required_fields):
                incomplete_crops.append(name)
                continue

            if not (0 <= crit_low <= moisture_min < moisture_max <= crit_high <= 100
                    and -10 <= soil_min < soil_max <= 70
                    and -10 <= air_min < air_max <= 70
                    and 0 <= hum_min < hum_max <= 100):
                raise RuntimeError(f"Invalid configured ranges for crop {name}")

            crop_config[crop_id] = {
                "name": name,
                "optimal_moisture": (float(moisture_min), float(moisture_max)),
                "critical_low": float(crit_low),
                "critical_high": float(crit_high),
                "soil_temp": (float(soil_min), float(soil_max)),
                "air_temp": (float(air_min), float(air_max)),
                "humidity": (float(hum_min), float(hum_max)),
            }

        if incomplete_crops:
            raise RuntimeError(
                f"crops {', '.join(incomplete_crops)} missing required config fields"
            )

        return crop_config

    def load_runtime_settings(self):
        try:
            with self._cursor() as cursor:
                cursor.execute("SELECT key,value FROM system_settings WHERE key LIKE 'simulator.%'")
                return dict(cursor.fetchall())
        except psycopg2.errors.UndefinedTable:
            # The simulator can run before the application schema migration.
            return {}

    def close(self):
        if self.pool and not getattr(self.pool, "closed", False):
            self.pool.closeall()
            logger.info("postgres_pool_closed")
