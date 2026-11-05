import logging
import time
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool as pg_pool

try:
    from .config import POSTGRES_CONFIG
except ImportError:
    from config import POSTGRES_CONFIG

logger = logging.getLogger("weather_database")

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
                self.pool = self._connect_with_retry(self.minconn, self.maxconn)
            conn = self.pool.getconn()
        except psycopg2.OperationalError as exc:
            logger.warning("pool_connection_broken_reconnecting error=%s", exc)
            self.pool = self._connect_with_retry(self.minconn, self.maxconn)
            conn = self.pool.getconn()

        try:
            with conn.cursor() as cur:
                yield cur
                conn.commit()
        except psycopg2.OperationalError:
            try:
                conn.rollback()
            except psycopg2.Error as rollback_err:
                logger.debug("rollback_failed error=%s", rollback_err)
            self.pool.putconn(conn, close=True)
            raise
        except Exception:
            try:
                conn.rollback()
            except psycopg2.Error as rollback_err:
                logger.debug("rollback_failed error=%s", rollback_err)
            self.pool.putconn(conn)
            raise
        else:
            self.pool.putconn(conn)

    def load_farms(self):
        with self._cursor() as cursor:
            cursor.execute(
                "SELECT farm_id, farm_name, latitude, longitude FROM farms ORDER BY farm_id"
            )
            columns = [column[0] for column in cursor.description]
            farms = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logger.info("farms_loaded count=%d", len(farms))
            return farms

    def close(self):
        if self.pool is not None and not getattr(self.pool, "closed", False):
            self.pool.closeall()
            logger.info("postgres_pool_closed")
