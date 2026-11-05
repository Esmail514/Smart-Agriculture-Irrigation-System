"""High-performance transactional PostgreSQL boundary with optional connection pooling."""
import logging
import os
import threading
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import Json, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from fastapi import HTTPException

logger = logging.getLogger(__name__)

_pool = None
_pool_lock = threading.Lock()


def init_pool(minconn=None, maxconn=None):
    """Initialize the shared thread-safe connection pool for production API use."""
    global _pool
    with _pool_lock:
        if _pool is None:
            minconn = minconn or int(os.getenv('POSTGRES_POOL_MIN', '2'))
            maxconn = maxconn or int(os.getenv('POSTGRES_POOL_MAX', '20'))
            try:
                _pool = ThreadedConnectionPool(
                    minconn=minconn,
                    maxconn=maxconn,
                    host=os.getenv('POSTGRES_HOST', 'postgres'),
                    port=os.getenv('POSTGRES_PORT', '5432'),
                    dbname=os.getenv('POSTGRES_DB', 'smart_agriculture'),
                    user=os.getenv('POSTGRES_USER', 'postgres'),
                    password=os.getenv('POSTGRES_PASSWORD', ''),
                    connect_timeout=5,
                    cursor_factory=RealDictCursor,
                    options='-c statement_timeout=10000',
                )
                logger.info("PostgreSQL connection pool initialized (min=%d, max=%d)", minconn, maxconn)
            except Exception as e:
                _pool = None
                logger.warning("PostgreSQL connection pool initialization deferred: %s", e)
    return _pool


def close_pool():
    """Cleanly close all pooled connections on server shutdown."""
    global _pool
    with _pool_lock:
        if _pool is not None:
            try:
                _pool.closeall()
            except Exception as e:
                logger.warning("Error closing connection pool: %s", e)
            _pool = None


def get_pool():
    return _pool


@contextmanager
def connect():
    connection = None
    from_pool = False
    try:
        if _pool is not None:
            try:
                connection = _pool.getconn()
                from_pool = True
            except Exception as pool_err:
                logger.warning("Failed to obtain connection from pool, falling back: %s", pool_err)
                connection = None
                from_pool = False

        if connection is None:
            connection = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                port=os.getenv('POSTGRES_PORT', '5432'),
                dbname=os.getenv('POSTGRES_DB', 'smart_agriculture'),
                user=os.getenv('POSTGRES_USER', 'postgres'),
                password=os.getenv('POSTGRES_PASSWORD', ''),
                connect_timeout=5,
                cursor_factory=RealDictCursor,
                options='-c statement_timeout=10000',
            )

        with connection:
            yield connection
    except psycopg2.IntegrityError:
        raise HTTPException(409, 'Database constraint conflict') from None
    except psycopg2.Error:
        logger.warning('Application database operation failed')
        raise HTTPException(503, 'Application database unavailable') from None
    finally:
        if connection is not None:
            if from_pool and _pool is not None:
                try:
                    _pool.putconn(connection)
                except Exception:
                    try:
                        connection.close()
                    except Exception:
                        pass
            else:
                connection.close()


def query(sql, params=(), conn=None):
    if conn is None:
        with connect() as connection:
            return query(sql, params, conn=connection)
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()] if cursor.description else []


def audit(conn, actor_id, action, *, farm_id=None, target_type=None, target_id=None, details=None):
    # Legacy viewer identifiers are strings, not rows in app_users. They never
    # authorize writes; optional read events retain their identity in details.
    actor = int(actor_id) if actor_id is not None and str(actor_id).isdigit() else None
    rows = query('INSERT INTO audit_events(actor_id,farm_id,action,target_type,target_id,details) '
                 'VALUES (%s,%s,%s,%s,%s,%s) RETURNING id',
                 (actor, farm_id, action, target_type, str(target_id) if target_id is not None else None,
                  Json(details or {})), conn=conn)
    return rows[0]['id']


