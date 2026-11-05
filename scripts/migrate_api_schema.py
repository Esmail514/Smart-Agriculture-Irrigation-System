"""Apply application tables explicitly and optionally bootstrap the first admin.

python scripts/migrate_api_schema.py --init-schema
Set BOOTSTRAP_ADMIN_EMAIL/PASSWORD/NAME, then add --bootstrap-admin.
No user or password is created unless explicitly requested.
"""
import argparse
import getpass
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.api import db
from src.api.security import hash_password


def bootstrap_admin(email, full_name, password):
    email = email.strip().lower()
    if '@' not in email or len(email) > 254:
        raise ValueError('A valid admin email is required')
    if not full_name.strip():
        raise ValueError('Admin full name is required')
    encoded = hash_password(password)
    with db.connect() as conn:
        # A transaction-scoped lock prevents two concurrent bootstrap commands
        # from racing to create different first administrators.
        db.query('SELECT pg_advisory_xact_lock(672310042)', conn=conn)
        if db.query("SELECT id FROM app_users WHERE platform_role='admin' AND is_active", conn=conn):
            raise ValueError('An active administrator already exists; use the administrator API')
        rows = db.query("INSERT INTO app_users(email,full_name,password_hash,platform_role) "
                        "VALUES (%s,%s,%s,'admin') RETURNING id", (email, full_name.strip(), encoded), conn=conn)
        db.audit(conn, rows[0]['id'], 'admin.bootstrap', target_type='user', target_id=rows[0]['id'])
        return rows[0]['id']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--init-schema', action='store_true')
    parser.add_argument('--bootstrap-admin', action='store_true')
    args = parser.parse_args()
    if not args.init_schema and not args.bootstrap_admin:
        parser.error('Choose --init-schema and/or --bootstrap-admin')
    if args.init_schema:
        schema = (ROOT / 'sql/postgres/init/02-api-schema.sql').read_text(encoding='utf-8')
        with db.connect() as conn:
            db.query(schema, conn=conn)
            db.query('ALTER TABLE irrigation_commands ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;', conn=conn)
            db.query('ALTER TABLE irrigation_commands ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;', conn=conn)
            db.query('ALTER TABLE irrigation_commands ADD COLUMN IF NOT EXISTS failure_reason TEXT;', conn=conn)
        print('Application schema initialized')
    if args.bootstrap_admin:
        email = os.getenv('BOOTSTRAP_ADMIN_EMAIL', '')
        name = os.getenv('BOOTSTRAP_ADMIN_NAME', '')
        if not email or not name:
            parser.error('Set BOOTSTRAP_ADMIN_EMAIL and BOOTSTRAP_ADMIN_NAME explicitly')
        password = os.getenv('BOOTSTRAP_ADMIN_PASSWORD') or getpass.getpass('Admin password (12+ characters): ')
        admin_id = bootstrap_admin(email, name, password)
        print(f'Initial administrator created: id={admin_id}')


if __name__ == '__main__':
    main()
