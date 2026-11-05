"""Database-backed opaque sessions and farm permissions; no trusted client roles."""
import base64
import hashlib
import hmac
import json
import os
import secrets

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from . import db

PASSWORD_ITERATIONS = 600_000
ADMIN_CAPABILITIES = ['manage_users', 'manage_farms', 'manage_devices', 'manage_settings', 'view_audit',
                      'manage_system', 'manage_catalog', 'control_irrigation', 'resolve_alerts']
bearer = HTTPBearer(auto_error=False)


def hash_password(password):
    if not isinstance(password, str) or not 12 <= len(password) <= 1024:
        raise ValueError('Password must contain 12 to 1024 characters')
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, PASSWORD_ITERATIONS)
    return f'pbkdf2_sha256${PASSWORD_ITERATIONS}${base64.b64encode(salt).decode()}${base64.b64encode(digest).decode()}'


def verify_password(password, encoded):
    try:
        algorithm, iterations, salt, digest = encoded.split('$')
        iterations = int(iterations)
        if algorithm != 'pbkdf2_sha256' or not 600_000 <= iterations <= 2_000_000 or len(password) > 1024:
            return False
        salt, digest = base64.b64decode(salt, validate=True), base64.b64decode(digest, validate=True)
        if len(salt) != 16 or len(digest) != 32:
            return False
        actual = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, iterations)
        return hmac.compare_digest(actual, digest)
    except (ValueError, TypeError, AttributeError):
        return False


def token_hash(token):
    return hashlib.sha256(token.encode('utf-8')).hexdigest()


def user_account(row, conn=None):
    memberships = db.query('SELECT farm_id,role FROM farm_memberships WHERE user_id=%s ORDER BY farm_id',
                           (row['id'],), conn=conn)
    if row['platform_role'] == 'admin':
        farms = db.query('SELECT farm_id FROM farms ORDER BY farm_id', conn=conn)
        roles = {int(farm['farm_id']): 'owner' for farm in farms}
    else:
        roles = {int(item['farm_id']): item['role'] for item in memberships}
    return {'id': str(row['id']), 'email': row['email'], 'full_name': row['full_name'],
            'platform_role': row['platform_role'], 'farm_ids': list(roles), 'farm_roles': roles,
            'capabilities': (list(ADMIN_CAPABILITIES) if row['platform_role'] == 'admin' else
                             ['control_irrigation', 'resolve_alerts']
                             if any(role in ('owner', 'manager') for role in roles.values()) else [])}


def legacy_account(token):
    try:
        accounts = json.loads(os.getenv('API_ACCOUNTS_JSON', '[]'))
        if not isinstance(accounts, list):
            raise ValueError('Invalid legacy account configuration')
        if not accounts and os.getenv('API_TOKEN'):
            accounts = [{'token': os.environ['API_TOKEN'], 'id': 'configured-user',
                         'email': os.getenv('API_USER_EMAIL', ''),
                         'full_name': os.getenv('API_USER_NAME', 'Farm viewer'),
                         'farm_ids': [int(x) for x in os.getenv('API_FARM_IDS', '').split(',') if x.strip()]}]
        for item in accounts:
            expected = item.get('token')
            if isinstance(expected, str) and expected and hmac.compare_digest(token.encode(), expected.encode()):
                farms = [int(farm) for farm in item.get('farm_ids', [])]
                return {'id': str(item.get('id', 'legacy-viewer')), 'email': item.get('email', ''),
                        'full_name': item.get('full_name', 'Farm viewer'), 'platform_role': 'user',
                        'farm_ids': farms, 'farm_roles': {farm: 'viewer' for farm in farms}, 'capabilities': [],
                        'legacy': True}
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(503, 'Authentication configuration unavailable') from None
    return None


def account(credentials: HTTPAuthorizationCredentials = Depends(bearer)):
    if credentials is None or credentials.scheme.lower() != 'bearer':
        raise HTTPException(401, 'Authentication required')
    token = credentials.credentials
    legacy = legacy_account(token)
    if legacy is not None:
        return legacy
    if not token.startswith('sa_') or len(token) > 256:
        raise HTTPException(401, 'Invalid access token')
    with db.connect() as conn:
        rows = db.query('SELECT u.* FROM auth_sessions s JOIN app_users u ON u.id=s.user_id '
                        'WHERE s.access_hash=%s AND s.revoked_at IS NULL AND s.access_expires_at>now() '
                        'AND u.is_active', (token_hash(token),), conn=conn)
        if not rows:
            raise HTTPException(401, 'Access token expired or revoked')
        return user_account(rows[0], conn=conn)


current_account = account


def require_admin(user=Depends(account)):
    if user.get('platform_role') != 'admin' or user.get('legacy'):
        raise HTTPException(403, 'Administrator access required')
    return user


def require_farm(user, farm_id, roles=('owner', 'manager', 'viewer')):
    role = user.get('farm_roles', {}).get(int(farm_id), user.get('farm_roles', {}).get(str(farm_id)))
    if int(farm_id) not in user.get('farm_ids', []) or role not in roles:
        raise HTTPException(403, 'Farm permission denied')
    return user
