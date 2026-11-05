"""Login and single-use refresh rotation. All persisted tokens are SHA-256 hashes."""
from datetime import datetime, timedelta, timezone
import secrets
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from . import db
from .security import account, hash_password, token_hash, user_account, verify_password

router = APIRouter(tags=['authentication'])
ACCESS_SECONDS = 900
REFRESH_SECONDS = 30 * 86400
# Similar work for an unknown email avoids a trivial password-hash timing leak.
_DUMMY_HASH = hash_password(secrets.token_urlsafe(32))


class Login(BaseModel):
    model_config = ConfigDict(extra='forbid')
    email: str = Field(min_length=3, max_length=254)
    password: str = Field(min_length=1, max_length=1024)


class Refresh(BaseModel):
    model_config = ConfigDict(extra='forbid')
    refresh_token: str = Field(min_length=10, max_length=256)


def _issue(conn, user, family_id=None):
    access = 'sa_' + secrets.token_urlsafe(48)
    refresh = 'sr_' + secrets.token_urlsafe(48)
    now = datetime.now(timezone.utc)
    db.query('INSERT INTO auth_sessions(id,family_id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) '
             'VALUES (%s,%s,%s,%s,%s,%s,%s)',
             (str(uuid4()), family_id or str(uuid4()), user['id'], token_hash(access), token_hash(refresh),
              now + timedelta(seconds=ACCESS_SECONDS), now + timedelta(seconds=REFRESH_SECONDS)), conn=conn)
    return {'access_token': access, 'refresh_token': refresh, 'expires_in': ACCESS_SECONDS,
            'token_type': 'bearer', 'user': user_account(user, conn=conn)}


@router.post('/auth/login')
def login(payload: Login):
    result = None
    email = payload.email.strip().lower()
    now = datetime.now(timezone.utc)
    with db.connect() as conn:
        key = token_hash(email)
        db.query('INSERT INTO auth_login_attempts(email_hash) VALUES (%s) ON CONFLICT DO NOTHING', (key,), conn=conn)
        attempt = db.query('SELECT * FROM auth_login_attempts WHERE email_hash=%s FOR UPDATE', (key,), conn=conn)[0]
        if attempt['blocked_until'] is None or attempt['blocked_until'] <= now:
            rows = db.query('SELECT * FROM app_users WHERE email=%s', (email,), conn=conn)
            row = rows[0] if rows else None
            valid = verify_password(payload.password, row['password_hash'] if row else _DUMMY_HASH)
            if row and valid and row['is_active']:
                db.query('DELETE FROM auth_login_attempts WHERE email_hash=%s', (key,), conn=conn)
                result = _issue(conn, row)
                db.audit(conn, row['id'], 'auth.login', target_type='user', target_id=row['id'])
            else:
                count = (attempt['failed_count'] if attempt['last_failed_at'] is not None
                         and now - attempt['last_failed_at'] < timedelta(minutes=15) else 0) + 1
                blocked = now + timedelta(minutes=15) if count >= 5 else None
                db.query('UPDATE auth_login_attempts SET failed_count=%s,last_failed_at=%s,blocked_until=%s '
                         'WHERE email_hash=%s', (count, now, blocked, key), conn=conn)
    # Failed-attempt counters must commit before the generic error is raised.
    if result is None:
        raise HTTPException(401, 'Invalid email or password')
    return result


@router.post('/auth/refresh')
def refresh(payload: Refresh):
    if not payload.refresh_token.startswith('sr_'):
        raise HTTPException(401, 'Invalid refresh token')
    result = None
    with db.connect() as conn:
        rows = db.query('SELECT s.*,u.email,u.full_name,u.platform_role,u.is_active FROM auth_sessions s '
                        'JOIN app_users u ON u.id=s.user_id WHERE s.refresh_hash=%s FOR UPDATE OF s',
                        (token_hash(payload.refresh_token),), conn=conn)
        if rows:
            row = rows[0]
            if row['revoked_at'] is not None:
                # A reused token revokes descendants too. Commit this change
                # before raising, otherwise transaction rollback would undo it.
                db.query('UPDATE auth_sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE family_id=%s',
                         (str(row['family_id']),), conn=conn)
                db.audit(conn, row['user_id'], 'auth.refresh_reuse', target_type='session', target_id=row['id'])
            elif row['is_active'] and row['refresh_expires_at'] > datetime.now(timezone.utc):
                db.query('UPDATE auth_sessions SET revoked_at=now() WHERE id=%s', (str(row['id']),), conn=conn)
                user = {**row, 'id': row['user_id']}
                result = _issue(conn, user, str(row['family_id']))
                db.audit(conn, row['user_id'], 'auth.refresh', target_type='session', target_id=row['id'])
    if result is None:
        raise HTTPException(401, 'Refresh token expired or revoked')
    return result


@router.post('/auth/logout')
def logout(user=Depends(account), payload: Refresh | None = None):
    # The caller must own the family being revoked; a leaked refresh token from
    # another account cannot be used to revoke its sessions through this route.
    if user.get('legacy'):
        return {'status': 'ok', 'message': 'Legacy tokens must be removed from deployment configuration'}
    with db.connect() as conn:
        if payload is not None:
            db.query('UPDATE auth_sessions SET revoked_at=COALESCE(revoked_at,now()) '
                     'WHERE user_id=%s AND family_id IN '
                     '(SELECT family_id FROM auth_sessions WHERE user_id=%s AND refresh_hash=%s)',
                     (user['id'], user['id'], token_hash(payload.refresh_token)), conn=conn)
        else:
            db.query('UPDATE auth_sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE user_id=%s',
                     (user['id'],), conn=conn)
        db.audit(conn, user['id'], 'auth.logout', target_type='user', target_id=user['id'])
    return {'status': 'ok'}


@router.get('/me')
def me(user=Depends(account)):
    return {key: value for key, value in user.items() if key != 'legacy'}
