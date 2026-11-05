"""Session lifecycle and permissions with a transactional in-memory DB boundary."""
import copy
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.testclient import TestClient

from src.api import auth_routes as auth, security

PASSWORD = 'example-secret-password!'
PASSWORD_HASH = security.hash_password(PASSWORD)


class Store:
    def __init__(self):
        self.users = {1: {'id': 1, 'email': 'owner@example.test', 'full_name': 'Owner',
                         'password_hash': PASSWORD_HASH, 'is_active': True, 'platform_role': 'user'}}
        self.sessions, self.attempts, self.events = {}, {}, []
        self.memberships = [{'farm_id': 1, 'role': 'owner'}, {'farm_id': 2, 'role': 'viewer'}]
        self.lock = threading.RLock()
        self.statements = []

    @contextmanager
    def connect(self):
        # Like PostgreSQL row locks, serializes concurrent rotations. Restore
        # state on raised errors so tests catch revocations accidentally rolled
        # back by raising HTTPException before leaving the transaction.
        with self.lock:
            original = copy.deepcopy((self.sessions, self.attempts, self.events))
            try:
                yield self
            except Exception:
                self.sessions, self.attempts, self.events = original
                raise

    def query(self, sql, params=(), conn=None):
        self.statements.append(sql)
        if sql.startswith('INSERT INTO auth_login_attempts'):
            self.attempts.setdefault(params[0], {'failed_count': 0, 'last_failed_at': None, 'blocked_until': None})
        elif sql.startswith('SELECT * FROM auth_login_attempts'):
            return [dict(self.attempts[params[0]])]
        elif sql.startswith('UPDATE auth_login_attempts'):
            self.attempts[params[3]] = dict(zip(('failed_count','last_failed_at','blocked_until'), params[:3]))
        elif sql.startswith('DELETE FROM auth_login_attempts'):
            self.attempts.pop(params[0], None)
        elif sql.startswith('SELECT * FROM app_users'):
            return [dict(u) for u in self.users.values() if u['email'] == params[0]]
        elif sql.startswith('SELECT farm_id,role'):
            return copy.deepcopy(self.memberships)
        elif sql.startswith('SELECT farm_id FROM farms'):
            return [{'farm_id': 1}, {'farm_id': 2}]
        elif sql.startswith('INSERT INTO auth_sessions'):
            row = dict(zip(('id','family_id','user_id','access_hash','refresh_hash','access_expires_at','refresh_expires_at'), params))
            row['revoked_at'] = None
            self.sessions[row['id']] = row
        elif sql.startswith('SELECT s.*,u.email'):
            assert 'FOR UPDATE OF s' in sql
            return [{**s, **{k: v for k,v in self.users[s['user_id']].items() if k != 'id'}}
                    for s in self.sessions.values() if s['refresh_hash'] == params[0]]
        elif sql.startswith('SELECT u.* FROM auth_sessions'):
            return [dict(self.users[s['user_id']]) for s in self.sessions.values()
                    if s['access_hash'] == params[0] and s['revoked_at'] is None
                    and s['access_expires_at'] > datetime.now(timezone.utc) and self.users[s['user_id']]['is_active']]
        elif sql.startswith('UPDATE auth_sessions SET revoked_at=now() WHERE id='):
            self.sessions[params[0]]['revoked_at'] = datetime.now(timezone.utc)
        elif sql.startswith('UPDATE auth_sessions SET revoked_at=COALESCE'):
            if 'WHERE family_id=' in sql:
                for s in self.sessions.values():
                    if s['family_id'] == params[0]:
                        s['revoked_at'] = s['revoked_at'] or datetime.now(timezone.utc)
            elif 'AND family_id IN' in sql:
                families = {s['family_id'] for s in self.sessions.values()
                            if str(s['user_id']) == str(params[1]) and s['refresh_hash'] == params[2]}
                for s in self.sessions.values():
                    if str(s['user_id']) == str(params[0]) and s['family_id'] in families:
                        s['revoked_at'] = datetime.now(timezone.utc)
            else:
                for s in self.sessions.values():
                    if str(s['user_id']) == str(params[0]):
                        s['revoked_at'] = datetime.now(timezone.utc)
        else:
            raise AssertionError(f'Unexpected query: {sql}')
        return []


@pytest.fixture
def api(monkeypatch):
    store = Store()
    monkeypatch.delenv('API_TOKEN', raising=False)
    monkeypatch.delenv('API_ACCOUNTS_JSON', raising=False)
    monkeypatch.setattr(auth.db, 'connect', store.connect)
    monkeypatch.setattr(auth.db, 'query', store.query)
    monkeypatch.setattr(auth.db, 'audit', lambda conn, actor_id, action, **kwargs: store.events.append(action))
    app = FastAPI()
    app.include_router(auth.router)
    return TestClient(app), store


def sign_in(client):
    response = client.post('/auth/login', json={'email': 'OWNER@example.test', 'password': PASSWORD})
    assert response.status_code == 200, response.text
    return response.json()


def bearer(token):
    return {'Authorization': 'Bearer ' + token}


def test_password_salted_pbkdf2_and_malformed_hashes():
    other = security.hash_password(PASSWORD)
    assert other != PASSWORD_HASH
    assert '$600000$' in other
    assert security.verify_password(PASSWORD, other)
    assert not security.verify_password('incorrect', other)
    for encoded in ['bad', 'pbkdf2_sha256$0$a$b', 'pbkdf2_sha256$999999999$a$b']:
        assert not security.verify_password(PASSWORD, encoded)
    with pytest.raises(ValueError):
        security.hash_password('short')


def test_login_persists_only_hashes_and_me_permissions(api):
    client, store = api
    result = sign_in(client)
    session = next(iter(store.sessions.values()))
    assert result['access_token'] not in session.values()
    assert result['refresh_token'] not in session.values()
    assert session['access_hash'] == security.token_hash(result['access_token'])
    me = client.get('/me', headers=bearer(result['access_token']))
    assert me.status_code == 200
    assert me.json()['farm_roles'] == {'1': 'owner', '2': 'viewer'}
    assert 'control_irrigation' in me.json()['capabilities']
    assert 'password_hash' not in me.json()


def test_unauthenticated_invalid_expired_and_disabled_fail(api):
    client, store = api
    assert client.get('/me').status_code == 401
    assert client.get('/me', headers=bearer('wrong')).status_code == 401
    result = sign_in(client)
    session = next(iter(store.sessions.values()))
    session['access_expires_at'] = datetime.now(timezone.utc) - timedelta(seconds=1)
    assert client.get('/me', headers=bearer(result['access_token'])).status_code == 401
    session['access_expires_at'] = datetime.now(timezone.utc) + timedelta(minutes=1)
    store.users[1]['is_active'] = False
    assert client.get('/me', headers=bearer(result['access_token'])).status_code == 401


def test_rotation_revokes_previous_access_and_reuse_revokes_family(api):
    client, store = api
    first = sign_in(client)
    rotated = client.post('/auth/refresh', json={'refresh_token': first['refresh_token']})
    assert rotated.status_code == 200
    second = rotated.json()
    assert second['refresh_token'] != first['refresh_token']
    assert client.get('/me', headers=bearer(first['access_token'])).status_code == 401
    assert client.get('/me', headers=bearer(second['access_token'])).status_code == 200
    assert client.post('/auth/refresh', json={'refresh_token': first['refresh_token']}).status_code == 401
    assert all(s['revoked_at'] is not None for s in store.sessions.values())
    assert client.get('/me', headers=bearer(second['access_token'])).status_code == 401
    assert 'auth.refresh_reuse' in store.events


def test_concurrent_refresh_has_no_two_valid_descendants(api):
    client, store = api
    first = sign_in(client)
    def rotate():
        try:
            return auth.refresh(auth.Refresh(refresh_token=first['refresh_token']))
        except HTTPException as error:
            return error.status_code
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: rotate(), range(2)))
    assert sum(isinstance(result, dict) for result in results) == 1
    assert 401 in results
    assert all(s['revoked_at'] is not None for s in store.sessions.values())


def test_expired_refresh_never_issues_session(api):
    client, store = api
    result = sign_in(client)
    next(iter(store.sessions.values()))['refresh_expires_at'] = datetime.now(timezone.utc) - timedelta(seconds=1)
    assert client.post('/auth/refresh', json={'refresh_token': result['refresh_token']}).status_code == 401
    assert len(store.sessions) == 1


def test_logout_revokes_persisted_family(api):
    client, store = api
    result = sign_in(client)
    response = client.post('/auth/logout', headers=bearer(result['access_token']),
                           json={'refresh_token': result['refresh_token']})
    assert response.status_code == 200
    assert client.get('/me', headers=bearer(result['access_token'])).status_code == 401
    assert client.post('/auth/refresh', json={'refresh_token': result['refresh_token']}).status_code == 401


def test_legacy_config_cannot_elevate_to_admin_or_farm_manager(monkeypatch):
    monkeypatch.setenv('API_ACCOUNTS_JSON', json.dumps([{'token': 'legacy', 'id': '1', 'farm_ids': [1],
                         'platform_role': 'admin', 'farm_roles': {'1':'owner'}, 'capabilities':['manage_system']}]))
    user = security.account(HTTPAuthorizationCredentials(scheme='Bearer', credentials='legacy'))
    assert user['platform_role'] == 'user' and user['capabilities'] == []
    security.require_farm(user, 1)
    for action in (lambda: security.require_admin(user), lambda: security.require_farm(user, 1, roles=('owner','manager')),
                   lambda: security.require_farm(user, 2)):
        with pytest.raises(HTTPException) as error:
            action()
        assert error.value.status_code == 403


def test_extra_fields_forbidden_and_login_lockout_commits(api, monkeypatch):
    client, store = api
    assert client.post('/auth/login', json={'email':'owner@example.test','password':PASSWORD,'platform_role':'admin'}).status_code == 422
    for _ in range(5):
        assert client.post('/auth/login', json={'email':'owner@example.test','password':'bad'}).status_code == 401
    monkeypatch.setattr(auth, 'verify_password', lambda *args: (_ for _ in ()).throw(AssertionError('locked login must not compute PBKDF2')))
    assert client.post('/auth/login', json={'email':'owner@example.test','password':PASSWORD}).status_code == 401
    attempt = next(iter(store.attempts.values()))
    assert attempt['failed_count'] == 5
    assert attempt['blocked_until'] > datetime.now(timezone.utc)


def test_lockout_can_expire_and_success_clears_failures(api):
    client, store = api
    key = security.token_hash('owner@example.test')
    store.attempts[key] = {'failed_count': 5,
        'last_failed_at': datetime.now(timezone.utc) - timedelta(minutes=16),
        'blocked_until': datetime.now(timezone.utc) - timedelta(seconds=1)}
    sign_in(client)
    assert key not in store.attempts


def test_unknown_and_inactive_accounts_share_generic_failure(api):
    client, store = api
    unknown = client.post('/auth/login', json={'email':'unknown@example.test','password':PASSWORD})
    store.users[1]['is_active'] = False
    inactive = client.post('/auth/login', json={'email':'owner@example.test','password':PASSWORD})
    assert unknown.status_code == inactive.status_code == 401
    assert unknown.json() == inactive.json() == {'detail':'Invalid email or password'}
    assert not store.sessions
    assert all(len(key) == 64 and '@' not in key for key in store.attempts)


def test_database_constraint_errors_are_conflicts_not_unavailable(monkeypatch):
    import psycopg2
    from src.api import db
    from unittest.mock import MagicMock
    connection = MagicMock()
    monkeypatch.setattr(db.psycopg2, 'connect', lambda **kwargs: connection)
    with pytest.raises(HTTPException) as error:
        with db.connect():
            raise psycopg2.IntegrityError('sensitive constraint and row data')
    assert error.value.status_code == 409
    assert error.value.detail == 'Database constraint conflict'
    connection.close.assert_called_once()


def test_existing_sessions_observe_membership_demotion_immediately(api):
    client, store = api
    result = sign_in(client)
    store.memberships = [{'farm_id': 1, 'role': 'viewer'}]
    user = security.account(HTTPAuthorizationCredentials(scheme='Bearer', credentials=result['access_token']))
    assert user['capabilities'] == []
    with pytest.raises(HTTPException) as error:
        security.require_farm(user, 1, roles=('owner','manager'))
    assert error.value.status_code == 403
    with pytest.raises(HTTPException):
        security.require_farm(user, 2)


def test_crafted_legacy_roles_denied_at_real_admin_and_farm_write_routes(monkeypatch):
    from src.api import control_routes
    app = FastAPI()
    app.include_router(control_routes.router)
    client = TestClient(app)
    monkeypatch.setenv('API_ACCOUNTS_JSON', json.dumps([{'token':'crafted-legacy','id':'1','farm_ids':[1],
                         'platform_role':'admin','farm_roles':{'1':'owner'},'capabilities':['manage_system']}]))
    monkeypatch.setattr(auth.db, 'connect', lambda: (_ for _ in ()).throw(AssertionError('Unauthorized request reached DB')))
    header = bearer('crafted-legacy')
    assert client.get('/admin/users', headers=header).status_code == 403
    assert client.post('/admin/system/services/kafka/restart', headers=header).status_code == 403
    assert client.post('/farms/1/irrigation-commands', headers=header,
                       json={'device_id':'pump1','duration_seconds':60,'idempotency_key':'request123'}).status_code == 403
    assert client.patch('/farms/1/alerts/1', headers=header,
                       json={'resolved':True,'event_time':'2026-10-11T00:00:00Z'}).status_code == 403
