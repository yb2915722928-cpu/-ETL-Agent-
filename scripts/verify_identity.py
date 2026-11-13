"""V01 public HTTP verification, against real PostgreSQL and Redis only.

Run once, restart the services, then run --recheck to prove persistence through
public reads and the original password. Generated probe secrets stay in .runtime.
"""
import argparse
from datetime import datetime, timezone
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import urllib.error
import urllib.request
import uuid


class Client:
    def __init__(self, url, forbidden_plaintext):
        self.url = url.rstrip('/')
        self.secrets = forbidden_plaintext
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def call(self, method, path, body=None, expected=200):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.url + path, data=data, method=method,
                                         headers={'Content-Type': 'application/json'})
        try:
            response = self.opener.open(request, timeout=15)
        except urllib.error.HTTPError as error:
            response = error
        status = response.code
        raw = response.read().decode()
        assert not any(secret and secret in raw for secret in self.secrets), 'Sensitive plaintext in API response'
        assert status == expected, f'{method} {path}: expected {expected}, got {status}: {raw}'
        return json.loads(raw)

    def login(self, credentials, username):
        self.call('POST', '/api/login', {'tenant': credentials['tenant'], 'username': username,
                  'password': credentials['accounts'][username]['password']})
        return self.call('GET', '/api/me')


def private_write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open('w', encoding='utf-8') as handle:
        os.chmod(path, 0o600)
        json.dump(content, handle, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:8000')
    parser.add_argument('--credentials', type=Path, default=Path('.runtime/bootstrap-demo.json'))
    parser.add_argument('--other-credentials', type=Path, default=Path('.runtime/bootstrap-isolation.json'))
    parser.add_argument('--state', type=Path, default=Path('.runtime/verification-state.json'))
    parser.add_argument('--output', type=Path)
    parser.add_argument('--recheck', action='store_true')
    args = parser.parse_args()
    credentials = json.loads(args.credentials.read_text(encoding='utf-8'))
    other = json.loads(args.other_credentials.read_text(encoding='utf-8'))
    plaintext = [entry['password'] for source in (credentials, other) for entry in source['accounts'].values()]
    admin = Client(args.url, plaintext)
    health = admin.call('GET', '/api/health')
    assert health['status'] == 'ok' and health['postgresql']['query'] == 1 and health['redis']['ping'] is True
    me = admin.login(credentials, 'admin')
    tenant = me['tenant']['id']
    base = f'/api/tenants/{tenant}'
    project = credentials['project_id']
    members_path = f'{base}/projects/{project}/members'
    passed = ['health queries real PostgreSQL and Redis']

    if args.recheck:
        state = json.loads(args.state.read_text(encoding='utf-8'))
        plaintext.extend([state['password']])
        original = next(a for a in admin.call('GET', base + '/accounts') if a['id'] == state['account_id'])
        assert original['username'] == state['username'] and original['enabled'] is True
        membership = next(m for m in admin.call('GET', members_path) if m['id'] == state['account_id'])
        assert membership['roles'] == ['auditor']
        probe = Client(args.url, plaintext)
        probe.call('POST', '/api/login', {'tenant': credentials['tenant'], 'username': state['username'], 'password': state['password']})
        assert probe.call('GET', '/api/me')['account']['id'] == state['account_id']
        assert any(e['id'] == state['event_id'] for e in admin.call('GET', base + '/identity-events'))
        passed.append('same account ID, original password, membership roles and audit event survive service restart/bootstrap repeat')
    else:
        expected_roles = {'maker': 'maker', 'data_checker': 'data_checker', 'security_checker': 'security_checker',
                          'operator': 'operator', 'auditor': 'auditor'}
        for username, role in expected_roles.items():
            client = Client(args.url, plaintext)
            role_me = client.login(credentials, username)
            assert role_me['account']['id'] != me['account']['id']
            listing = client.call('GET', base + '/projects')
            own_project = next(p for p in listing if p['id'] == project)
            assert own_project['roles'] == [role] and own_project['can_manage_members'] is False
            client.call('GET', members_path)
            client.call('GET', base + '/accounts', expected=403)
            client.call('PUT', members_path + '/' + role_me['account']['id'], {'roles': ['project_admin']}, expected=403)
        passed.append('five independent named role accounts log in; read membership and cannot manage accounts/members')

        # Existing authenticated sessions never authorize by submitted tenant/project IDs.
        foreign = f"/api/tenants/{other['tenant_id']}"
        admin.call('GET', foreign + '/projects', expected=403)
        admin.call('GET', foreign + '/accounts', expected=403)
        admin.call('GET', foreign + f"/projects/{other['project_id']}/members", expected=403)
        admin.call('GET', base + f"/projects/{other['project_id']}/members", expected=403)
        admin.call('PUT', members_path + '/' + other['accounts']['maker']['id'], {'roles': ['maker']}, expected=404)
        admin.call('PATCH', base + '/accounts/' + other['accounts']['maker']['id'], {'enabled': False}, expected=404)
        suffix = uuid.uuid4().hex[:10]
        private = admin.call('POST', base + '/projects', {'slug': 'private_' + suffix, 'name': '隔离验证项目'}, expected=201)
        maker = Client(args.url, plaintext)
        maker.login(credentials, 'maker')
        assert private['id'] not in [p['id'] for p in maker.call('GET', base + '/projects')]
        maker.call('GET', base + f"/projects/{private['id']}/members", expected=403)
        passed.append('cross-tenant account/member IDs and foreign/unjoined project IDs rejected server-side')

        username = 'probe_' + suffix
        password = secrets.token_urlsafe(24)
        plaintext.append(password)
        probe_account = admin.call('POST', base + '/accounts', {'username': username, 'display_name': '权限持久化验证', 'password': password}, expected=201)
        account_id = probe_account['id']
        admin.call('PUT', members_path + '/' + account_id, {'roles': ['project_admin']})
        probe = Client(args.url, plaintext)
        probe.call('POST', '/api/login', {'tenant': credentials['tenant'], 'username': username, 'password': password})
        probe.call('GET', f'{base}/projects/{project}/eligible-accounts')
        probe.call('GET', base + '/accounts', expected=403)
        probe.call('PATCH', base + '/accounts/' + account_id, {'tenant_admin': True}, expected=403)
        admin.call('PUT', members_path + '/' + account_id, {'roles': ['auditor']})
        probe.call('GET', members_path)
        probe.call('GET', f'{base}/projects/{project}/eligible-accounts', expected=403)
        probe.call('PUT', members_path + '/' + account_id, {'roles': ['project_admin']}, expected=403)
        admin.call('DELETE', members_path + '/' + account_id)
        probe.call('GET', members_path, expected=403)
        assert project not in [p['id'] for p in probe.call('GET', base + '/projects')]
        admin.call('PUT', members_path + '/' + account_id, {'roles': ['auditor']})
        admin.call('PATCH', base + '/accounts/' + account_id, {'enabled': False})
        probe.call('GET', '/api/me', expected=401)
        probe.call('POST', '/api/login', {'tenant': credentials['tenant'], 'username': username, 'password': password}, expected=401)
        admin.call('PATCH', base + '/accounts/' + account_id, {'enabled': True, 'tenant_admin': True})
        probe.call('GET', base + '/accounts')
        admin.call('PATCH', base + '/accounts/' + account_id, {'tenant_admin': False})
        probe.call('GET', base + '/accounts', expected=403)
        passed.append('live project-admin downgrade/removal, account disable and tenant-admin revocation reject existing session immediately')

        sentinel = 'V01_SECRET_MUST_NEVER_BE_ECHOED_' + suffix
        plaintext.append(sentinel)
        admin.call('POST', '/api/login', {'tenant': credentials['tenant'], 'username': username, 'password': {'secret': sentinel}}, expected=422)
        admin.call('PATCH', base + '/accounts/' + account_id, {'password': sentinel * 10}, expected=422)
        before = admin.call('GET', base + '/identity-events')
        admin.call('POST', base + '/accounts', {'username': username, 'display_name': '重复账号', 'password': password}, expected=409)
        after = admin.call('GET', base + '/identity-events')
        assert len(before) == len(after), 'Failed transaction left an audit fact'
        assert [e['sequence'] for e in after] == list(range(1, len(after) + 1))
        assert all(e['actor_id'] and e['occurred_at'] for e in after)
        assert any(e['action'] == 'account.updated' and e['facts'].get('target_account_id') == account_id for e in after)
        passed.append('validation/response/audit plaintext scan; ordered actor-linked audit; rejected write leaves no success event')
        private_write(args.state, {'username': username, 'password': password, 'account_id': account_id,
                                  'event_id': after[-1]['id']})

    report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'phase': 'persistence_recheck' if args.recheck else 'identity',
              'passed': passed, 'health': health, 'tenant_id': tenant, 'project_id': project}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
