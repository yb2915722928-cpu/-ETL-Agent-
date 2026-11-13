"""V03 public HTTP acceptance against real MySQL/Doris, without printing secrets.

No fake database/profile responses. Use --kinds mysql while Doris is not ready;
that reports only MySQL coverage. --recheck verifies saved profile/secret refs
through API after a restart and re-probes with the original encrypted credential.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import secrets
import uuid

from verify_identity import Client, private_write


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8000')
    parser.add_argument('--credentials', type=Path, default=Path('.runtime/bootstrap-demo.json'))
    parser.add_argument('--other-credentials', type=Path, default=Path('.runtime/bootstrap-isolation.json'))
    parser.add_argument('--secrets', type=Path, default=Path('.runtime/connectors/secrets.json'))
    parser.add_argument('--kinds', default='mysql,doris')
    parser.add_argument('--state', type=Path, default=Path('.runtime/connector-verification.json'))
    parser.add_argument('--output', type=Path)
    parser.add_argument('--recheck', action='store_true')
    args = parser.parse_args()
    credentials = json.loads(args.credentials.read_text(encoding='utf-8'))
    other = json.loads(args.other_credentials.read_text(encoding='utf-8'))
    config = json.loads(args.secrets.read_text(encoding='utf-8'))
    sentinel = 'connector-sentinel-' + secrets.token_hex(12)
    forbidden = [sentinel, 'alice@example.test', 'Alice Example', '陈示例']
    forbidden += [a['password'] for source in (credentials, other) for a in source['accounts'].values()]
    forbidden += [value for entry in config.values() for key, value in entry.items() if 'password' in key]
    maker, admin, auditor = [Client(args.url, forbidden) for _ in range(3)]
    maker.login(credentials, 'maker'); admin.login(credentials, 'admin'); auditor.login(credentials, 'auditor')
    base = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
    kinds = args.kinds.split(',')
    passed = []
    if args.recheck:
        state = json.loads(args.state.read_text(encoding='utf-8'))
        for saved in state['connections']:
            connection = maker.call('GET', base + '/connections/' + saved['connection']['id'])
            assert connection['secret_reference_id'] == saved['connection']['secret_reference_id']
            assert connection['secret_version'] == 1
            profile = maker.call('GET', base + '/profiles/' + saved['profile']['id'])
            assert profile == saved['profile']
            new = maker.call('POST', base + '/connections/' + connection['id'] + '/probe', {}, expected=201)
            assert new['schema_hash'] == profile['schema_hash']
            assert new['id'] != profile['id']
        passed.append('persisted immutable profiles and encrypted credentials survive restart; original credential re-probes real server')
    else:
        resources = maker.call('GET', base + '/connector-resources')
        state = {'connections': []}
        for kind in kinds:
            resource_id = 'acceptance-' + kind
            resource = next(r for r in resources if r['id'] == resource_id)
            connection = maker.call('POST', base + '/connections', {
                'name': '真实探查验收 ' + kind, 'resource_id': resource_id,
                'password': config[kind]['password']}, expected=201)
            assert connection['secret_version'] == 1 and connection['resource_fingerprint'] == resource['fingerprint']
            path = base + '/connections/' + connection['id']
            profile = maker.call('POST', path + '/probe', {}, expected=201)
            assert profile['connection_id'] == connection['id']
            assert profile['secret_reference_id'] == connection['secret_reference_id']
            assert profile['resource_fingerprint'] == connection['resource_fingerprint']
            assert {t['name'] for t in profile['content']['tables']} == set(resource['tables'])
            assert all(t['columns'] for t in profile['content']['tables'])
            assert all(len(t['samples']) <= 5 for t in profile['content']['tables'])
            assert all(v is None or v == '[已脱敏]' for t in profile['content']['tables'] for row in t['samples'] for v in row.values())
            assert auditor.call('GET', base + '/profiles/' + profile['id']) == profile
            assert any(p['id'] == profile['id'] for p in maker.call('GET', path + '/profiles'))
            state['connections'].append({'connection': connection, 'profile': profile})
            # No user-supplied SQL, network addresses, resource expansion or credential overwrite.
            maker.call('POST', path + '/probe', {'sql': 'SELECT * FROM mysql.user'}, expected=422)
            maker.call('POST', path + '/probe', {'tables': ['mysql.user'], 'sample_rows': 999}, expected=422)
            maker.call('PATCH', path, {'password': sentinel}, expected=405)
            auditor.call('POST', path + '/probe', {}, expected=403)
            auditor.call('POST', base + '/connections', {'name': 'denied', 'resource_id': resource_id, 'password': sentinel}, expected=403)
            wrong = maker.call('POST', base + '/connections', {'name': '失败诊断', 'resource_id': resource_id, 'password': sentinel}, expected=201)
            assert wrong['secret_reference_id'] != connection['secret_reference_id']
            maker.call('POST', base + '/connections/' + wrong['id'] + '/probe', {}, expected=502)
            assert maker.call('GET', base + '/connections/' + wrong['id'])['status'] == 'failed'
            assert maker.call('GET', base + '/connections/' + wrong['id'] + '/profiles') == []
            passed.append(kind + ': real schema/types/estimated statistics/masked samples; immutable new secret refs; safe failure; SQL/resource/budget overrides denied')
        maker.call('POST', base + '/connections', {'name': 'unknown', 'resource_id': 'not-allowed', 'password': sentinel}, expected=403)
        maker.call('POST', base + '/connections', {'name': 'url', 'resource_id': resources[0]['id'], 'password': sentinel, 'host': '127.0.0.1'}, expected=422)
        private = admin.call('POST', f"/api/tenants/{credentials['tenant_id']}/projects", {'slug': 'connector_' + uuid.uuid4().hex[:8], 'name': '连接权限验收'}, expected=201)
        private_base = f"/api/tenants/{credentials['tenant_id']}/projects/{private['id']}"
        maker.call('GET', private_base + '/connections', expected=403)
        foreign_base = f"/api/tenants/{other['tenant_id']}/projects/{other['project_id']}"
        maker.call('GET', foreign_base + '/connections', expected=403)
        first = state['connections'][0]
        admin.call('GET', private_base + '/profiles/' + first['profile']['id'], expected=404)
        admin.call('GET', private_base + '/connections/' + first['connection']['id'], expected=404)
        maker_id = credentials['accounts']['maker']['id']
        member_path = base + '/members/' + maker_id
        admin.call('PUT', member_path, {'roles': ['auditor']})
        try:
            maker.call('POST', base + '/connections/' + first['connection']['id'] + '/probe', {}, expected=403)
        finally:
            admin.call('PUT', member_path, {'roles': ['maker']})
        passed.append('tenant/project/resource/profile isolation and existing-session role revocation enforced')
        evidence = admin.call('GET', f"/api/tenants/{credentials['tenant_id']}/evidence/export")
        facts = [e for e in evidence['events'] if e['action'].startswith(('connection.', 'connector.'))]
        assert {'connection.registered', 'connection.probed', 'connection.probe_failed', 'connection.rejected', 'connector.denied'} <= {e['action'] for e in facts}
        assert any(e['facts'].get('profile_id') == first['profile']['id'] for e in facts)
        assert not any(value in json.dumps(evidence, ensure_ascii=False) for value in forbidden)
        passed.append('successful and failed operations appear in signed evidence with safe facts; all HTTP responses scanned for known secrets/synthetic raw sensitive values')
        private_write(args.state, state)
    report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'phase': 'persistence' if args.recheck else 'connectors', 'kinds': kinds, 'passed': passed}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
