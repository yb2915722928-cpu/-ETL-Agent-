"""V05 public HTTP acceptance using persisted real MySQL/Doris profiles.

Run after verify_connectors_http.py, or pass --probe to create real profiles.
Reads private credentials only in this process; never prints passwords or rows.
"""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import urllib.request
import uuid

from verify_identity import Client, private_write


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8000')
    parser.add_argument('--credentials', type=Path, default=Path('.runtime/bootstrap-demo.json'))
    parser.add_argument('--other-credentials', type=Path, default=Path('.runtime/bootstrap-isolation.json'))
    parser.add_argument('--secrets', type=Path, default=Path('.runtime/connectors/secrets.json'))
    parser.add_argument('--connector-state', type=Path, default=Path('.runtime/connector-verification.json'))
    parser.add_argument('--state', type=Path, default=Path('.runtime/studio-verification.json'))
    parser.add_argument('--output', type=Path)
    parser.add_argument('--probe', action='store_true')
    parser.add_argument('--recheck', action='store_true')
    args = parser.parse_args()
    credentials = json.loads(args.credentials.read_text(encoding='utf-8'))
    other = json.loads(args.other_credentials.read_text(encoding='utf-8'))
    config = json.loads(args.secrets.read_text(encoding='utf-8'))
    forbidden = [a['password'] for c in (credentials, other) for a in c['accounts'].values()]
    forbidden += [value for entry in config.values() for key, value in entry.items() if 'password' in key]
    maker, auditor, admin = [Client(args.url, forbidden) for _ in range(3)]
    for client, role in ((maker, 'maker'), (auditor, 'auditor'), (admin, 'admin')):
        client.login(credentials, role)
    base = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
    path = base + '/studio/candidates'
    passed = []
    if args.recheck:
        state = json.loads(args.state.read_text(encoding='utf-8'))
        saved = maker.call('GET', path + '/' + state['candidate']['id'])
        assert saved == state['candidate']
        live = maker.call('POST', base + '/studio/validate', {'plan': saved['plan']})
        assert live['passed'] and live['execution_facts'] == saved['assessment']['execution_facts']
        passed.append('candidate, revision, plan and review artifacts survive restart; current gates still pass')
    else:
        empty = maker.call('POST', base + '/studio/validate', {'plan': {}})
        assert not empty['passed'] and any(g['gate'] == 'syntax' and not g['passed'] for g in empty['gates'])
        entries = [] if args.probe else json.loads(args.connector_state.read_text(encoding='utf-8'))['connections']
        if args.probe:
            for kind in ('mysql', 'doris'):
                connection = maker.call('POST', base + '/connections', {
                    'name': 'Studio 真实验收 ' + kind, 'resource_id': 'acceptance-' + kind,
                    'password': config[kind]['password']}, expected=201)
                profile = maker.call('POST', base + '/connections/' + connection['id'] + '/probe', {}, expected=201)
                entries.append({'connection': connection, 'profile': profile})
        source, target = [next(e for e in entries if e['connection']['kind'] == kind) for kind in ('mysql', 'doris')]
        # Only use actual public profiles, never fabricated schema or row data.
        source_table = next(t for t in source['profile']['content']['tables'] if t['name'] == 'customers')
        target_table = next(t for t in target['profile']['content']['tables'] if t['name'] == 'customers')
        target_names = {c['name'] for c in target_table['columns']}
        mappings = [{'source': c['name'], 'target': c['name'], 'operation': 'direct'}
                    for c in source_table['columns'] if c['name'] in target_names]
        plan = {'version': 'etl-plan-v1', 'mode': 'bounded_full',
                'source': {'connection_id': source['connection']['id'], 'profile_id': source['profile']['id'], 'table': 'customers'},
                'target': {'connection_id': target['connection']['id'], 'profile_id': target['profile']['id'], 'table': 'customers'},
                'mappings': mappings, 'quality': [{'kind': 'not_null', 'field': mappings[0]['target']}],
                'budget': {'max_rows': 100000, 'max_bytes': 268435456, 'max_seconds': 600,
                           'max_reject_ratio': 0.2, 'max_amplification': 2},
                'tools': ['mysql.read', 'doris.write']}
        review = maker.call('POST', base + '/studio/validate', {'plan': plan})
        assert review['passed'], review['gates']
        assert review['hocon'] and review['dag']['edges']
        assert review['execution_facts']['versions']['capability'] == 'mysql-doris-projection-v1'
        assert source['connection']['secret_reference_id'] in review['hocon']
        assert 'samples' not in json.dumps(review['execution_facts']) and 'approx_rows' not in json.dumps(review['execution_facts'])
        candidate = maker.call('POST', path, {'name': '真实 MySQL→Doris 手工候选', 'plan': plan}, expected=201)
        assert candidate['revision'] == 1 and candidate['assessment'] == review
        assert auditor.call('GET', path + '/' + candidate['id']) == candidate
        assert any(c['id'] == candidate['id'] for c in maker.call('GET', path))
        passed.append('real profiled MySQL→Doris projection passes deterministic gates; candidate persisted with same-source HOCON/DAG and safe credential references')
        # Each failed gate is observed through the public validator and stored candidate API.
        for change, gate in (({'mode': 'cdc'}, 'syntax'), ({'tools': ['shell.exec']}, 'tools'),
                             ({'budget': {**plan['budget'], 'max_rows': 100001}}, 'budget'),
                             ({'mappings': [{**mappings[0], 'source': 'missing_field'}]}, 'mapping'),
                             ({'mappings': [{**mappings[0], 'operation': 'sql', 'sql': 'DROP TABLE customers'}]}, 'syntax'),
                             ({'source': {**plan['source'], 'profile_id': target['profile']['id']}}, 'resources'),
                             ({'mappings': [{**m, 'source': 'email'} if m['target'] == 'balance' else m for m in mappings]}, 'schema'),
                             ({'quality': [{'kind': 'numeric_range', 'field': 'email', 'minimum': 0}]}, 'quality')):
            invalid = {**deepcopy(plan), **change}
            result = maker.call('POST', base + '/studio/validate', {'plan': invalid})
            assert result['passed'] is False and any(g['gate'] == gate and not g['passed'] for g in result['gates'])
            assert result['hocon'] is None and result['execution_facts'] is None
        invalid = {**deepcopy(plan), 'tools': ['shell.exec']}
        failed = maker.call('POST', path, {'name': '失败候选可修复', 'plan': invalid}, expected=201)
        assert not failed['assessment']['passed']
        fixed = maker.call('PUT', path + '/' + failed['id'], {'name': failed['name'], 'plan': plan, 'expected_revision': 1})
        assert fixed['revision'] == 2 and fixed['assessment']['passed']
        maker.call('PUT', path + '/' + failed['id'], {'name': failed['name'], 'plan': plan, 'expected_revision': 1}, expected=409)
        passed.append('unsupported syntax/SQL, tool, budget, mapping and mismatched profile are explained and blocked; failed candidate can be repaired with revision conflict checks')
        auditor.call('POST', path, {'name': 'denied', 'plan': plan}, expected=403)
        auditor.call('POST', base + '/studio/validate', {'plan': plan}, expected=403)
        private = admin.call('POST', f"/api/tenants/{credentials['tenant_id']}/projects", {
            'slug': 'studio_' + uuid.uuid4().hex[:8], 'name': 'Studio 隔离验收'}, expected=201)
        private_base = f"/api/tenants/{credentials['tenant_id']}/projects/{private['id']}"
        admin.call('PUT', private_base + '/members/' + credentials['accounts']['maker']['id'], {'roles': ['maker']})
        maker.call('GET', private_base + '/studio/candidates/' + candidate['id'], expected=404)
        maker.call('POST', private_base + '/studio/validate', {'plan': plan}, expected=404)
        foreign = f"/api/tenants/{other['tenant_id']}/projects/{other['project_id']}/studio/candidates"
        maker.call('GET', foreign, expected=403)
        member_path = base + '/members/' + credentials['accounts']['maker']['id']
        original_roles = next(m['roles'] for m in admin.call('GET', base + '/members')
                              if m['id'] == credentials['accounts']['maker']['id'])
        admin.call('PUT', member_path, {'roles': ['auditor']})
        try:
            maker.call('POST', base + '/studio/validate', {'plan': plan}, expected=403)
        finally:
            admin.call('PUT', member_path, {'roles': original_roles})
        for artifact in ('plan.json', 'dag.json', 'review.conf'):
            request = urllib.request.Request(args.url.rstrip('/') + path + '/' + candidate['id'] + '/artifacts/' + artifact)
            with maker.opener.open(request, timeout=15) as response:
                raw = response.read().decode()
                assert not any(value and value in raw for value in forbidden)
                assert 'attachment' in response.headers['Content-Disposition']
                assert response.headers['X-Content-SHA256'] == hashlib.sha256(raw.encode()).hexdigest()
        private_write(args.state, {'candidate': candidate})
        passed.append('read/write/download scope and Maker revocation enforced; artifacts downloadable with recomputable digests and no known secrets')
    report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'phase': 'studio-persistence' if args.recheck else 'studio', 'passed': passed,
              'limits': 'manual deterministic review only; no model, engine submission, quality execution, freeze or publication'}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
