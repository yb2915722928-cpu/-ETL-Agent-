"""CSV public HTTP acceptance: real managed bytes, full parse, scope, persistence."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import urllib.error
import urllib.parse
import urllib.request
import uuid

from verify_identity import Client, private_write


def upload(client, base, data, expected=201, resource_id='acceptance-csv'):
    query = urllib.parse.urlencode({'name': 'CSV公开行为验收', 'resource_id': resource_id})
    request = urllib.request.Request(client.url + base + '/csv-uploads?' + query,
                                     data=data, method='POST', headers={'Content-Type': 'text/csv'})
    try:
        response = client.opener.open(request, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    raw = response.read().decode()
    assert not any(s and s in raw for s in client.secrets), 'Sensitive plaintext in upload response'
    assert response.code == expected, f'upload: expected {expected}, got {response.code}: {raw}'
    return json.loads(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8000')
    parser.add_argument('--credentials', type=Path, default=Path('.runtime/bootstrap-demo.json'))
    parser.add_argument('--other-credentials', type=Path, default=Path('.runtime/bootstrap-isolation.json'))
    parser.add_argument('--fixtures', type=Path, default=Path('.runtime/connectors/csv'))
    parser.add_argument('--state', type=Path, default=Path('.runtime/csv-verification.json'))
    parser.add_argument('--output', type=Path)
    parser.add_argument('--recheck', action='store_true')
    args = parser.parse_args()
    credentials = json.loads(args.credentials.read_text(encoding='utf-8'))
    other = json.loads(args.other_credentials.read_text(encoding='utf-8'))
    forbidden = ['alice@example.test', 'Alice Example', 'csv-sensitive-sentinel']
    forbidden += [v['password'] for source in (credentials, other) for v in source['accounts'].values()]
    maker, admin, auditor = [Client(args.url, forbidden) for _ in range(3)]
    maker.login(credentials, 'maker'); admin.login(credentials, 'admin'); auditor.login(credentials, 'auditor')
    base = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
    passed = []
    if args.recheck:
        state = json.loads(args.state.read_text(encoding='utf-8'))
        for saved in state['connections']:
            connection = maker.call('GET', base + '/connections/' + saved['connection']['id'])
            assert connection['storage_id'] == saved['connection']['storage_id']
            assert connection['secret_reference_id'] is None
            profile = maker.call('GET', base + '/profiles/' + saved['profile']['id'])
            assert profile == saved['profile']
            new = maker.call('POST', base + '/connections/' + connection['id'] + '/probe', {}, expected=201)
            assert new['schema_hash'] == profile['schema_hash']
        passed.append('same managed file and immutable profile survive restart; full-file re-probe succeeds')
    else:
        resources = maker.call('GET', base + '/connector-resources')
        assert any(r['id'] == 'acceptance-csv' and r['kind'] == 'csv' for r in resources)
        assert not any('root' in r or 'projects' in r for r in resources)
        state = {'connections': []}
        for filename, count in [('customers-utf8.csv', 3), ('quoted-multiline.csv', 2)]:
            connection = upload(maker, base, (args.fixtures / filename).read_bytes())
            assert connection['kind'] == 'csv' and connection['secret_reference_id'] is None
            path = base + '/connections/' + connection['id']
            profile = maker.call('POST', path + '/probe', {}, expected=201)
            table = profile['content']['tables'][0]
            assert table['approx_rows'] == count and table['columns'] and table['samples']
            assert all(v == '[已脱敏]' for row in table['samples'] for v in row.values())
            assert auditor.call('GET', base + '/profiles/' + profile['id']) == profile
            maker.call('GET', path + '/file', expected=404)
            state['connections'].append({'connection': connection, 'profile': profile})
        for filename in ['invalid-unclosed-quote.csv', 'invalid-row-width.csv', 'invalid-utf8.csv']:
            connection = upload(maker, base, (args.fixtures / filename).read_bytes())
            path = base + '/connections/' + connection['id']
            error = maker.call('POST', path + '/probe', {}, expected=422)
            assert error['code'] == 'SourceParseFailure' and error['source_ref'] == 'csv:' + connection['storage_id']
            assert error['line'] >= 1 and error['column'] >= 1
            assert maker.call('GET', path)['status'] == 'failed'
            assert maker.call('GET', path + '/profiles') == []
        passed.append('two valid files produce full schema/text types/complete counts/masked samples; three corrupt batches fail without profiles and return safe source/line/column')
        upload(maker, base, b'x' * (2 * 1024 * 1024 + 1), expected=413)
        upload(auditor, base, b'email\ncsv-sensitive-sentinel', expected=403)
        upload(maker, base, b'a\nx', resource_id='../../host-file', expected=403)
        maker.call('POST', base + '/connections', {'name': 'path injection', 'resource_id': 'acceptance-csv',
                   'password': 'csv-sensitive-sentinel', 'path': '/etc/passwd'}, expected=422)
        foreign = f"/api/tenants/{other['tenant_id']}/projects/{other['project_id']}"
        upload(maker, foreign, b'a\nx', expected=403)
        private = admin.call('POST', f"/api/tenants/{credentials['tenant_id']}/projects", {
            'slug': 'csv_' + uuid.uuid4().hex[:8], 'name': 'CSV隔离验收'}, expected=201)
        private_base = f"/api/tenants/{credentials['tenant_id']}/projects/{private['id']}"
        upload(admin, private_base, b'a\nx', expected=403)
        admin.call('PUT', private_base + '/members/' + credentials['accounts']['maker']['id'], {'roles': ['maker']})
        upload(maker, private_base, b'a\nx', expected=403)
        assert not any(r['kind'] == 'csv' for r in maker.call('GET', private_base + '/connector-resources'))
        first = state['connections'][0]
        maker.call('GET', private_base + '/connections/' + first['connection']['id'], expected=404)
        maker.call('GET', private_base + '/profiles/' + first['profile']['id'], expected=404)
        member = base + '/members/' + credentials['accounts']['maker']['id']
        admin.call('PUT', member, {'roles': ['auditor']})
        try:
            upload(maker, base, b'a\nx', expected=403)
        finally:
            admin.call('PUT', member, {'roles': ['maker']})
        evidence = admin.call('GET', f"/api/tenants/{credentials['tenant_id']}/evidence/export")
        assert not any(v in json.dumps(evidence, ensure_ascii=False) for v in forbidden)
        assert any(e['facts'].get('code') == 'SourceParseFailure' for e in evidence['events'])
        private_write(args.state, state)
        passed.append('bounded upload; maker authorization before body; tenant/project/root whitelist isolation; live role revocation; safe signed evidence; raw files have no read API')
    report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'phase': 'persistence' if args.recheck else 'csv-http', 'passed': passed}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
