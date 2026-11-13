"""V07 public HTTP acceptance, with real PostgreSQL row-lock concurrency.

Uses existing immutable versions. No engine or source mutations. Run against an
isolated API for --fixture cases; never adds production fault endpoints.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path

from verify_identity import Client, private_write


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8000')
    parser.add_argument('--runtime', type=Path, default=Path('.runtime'))
    parser.add_argument('--output', type=Path)
    parser.add_argument('--recheck', action='store_true')
    args = parser.parse_args()
    credentials = json.loads((args.runtime / 'bootstrap-demo.json').read_text(encoding='utf-8'))
    other = json.loads((args.runtime / 'bootstrap-isolation.json').read_text(encoding='utf-8'))
    clients = {}
    for role in ('maker', 'data_checker', 'security_checker', 'auditor', 'admin'):
        clients[role] = Client(args.url, [])
        clients[role].login(credentials, role)
    maker, data, security, auditor, admin = [clients[r] for r in ('maker', 'data_checker', 'security_checker', 'auditor', 'admin')]
    base = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
    path = base + '/preparations'
    state_file = args.runtime / 'preparations-verification.json'
    passed = []
    if args.recheck:
        saved = json.loads(state_file.read_text(encoding='utf-8'))
        row = auditor.call('GET', path + '/' + saved['id'])
        assert row['frozen'] == saved['frozen'] and row['decisions'] == saved['decisions']
        assert row['expires_at'] == saved['expires_at'] and row['validation']['state'] == 'approved'
        passed.append('frozen facts, named decisions and fixed expiry survive API restart')
    else:
        version = maker.call('GET', base + '/versions')[0]
        prepare = lambda tool='pipeline.publish': maker.call('POST', path, {'version_id': version['id'], 'tool': tool}, expected=201)
        row = prepare()
        assert row['risk_level'] == 'P1' and row['validation']['state'] == 'pending'
        assert (datetime.fromisoformat(row['expires_at']) - datetime.fromisoformat(row['created_at'])).total_seconds() == 86400
        decide = lambda client, slot, decision='approve', target=row: client.call('POST', path + '/' + target['id'] + '/decisions', {'slot': slot, 'decision': decision})
        maker.call('POST', path + '/' + row['id'] + '/decisions', {'slot': 'data', 'decision': 'approve'}, expected=403)
        auditor.call('POST', path, {'version_id': version['id'], 'tool': 'pipeline.publish'}, expected=403)
        data.call('POST', path + '/' + row['id'] + '/decisions', {'slot': 'security', 'decision': 'approve'}, expected=403)
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda pair: decide(*pair), [(data, 'data'), (security, 'security')]))
        approved = auditor.call('GET', path + '/' + row['id'])
        assert approved['validation']['state'] == 'approved'
        assert len(approved['decisions']) == 2 and len({d['checker_id'] for d in approved['decisions']}) == 2
        assert approved['expires_at'] == row['expires_at'] and approved['frozen_sha256'] == row['frozen_sha256']
        passed.append('P1 independently approved in concurrent PostgreSQL transactions; immutable frozen facts and exactly 24h expiry; unauthorized/self/incorrect-slot approval denied')
        member = base + '/members/' + credentials['accounts']['data_checker']['id']
        try:
            admin.call('PUT', member, {'roles': ['data_checker', 'security_checker']})
            dual = prepare('pipeline.trial')
            decide(data, 'data', target=dual)
            data.call('POST', path + '/' + dual['id'] + '/decisions', {'slot': 'security', 'decision': 'approve'}, expected=403)
            assert dual['risk_level'] == 'P2'
            admin.call('PUT', member, {'roles': ['auditor']})
            revoked = auditor.call('GET', path + '/' + row['id'])
            assert revoked['validation']['state'] == 'invalidated' and not revoked['validation']['valid']
            assert not revoked['validation']['current_eligibility']['data']
        finally:
            admin.call('PUT', member, {'roles': ['data_checker']})
        rejected = prepare()
        decide(data, 'data', 'reject', rejected)
        assert auditor.call('GET', path + '/' + rejected['id'])['validation']['state'] == 'rejected'
        security.call('POST', path + '/' + rejected['id'] + '/decisions', {'slot': 'security', 'decision': 'approve'}, expected=409)
        assert prepare('metadata.read')['validation']['state'] == 'auto_authorized'
        assert prepare('unregistered.tool')['validation']['state'] == 'denied'
        passed.append('P2 requires dual approval; same account cannot fill both slots; current Checker revocation invalidates; rejection is terminal; bounded P3 auto-authorizes and unknown tool is P0 denied')
        outsider = Client(args.url, [])
        outsider.login(other, 'admin')
        outsider.call('GET', path + '/' + row['id'], expected=403)
        outsider.call('POST', path + '/' + row['id'] + '/decisions', {'slot': 'data', 'decision': 'approve'}, expected=403)
        for method in ('PUT', 'PATCH', 'DELETE'):
            maker.call(method, path + '/' + row['id'], {}, expected=409)
        events = auditor.call('GET', base + '/evidence')['events']
        assert any(e['action'] == 'preparation.created' and e['preparation_id'] == row['id'] for e in events)
        assert sum(e['action'] == 'approval.decided' and e['preparation_id'] == row['id'] for e in events) == 2
        private_write(state_file, approved)
        passed.append('tenant isolation, immutable Preparation and approval/denial evidence exposed through public API')
    report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'phase': 'preparations', 'passed': passed,
              'limits': 'V07 authorization facts only; no execution or capability issuance'}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
