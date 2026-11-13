"""V08 public HTTP acceptance against real PostgreSQL; no engine is submitted."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import urllib.error
import urllib.request
from datetime import datetime, timezone
import json
from pathlib import Path
from uuid import uuid4

from verify_identity import Client, private_write


def commit_response(client, path, body):
    request = urllib.request.Request(client.url + path + '/commit', data=json.dumps(body).encode(),
                                     method='POST', headers={'Content-Type': 'application/json'})
    try:
        response = client.opener.open(request, timeout=30)
    except urllib.error.HTTPError as error:
        response = error
    return response.code, json.loads(response.read())


def run(url, runtime, recheck=False):
    credentials = json.loads((runtime / 'bootstrap-demo.json').read_text(encoding='utf-8'))
    clients = {}
    for role in ('maker', 'data_checker', 'security_checker', 'operator', 'auditor', 'admin'):
        clients[role] = Client(url, [])
        clients[role].login(credentials, role)
    maker, data, security, operator, auditor, admin = [clients[r] for r in clients]
    scope = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
    state_file = runtime / 'commits-verification.json'
    if recheck:
        saved = json.loads(state_file.read_text(encoding='utf-8'))
        assert auditor.call('GET', scope + '/runs/' + saved['id']) == saved
        return {'passed': ['same queued Run, original subject, signed command and pending Outbox survive API restart']}
    version = maker.call('GET', scope + '/versions')[0]
    row = maker.call('POST', scope + '/preparations', {'version_id': version['id'], 'tool': 'pipeline.publish'}, 201)
    path = scope + '/preparations/' + row['id']
    for client, slot in ((data, 'data'), (security, 'security')):
        client.call('POST', path + '/decisions', {'slot': slot, 'decision': 'approve'})
    capability = operator.call('POST', path + '/capabilities', {}, 201)
    payload = capability['capability']['payload']
    assert payload['expires_at'] - payload['issued_at'] == 300
    assert payload['subject'] == credentials['accounts']['operator']['id']
    assert payload['artifact_hashes'] == row['frozen']['artifact_hashes']
    body = {'capability': capability['capability'], 'idempotency_key': str(uuid4())}
    parallel_operator = Client(url, [])
    parallel_operator.login(credentials, 'operator')
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda client: commit_response(client, path, body), (operator, parallel_operator)))
    assert sorted(status for status, _ in responses) == [200, 201]
    accepted = responses[0][1]
    assert responses[1][1] == accepted
    changed_capability = operator.call('POST', path + '/commit', {**body, 'idempotency_key': str(uuid4())}, 409)
    assert changed_capability['detail']
    altered = json.loads(json.dumps(body))
    altered['capability']['payload']['environment'] = 'other'
    operator.call('POST', path + '/commit', altered, 409)
    assert accepted['status'] == 'queued' and accepted['engine_job_id'] is None
    assert accepted['managed_command']['payload']['subject'] == payload['subject']
    assert accepted['outbox']['status'] == 'pending'
    assert auditor.call('GET', scope + '/runs/' + accepted['id']) == accepted
    assert operator.call('POST', path + '/commit', body) == accepted
    rows = auditor.call('GET', scope + '/runs')
    assert sum(run['preparation_id'] == row['id'] for run in rows) == 1
    maker.call('POST', path + '/capabilities', {}, 403)
    auditor.call('POST', path + '/commit', body, 403)
    outsider = Client(url, [])
    outsider.login(json.loads((runtime / 'bootstrap-isolation.json').read_text(encoding='utf-8')), 'admin')
    outsider.call('GET', scope + '/runs/' + accepted['id'], expected=403)
    events = auditor.call('GET', scope + '/evidence')['events']
    assert sum(event['action'] == 'commit.accepted' and event['run_id'] == accepted['id'] for event in events) == 1
    # Independent valid Capability/key pair race for the same Preparation.
    trial = maker.call('POST', scope + '/preparations', {'version_id': version['id'], 'tool': 'metadata.read'}, 201)
    trial_path = scope + '/preparations/' + trial['id']
    race = [{'capability': operator.call('POST', trial_path + '/capabilities', {}, 201)['capability'],
             'idempotency_key': str(uuid4())} for _ in range(2)]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda pair: commit_response(pair[0], trial_path, pair[1]), zip((operator, parallel_operator), race)))
    assert sorted(status for status, _ in results) == [201, 409]
    readonly = next(result for status, result in results if status == 201)
    assert readonly['managed_command']['payload']['authorized_operation']['data_plane_mutation'] is False
    assert sum(run['preparation_id'] == trial['id'] for run in auditor.call('GET', scope + '/runs')) == 1
    private_write(state_file, accepted)
    return {'passed': ['300-second subject/artifact-bound capability; unique queued Run, signed command and pending Outbox publicly queryable',
        'real PostgreSQL concurrent identical requests return one201/one200 and same receipt; separate valid tokens/keys race one201/one409',
        'same-key changed request409, consumed-token new-key replay409, unauthorized roles403, tenant isolation403; one accepted evidence event',
        'P3 command remains metadata-only without data-plane mutation; GET/list expose exactly one Run per Preparation'], 'run_id': accepted['id']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8000')
    parser.add_argument('--runtime', type=Path, default=Path('.runtime'))
    parser.add_argument('--output', type=Path)
    parser.add_argument('--recheck', action='store_true')
    args = parser.parse_args()
    report = {'checked_at': datetime.now(timezone.utc).isoformat(), **run(args.url, args.runtime, args.recheck)}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
