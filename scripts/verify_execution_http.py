"""Public L1 tracer: accepted command eventually has a durable correlated engine job."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from uuid import uuid4
from verify_identity import Client


def accept(url, runtime, tool='pipeline.publish', version_id=None, credentials=None):
    credentials = credentials or json.loads((runtime / 'bootstrap-demo.json').read_text(encoding='utf-8'))
    clients = {}
    for role in ('maker', 'data_checker', 'security_checker', 'operator', 'auditor'):
        clients[role] = Client(url, [])
        clients[role].login(credentials, role)
    scope = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
    version_id = version_id or clients['maker'].call('GET', scope + '/versions')[0]['id']
    row = clients['maker'].call('POST', scope + '/preparations', {'version_id': version_id, 'tool': tool}, 201)
    path = scope + '/preparations/' + row['id']
    if tool != 'metadata.read':
        for role, slot in (('data_checker', 'data'), ('security_checker', 'security')):
            clients[role].call('POST', path + '/decisions', {'slot': slot, 'decision': 'approve'})
    capability = clients['operator'].call('POST', path + '/capabilities', {}, 201)['capability']
    accepted = clients['operator'].call('POST', path + '/commit', {'capability': capability, 'idempotency_key': str(uuid4())}, 201)
    assert accepted['status'] == 'queued' and accepted['engine_job_id'] is None
    return clients['auditor'], scope, accepted


def wait_run(client, scope, run_id, predicate, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        row = client.call('GET', scope + '/runs/' + run_id)
        if predicate(row):
            return row
        time.sleep(.5)
    raise AssertionError({'timeout_run': row})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8010')
    parser.add_argument('--runtime', type=Path, default=Path('.runtime/l1'))
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    client, scope, accepted = accept(args.url, args.runtime)
    assert 'execution' in accepted, 'Run must expose durable L1 attempt and target ownership facts'
    row = wait_run(client, scope, accepted['id'], lambda r: r['status'] == 'submitted')
    assert row['engine_job_id'] and row['execution']['attempt']['correlation_id']
    assert row['execution']['ownership']['retained'] is True
    events = client.call('GET', scope + '/evidence')['events']
    assert any(e['action'] == 'execution.submission_prepared' and e['run_id'] == row['id'] for e in events)
    report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'run_id': row['id'],
              'passed': ['public Prepare/dualApprove/Commit accepts queued; real Outbox/Celery records one durable correlated L1 job and retained target ownership']}
    if args.output:
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
