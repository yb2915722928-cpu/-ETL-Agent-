"""Real isolated L1 faults through public authorization and Linux Celery/Redis.

Requires fresh scripts/seed_l1.py state, API/Engine running and publisher/worker
stopped. Only this L1's signed-command test fixtures are altered for bad-envelope
cases. No production fault endpoints; no ordinary stack mutation.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time
import urllib.request
from verify_execution_http import accept, wait_run
from verify_identity import Client, private_write

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url', default='http://127.0.0.1:8010')
parser.add_argument('--runtime', type=Path, default=Path('.runtime/l1'))
parser.add_argument('--output', type=Path, default=Path('artifacts/issue-10/real-l1.json'))
args = parser.parse_args()
runtime = args.runtime.resolve()
compose = ['docker', 'compose', '--env-file', str(runtime / 'deployment.env'), '-f', 'compose.l1.yaml']
passed = []


def cli(*command, check=True):
    result = subprocess.run([*compose, *command], text=True, encoding='utf-8', capture_output=True)
    if check and result.returncode:
        raise RuntimeError(result.stderr[-3000:] + result.stdout[-1000:])
    return result


def python(service, code):
    return cli('exec', '-T', service, 'python', '-c', code).stdout.strip()


def publish():
    cli('run', '--rm', '--no-deps', 'publisher', 'python', '-c', 'from app.outbox_publisher import publish_once; publish_once()')


def duplicates(command_id, count=3):
    return json.loads(python('api', f"import json; from app.celery_app import execute; print(json.dumps([execute.apply_async(args=[{command_id!r}]).id for _ in range({count})]))"))


def worker(name='l1-acceptance-worker', barrier=None):
    command = ['run', '-d', '--no-deps', '--name', name]
    if barrier:
        command += ['-e', 'PYTHONPATH=/app', '-e', 'FAULT_COMMAND_ID=' + barrier, '-v', str(Path('scripts').resolve()) + ':/fixtures:ro',
                    '-v', str(runtime / 'faults') + ':/faults', 'worker', 'python', '/fixtures/l1_process_fixture.py', 'worker-before-ack']
    else:
        command += ['worker']
    cli(*command)
    subprocess.run(['docker', 'update', '--restart=no', name], check=True, capture_output=True)
    return name


def kill(name):
    subprocess.run(['docker', 'kill', '--signal=KILL', name], check=True, capture_output=True)


def ledger():
    with urllib.request.urlopen('http://127.0.0.1:8011/ledger', timeout=5) as response:
        return json.load(response)['jobs']


def until(check, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(.5)
    raise AssertionError('timed out waiting for real fault observation')


credentials = json.loads((runtime / 'bootstrap-demo.json').read_text(encoding='utf-8'))
alias_credentials = json.loads((runtime / 'alias-credentials.json').read_text(encoding='utf-8'))
versions = json.loads((runtime / 'versions.json').read_text(encoding='utf-8'))
alias_versions = json.loads((runtime / 'alias-versions.json').read_text(encoding='utf-8'))
# A real >5 minute queue, no fabricated command timestamps or capability re-use.
client, scope, aged = accept(args.url, runtime, 'metadata.read', versions['customers'])
aged_start = time.monotonic()
assert aged['execution']['attempt'] is None
print('accepted actual aged command; running setup/fault checks during queue wait', flush=True)

# Failure before Redis publication: actual unavailable broker process exits; PG
# outbox remains pending and later recovers through the normal publisher.
cli('stop', 'redis')
failed = cli('run', '--rm', '--no-deps', '-e', 'REDIS_URL=redis://redis:6379/0', 'publisher',
    'python', '-c', 'from app.outbox_publisher import publish_once; publish_once()', check=False)
assert failed.returncode != 0
cli('start', 'redis')
until(lambda: cli('exec', '-T', 'redis', 'redis-cli', 'ping', check=False).stdout.strip() == 'PONG')
row = client.call('GET', scope + '/runs/' + aged['id'])
assert row['outbox']['status'] == 'pending' and row['outbox']['attempts'] == 1
# After publish, before PG sent: intentionally KILL the publisher process wrapper.
crashed = cli('run', '--rm', '--no-deps', '-e', 'PYTHONPATH=/app', '-e', 'FAULT_COMMAND_ID=' + aged['outbox']['command_id'],
    '-v', str(Path('scripts').resolve()) + ':/fixtures:ro', 'publisher',
    'python', '/fixtures/l1_process_fixture.py', 'publisher-after-publish', check=False)
assert crashed.returncode == 74, (crashed.returncode, crashed.stderr)
row = client.call('GET', scope + '/runs/' + aged['id'])
assert row['outbox']['status'] == 'pending' and row['outbox']['attempts'] == 2
publish()
assert client.call('GET', scope + '/runs/' + aged['id'])['outbox']['status'] == 'sent'
passed.append('real Redis-down publisher failure and Redis-published/PG-unsent process death recover from pending Outbox')

# Validly signed scope/env/manifest mismatches and invalid signature all originate
# from ordinary public Commit; fixture-only corruption is isolated to this DB.
denials = []
for mode in ('signature', 'environment', 'scope', 'manifest'):
    c, s, row = accept(args.url, runtime, 'metadata.read', versions['customers'])
    command_id = row['outbox']['command_id']
    code = f"""from copy import deepcopy
from app.db import Session
from app.execution import ManagedCommand
from app.evidence_signing import EvidenceSigner
from app.command_signing import sign_envelope
with Session.begin() as db:
 row=db.get(ManagedCommand,{command_id!r}); envelope=deepcopy(row.envelope)
 payload=envelope['payload']
 mode={mode!r}
 if mode=='environment': payload['environment']='production'
 if mode=='scope': payload['project_id']='other-project'
 if mode=='manifest': payload['artifact_hashes']['manifest.json']='0'*64
 row.envelope=sign_envelope(EvidenceSigner(),payload) if mode!='signature' else {{**envelope,'signature':'AAAA'}}
"""
    python('api', code)
    denials.append((c, s, row, mode))
# A current role removal in another project must not prevent the aged valid one.
c, s, revoked = accept(args.url, runtime, 'pipeline.publish', alias_versions['customers'], alias_credentials)
admin = Client(args.url, [])
admin.login(credentials, 'admin')
admin.call('PUT', s + '/members/' + credentials['accounts']['operator']['id'], {'roles': ['auditor']})
denials.append((c, s, revoked, 'current operator revoked'))
publish()

# Keep this a real queue-age test. The script runs as a managed background job;
# host tooling remains available while the queue has no consumer.
remaining = max(0, 306 - (time.monotonic() - aged_start))
print('setup complete; waiting real queue age, remaining seconds=' + str(round(remaining)), flush=True)
while time.monotonic() - aged_start < 306:
    time.sleep(1)
name = worker()
aged_row = wait_run(client, scope, aged['id'], lambda r: r['status'] == 'metadata_checked', 90)
aged_queue_seconds = (datetime.now(timezone.utc) - datetime.fromisoformat(aged['accepted_at'])).total_seconds()
assert aged_queue_seconds > 300
assert aged_row['execution']['attempt'] is None and not ledger()
for c, s, row, mode in denials:
    denied = wait_run(c, s, row['id'], lambda r: r['status'] == 'denied', 90)
    assert denied['execution']['attempt'] is None
    assert any(event['action'] == 'execution.denied' for event in denied['execution']['control_facts'])
passed.append('actual >5 minute accepted queue executes metadata.read without TTL/reconsume; current revocation and signature/env/scope/manifest mismatches deny before submit')
kill(name)
# Restore project role via the same public membership mutation for alias test.
admin.call('PUT', s + '/members/' + credentials['accounts']['operator']['id'], {'roles': ['operator']})

# Worker death after the external durable job exists and before response. Hide
# discovery at Engine startup, then prove repeated delivery cannot submit again.
(runtime / 'fixture' / 'fixture.json').write_text(json.dumps({'orders': 'unknown'}), encoding='utf-8')
cli('restart', 'engine')
c, s, unknown = accept(args.url, runtime, 'pipeline.publish', versions['orders'])
publish()
name = worker('l1-submit-loss-worker')
jobs = until(lambda: [j for j in ledger() if j['request']['command_id'] == unknown['outbox']['command_id']])
assert len(jobs) == 1
original = jobs[0]
kill(name)
name = worker('l1-recovery-worker')
unknown_row = wait_run(c, s, unknown['id'], lambda r: r['status'] == 'submission_unknown', 120)
assert unknown_row['engine_job_id'] is None and unknown_row['execution']['ownership']['retained']
duplicates(unknown['outbox']['command_id'], 5)
c_alias, s_alias, blocked = accept(args.url, runtime, 'pipeline.publish', alias_versions['orders'], alias_credentials)
publish()
blocked_row = wait_run(c_alias, s_alias, blocked['id'], lambda r: r['status'] == 'blocked_target')
assert blocked_row['execution']['attempt'] is None
assert len(ledger()) == 1
passed.append('KILL after durable external job creation retains unknown Attempt/ownership; cross-project resource alias for same physical target excluded; duplicate redelivery never resubmits')

# Independent target with P2 effective clamp and actual lost ACK. Barrier runs
# in prefork task_postrun before parent sees success and sends the Redis ACK.
kill(name)
c, s, success = accept(args.url, runtime, 'pipeline.trial', versions['customers'])
name = worker('l1-lost-ack-worker', success['outbox']['command_id'])
publish()
until(lambda: (runtime / 'faults' / 'before-ack').exists())
ack_task = (runtime / 'faults' / 'before-ack').read_text(encoding='utf-8')
row = wait_run(c, s, success['id'], lambda r: r['status'] == 'submitted')
assert row['execution']['attempt']['request']['budget']['max_rows'] == 10000
redis_before = json.loads(python('api', "import json,os,redis; r=redis.Redis.from_url(os.environ['REDIS_URL'],decode_responses=True); print(json.dumps(r.hgetall('unacked')))"))
assert any(ack_task in value for value in redis_before.values()), 'must observe actual unACKed Redis task before KILL'
kill(name)
name = worker('l1-after-lost-ack-worker')
# No manual enqueue of the lost task: wait for genuine Redis visibility restore.
def redelivered():
    logs = subprocess.run(['docker', 'logs', name], text=True, encoding='utf-8', capture_output=True)
    return ('etl.drive[' + ack_task + '] received') in logs.stdout + logs.stderr
until(redelivered, 150)
duplicate_ids = duplicates(success['outbox']['command_id'], 8)
def duplicates_completed():
    logs = subprocess.run(['docker', 'logs', name], text=True, encoding='utf-8', capture_output=True)
    text = logs.stdout + logs.stderr
    return all('etl.drive[' + task_id + '] succeeded' in text for task_id in duplicate_ids)
until(duplicates_completed, 150)
assert len([j for j in ledger() if j['request']['command_id'] == success['outbox']['command_id']]) == 1
assert len(ledger()) == 2
passed.append('independent physical target progresses with effective P2 clamp; real Redis unACKed task observed then whole worker KILL and visibility redelivery of same task id; concurrent duplicates create no extra job')

# Restart separate Engine with discovery visible: only the original job is linked.
(runtime / 'fixture' / 'fixture.json').write_text('{}\n', encoding='utf-8')
cli('restart', 'engine')
duplicates(unknown['outbox']['command_id'], 2)
recovered = wait_run(client, scope, unknown['id'], lambda r: r['status'] == 'submitted', 90)
assert recovered['engine_job_id'] == original['id']
assert recovered['execution']['attempt']['correlation_id'] == original['correlation_id']
assert len(ledger()) == 2
passed.append('separate Engine restart retains SQLite ledger; later discovery associates exact original job/correlation with retained ownership, no resubmit/publication/cleanup')
kill(name)
cli('up', '-d', 'worker', 'publisher', 'web')
report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'level': 'L1, not L2', 'passed': passed,
          'queue_age_seconds': round(aged_queue_seconds), 'acceptance_duration_seconds': round(time.monotonic() - aged_start),
          'lost_ack_task_id': ack_task, 'lost_ack_unacked_observed': True,
          'durable_jobs': [{'id': j['id'], 'correlation_id': j['correlation_id'], 'command_id': j['request']['command_id']} for j in ledger()],
          'runs': {'aged': aged['id'], 'unknown_recovered': unknown['id'], 'physical_alias_blocked': blocked['id'], 'lost_ack_success': success['id']}}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
private_write(runtime / 'execution-state.json', report)
print(json.dumps(report, ensure_ascii=False, indent=2))
