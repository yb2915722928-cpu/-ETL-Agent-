"""Pure public recheck of retained L1 control facts after service recreation."""
from datetime import datetime
import json
from pathlib import Path
import urllib.request
from verify_identity import Client

runtime = Path('.runtime/l1')
state = json.loads((runtime / 'execution-state.json').read_text(encoding='utf-8'))
credentials = json.loads((runtime / 'bootstrap-demo.json').read_text(encoding='utf-8'))
alias = json.loads((runtime / 'alias-credentials.json').read_text(encoding='utf-8'))
client = Client('http://127.0.0.1:8010', [])
client.login(credentials, 'auditor')
base = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
rows = {kind: client.call('GET', base + '/runs/' + state['runs'][kind])
        for kind in ('aged', 'unknown_recovered', 'lost_ack_success')}
assert rows['aged']['status'] == 'metadata_checked' and rows['aged']['execution']['attempt'] is None
checked = next(e for e in rows['aged']['execution']['control_facts'] if e['action'] == 'execution.metadata_checked')
queue_age = (datetime.fromisoformat(checked['occurred_at']) - datetime.fromisoformat(rows['aged']['accepted_at'])).total_seconds()
assert queue_age > 300
for kind in ('unknown_recovered', 'lost_ack_success'):
    row = rows[kind]
    original = next(j for j in state['durable_jobs'] if j['command_id'] == row['outbox']['command_id'])
    assert row['status'] == 'submitted' and row['engine_job_id'] == original['id']
    assert row['execution']['attempt']['correlation_id'] == original['correlation_id']
    assert row['execution']['ownership']['retained'] is True
alias_scope = f"/api/tenants/{alias['tenant_id']}/projects/{alias['project_id']}"
blocked = client.call('GET', alias_scope + '/runs/' + state['runs']['physical_alias_blocked'])
assert blocked['status'] == 'blocked_target' and blocked['execution']['attempt'] is None
with urllib.request.urlopen('http://127.0.0.1:8011/ledger') as response:
    jobs = json.load(response)['jobs']
assert {j['id'] for j in jobs} == {j['id'] for j in state['durable_jobs']}
report = {'passed': ['recreated API/worker/Engine preserve both original jobs, correlations, ownership, blocked alias and metadata-only control facts; public GET remains pure'],
          'aged_metadata_queue_seconds_from_durable_events': round(queue_age, 3), 'engine_job_count': len(jobs)}
Path('artifacts/issue-10/restart-recheck.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps(report, ensure_ascii=False, indent=2))
