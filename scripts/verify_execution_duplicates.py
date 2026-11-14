"""Observe every concurrent real duplicate task finish before checking Engine ledger."""
import json
from pathlib import Path
import subprocess
import time
import urllib.request
from verify_identity import Client

runtime = Path('.runtime/l1')
compose = ['docker', 'compose', '--env-file', str(runtime / 'deployment.env'), '-f', 'compose.l1.yaml']
state = json.loads((runtime / 'execution-state.json').read_text(encoding='utf-8'))
credentials = json.loads((runtime / 'bootstrap-demo.json').read_text(encoding='utf-8'))
client = Client('http://127.0.0.1:8010', [])
client.login(credentials, 'auditor')
base = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
row = client.call('GET', base + '/runs/' + state['runs']['lost_ack_success'])
command_id = row['outbox']['command_id']
code = f"import json; from app.celery_app import execute; print(json.dumps([execute.apply_async(args=[{command_id!r}]).id for _ in range(8)]))"
result = subprocess.run([*compose, 'exec', '-T', 'api', 'python', '-c', code], check=True, capture_output=True, text=True, encoding='utf-8')
ids = json.loads(result.stdout)
deadline = time.monotonic() + 90
while True:
    logs = subprocess.run([*compose, 'logs', '--no-color', 'worker'], check=True, capture_output=True, text=True, encoding='utf-8')
    text = logs.stdout + logs.stderr
    completed = [task_id for task_id in ids if 'etl.drive[' + task_id + '] succeeded' in text]
    if len(completed) == len(ids):
        break
    assert time.monotonic() < deadline, {'completed': len(completed), 'expected': len(ids)}
    time.sleep(.5)
with urllib.request.urlopen('http://127.0.0.1:8011/ledger') as response:
    jobs = [j for j in json.load(response)['jobs'] if j['request']['command_id'] == command_id]
assert len(jobs) == 1 and jobs[0]['id'] == row['engine_job_id']
report = {'command_id': command_id, 'run_id': row['id'], 'completed_duplicate_task_ids': completed,
          'engine_job_count_after_all_completed': len(jobs), 'original_engine_job_id': jobs[0]['id'],
          'passed': ['all eight actual concurrent Celery duplicate deliveries observed succeeded before checking the one original durable Engine job']}
Path('artifacts/issue-10/consumed-duplicates.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps(report, ensure_ascii=False, indent=2))
