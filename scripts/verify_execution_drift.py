"""Real queued-command resource drift check against only the isolated L1 policy."""
import argparse
import json
from pathlib import Path
import subprocess
from verify_execution_http import accept, wait_run

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--runtime', type=Path, default=Path('.runtime/l1'))
parser.add_argument('--url', default='http://127.0.0.1:8010')
parser.add_argument('--output', type=Path, default=Path('artifacts/issue-10/resource-drift.json'))
args = parser.parse_args()
compose = ['docker', 'compose', '--env-file', str(args.runtime / 'deployment.env'), '-f', 'compose.l1.yaml']
def run(*cmd):
    subprocess.run([*compose, *cmd], check=True, capture_output=True)
run('stop', 'publisher', 'worker')
policy_file = args.runtime / 'policy' / 'resources.json'
original = policy_file.read_text(encoding='utf-8')
credentials = json.loads((args.runtime / 'alias-credentials.json').read_text(encoding='utf-8'))
versions = json.loads((args.runtime / 'alias-versions.json').read_text(encoding='utf-8'))
try:
    client, scope, accepted = accept(args.url, args.runtime, 'metadata.read', versions['customers'], credentials)
    policy = json.loads(original)
    policy['doris-alias']['physical_cluster_id'] = 'changed-deployment-identity'
    policy_file.write_text(json.dumps(policy), encoding='utf-8')
    run('run', '--rm', '--no-deps', 'publisher', 'python', '-c', 'from app.outbox_publisher import publish_once; publish_once()')
    run('up', '-d', 'worker')
    denied = wait_run(client, scope, accepted['id'], lambda r: r['status'] == 'denied')
    assert denied['execution']['attempt'] is None and denied['execution']['ownership'] is None
    assert any(e['action'] == 'execution.denied' and '资源' in str(e['facts']) for e in denied['execution']['control_facts'])
    report = {'run_id': denied['id'], 'passed': ['real queued accepted command rechecks current deployment resource fingerprint; policy drift denies before EngineAttempt/ownership/submit']}
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))
finally:
    policy_file.write_text(original, encoding='utf-8')
    run('up', '-d', 'worker', 'publisher')
