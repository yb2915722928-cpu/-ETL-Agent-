"""Host driver: brief real L1 PostgreSQL outage with isolated worker/Engine fixtures.

Run from the root Compose workspace; --scripts can point at a review worktree.
No volume is removed, no retained engine job or control-plane row is modified.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time
import urllib.request
from uuid import uuid4


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scripts', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    runtime = Path('.runtime/l1').resolve()
    name = 'review-db-' + uuid4().hex
    control = runtime / name
    control.mkdir()
    compose = ['docker', 'compose', '--env-file', str(runtime / 'deployment.env'), '-f', 'compose.l1.yaml']
    def cli(*command):
        return subprocess.run([*compose, *command], check=True, capture_output=True, text=True)
    def ledger():
        with urllib.request.urlopen('http://127.0.0.1:8011/ledger', timeout=5) as response:
            return json.load(response)
    original_jobs = ledger()
    process = None
    stopped = False
    log_path = control / 'result.log'
    def wait_marker(marker):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if control.joinpath(marker).exists():
                return
            if process.poll() is not None:
                raise AssertionError(log_path.read_text(encoding='utf-8')[-6000:])
            time.sleep(.1)
        raise AssertionError('fixture marker timeout: ' + marker)
    try:
        with log_path.open('w', encoding='utf-8') as output:
            process = subprocess.Popen([*compose, 'run', '--rm', '--no-deps', '-T', '--name', name,
                '-e', 'PYTHONPATH=/app:/tests', '-v', str(args.scripts.resolve()) + ':/tests:ro', 'api',
                'python', '/tests/verify_execution_database_fixtures.py', '/run/identity/' + name],
                stdout=output, stderr=subprocess.STDOUT)
            wait_marker('outage-ready')
            cli('stop', 'postgres')
            stopped = True
            control.joinpath('postgres-stopped').touch()
            wait_marker('outage-observed')
            cli('start', 'postgres')
            stopped = False
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                status = subprocess.run([*compose, 'exec', '-T', 'postgres', 'pg_isready', '-U', 'etl', '-d', 'etl_l1'],
                    capture_output=True, text=True)
                if status.returncode == 0:
                    break
                time.sleep(.2)
            assert status.returncode == 0, 'PostgreSQL did not recover'
            control.joinpath('postgres-restored').touch()
            returncode = process.wait(timeout=90)
        assert control.joinpath('report.json').exists(), log_path.read_text(encoding='utf-8')[-6000:]
        report = json.loads(control.joinpath('report.json').read_text(encoding='utf-8'))
        assert ledger() == original_jobs, 'retained Engine ledger changed'
        report['retained_public_engine_jobs'] = len(original_jobs['jobs'])
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps(report, ensure_ascii=False, indent=2))
        assert returncode == 0, 'database retry regressions failed; see report'
    finally:
        if stopped:
            cli('start', 'postgres')
        if process and process.poll() is None:
            subprocess.run(['docker', 'stop', '-t', '5', name], capture_output=True, check=True)
            process.wait(timeout=15)
        # A real PG outage can terminate the ordinary standalone publisher. Its
        # existing restart policy normally recovers; start is idempotent here.
        cli('start', 'publisher')


if __name__ == '__main__':
    main()
