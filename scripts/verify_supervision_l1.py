"""Public HTTP + real isolated PG/Redis/Celery + private durable L1 engine tracer.

Run with current backend mounted over the API image and root identity/policy read-only.
Only disposable schema/queue/temp engine are mutated. Reports contain no credentials.
"""
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.request
from unittest.mock import patch

from sqlalchemy import text
from app import execution_runtime, outbox_publisher
from app.celery_app import celery
from app.db import engine
from isolated_api_fixture import isolated_api
from verify_execution_http import accept, wait_run
from verify_identity import Client


def until(check):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except OSError:
            pass
        time.sleep(.1)
    raise AssertionError('isolated supervision fixture timeout')


def run(output):
    basic = dict(source_read=100, evaluated=100, valid=100, rejected=0, final_output=100, sink_bytes=1000, elapsed_seconds=1)
    cases = [
        ('reject_99_100', [{**basic, 'source_read': 99, 'evaluated': 99, 'valid': 0, 'rejected': 99}, {**basic, 'valid': 0, 'rejected': 100}], 'reject_ratio_exceeded'),
        ('final_small', [{**basic, 'source_read': 2, 'evaluated': 2, 'valid': 0, 'rejected': 2, 'final_output': 1, 'state': 'finished'}], 'reject_ratio_exceeded'),
        ('rows', [{**basic, 'source_read': 100001}], 'rows_exceeded'),
        ('bytes', [{**basic, 'evaluated': 1, 'valid': 1, 'sink_bytes': 268435457}], 'bytes_exceeded'),
        ('duration', [{**basic, 'elapsed_seconds': 601}], 'duration_exceeded'),
        ('amplification', [{**basic, 'final_output': 201}], 'amplification_exceeded'),
        ('missing', [None], 'metrics_missing'),
        ('stale', [{**basic, 'observed_at': '2020-01-01T00:00:00+00:00'}], 'metrics_stale'),
        ('reset', [basic, {**basic, 'sink_bytes': 0}], 'metrics_reset'),
        ('estimated', [{**basic, 'byte_kind': 'estimated'}], 'bytes_not_measured'),
        ('zero', [{'elapsed_seconds': 0}], None),
        ('finished', [{**basic, 'state': 'finished'}], None),
    ]
    passed = []
    with isolated_api('supervision', 18011) as (session, schema), tempfile.TemporaryDirectory() as directory:
        directory = Path(directory)
        with session.begin() as db:
            db.execute(text('DELETE FROM target_ownership'))
            db.execute(text("UPDATE execution_outbox SET status='sent'"))
        environment = {key: value for key, value in os.environ.items() if key != 'EVIDENCE_SIGNING_KEY'}
        environment['DATABASE_URL'] = engine.url.update_query_dict({'options': f'-csearch_path={schema}'}).render_as_string(False)
        environment.update(L1_ENGINE_URL='http://127.0.0.1:8011', L1_LEDGER=str(directory / 'jobs.sqlite3'))
        os.environ['L1_ENGINE_URL'] = environment['L1_ENGINE_URL']
        config = directory / 'engine.json'
        config.write_text(json.dumps({'__metric_scenarios': [case[1] for case in cases]}))
        environment['L1_FIXTURE_CONFIG'] = str(config)
        celery.conf.task_default_queue = schema
        fixture = worker = None
        log = directory / 'worker.log'
        with log.open('w') as worker_output, (directory / 'engine.log').open('w') as engine_output:
            try:
                fixture = subprocess.Popen([sys.executable, '/tests/l1_engine_fixture.py'], env=environment, stdout=engine_output, stderr=subprocess.STDOUT)
                def get(path):
                    with urllib.request.urlopen(environment['L1_ENGINE_URL'] + path, timeout=5) as response:
                        return json.load(response)
                until(lambda: get('/health'))
                worker = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'app.celery_app:celery', 'worker',
                    '-Q', schema, '--hostname=' + schema, '--loglevel=WARNING', '--concurrency=1',
                    '--without-gossip', '--without-mingle', '--without-heartbeat'], env=environment, stdout=worker_output, stderr=subprocess.STDOUT)
                credentials = json.loads(Path('/run/identity/bootstrap-demo.json').read_text())
                maker = Client('http://127.0.0.1:18011', [])
                maker.login(credentials, 'maker')
                scope = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
                original = maker.call('GET', scope + '/versions')[0]
                original = maker.call('GET', scope + '/versions/' + original['id'])
                plan = deepcopy(original['frozen']['normalized_plan'])
                for side in ('source', 'target'):
                    profile = maker.call('POST', scope + '/connections/' + plan[side]['connection_id'] + '/probe', {}, 201)
                    plan[side]['profile_id'] = profile['id']
                candidate = maker.call('POST', scope + '/studio/candidates', {'name': 'V10 隔离监督验证', 'plan': plan}, 201)
                version = maker.call('POST', scope + '/versions', {'candidate_id': candidate['id'], 'expected_revision': candidate['revision']}, 201)
                assert version['frozen']['execution_facts']['runtime_supervision_contract']['max_bytes'] == 268435456
                for name, sequence, expected_reason in cases:
                    # Deliberate fixture reset between independent scenarios in clone only.
                    with session.begin() as db:
                        db.execute(text('DELETE FROM target_ownership'))
                    with patch.object(outbox_publisher, 'Session', session):
                        client, scope, accepted = accept('http://127.0.0.1:18011', Path('/run/identity'), version_id=version['id'])
                        contract = accepted['execution']['supervision']['contract']
                        assert contract == accepted['managed_command']['payload']['runtime_supervision_contract']
                        assert outbox_publisher.publish_once() == 1
                    row = wait_run(client, scope, accepted['id'], lambda r: r['execution']['supervision']['latest_observation'] is not None, seconds=30)
                    if name == 'reject_99_100':
                        assert row['execution']['supervision']['current_metrics']['evaluated'] == 99
                        assert row['status'] == 'submitted' and row['execution']['supervision']['stop_request'] is None
                    if expected_reason:
                        row = wait_run(client, scope, accepted['id'], lambda r: r['execution']['supervision']['stop_request'] and r['execution']['supervision']['stop_request']['delivery_status'] == 'acknowledged', seconds=30)
                        supervision = row['execution']['supervision']
                        assert expected_reason in supervision['reasons'], (name, supervision['reasons'])
                        assert supervision['publication_veto'] and row['status'] == 'stop_requested'
                        assert supervision['stop_request']['termination_confirmed'] is False and row['execution']['ownership']['retained']
                        requests = get('/stop-requests')['requests']
                        assert len([r for r in requests if r['job_id'] == row['engine_job_id']]) == 1
                        # Redelivery uses the same durable intent and never clears veto.
                        with patch.object(execution_runtime, 'Session', session), patch.object(execution_runtime, 'engine', session.kw['bind']):
                            assert execution_runtime.drive(accepted['managed_command']['payload']['command_id']) == 'stop_requested'
                        if name in ('missing', 'stale', 'reset', 'estimated'):
                            assert all(v is None for v in supervision['current_metrics'].values())
                    elif name == 'zero':
                        supervision = row['execution']['supervision']
                        assert supervision['current_metrics']['reject_ratio'] is None
                        assert supervision['current_metrics']['amplification'] is None
                        assert supervision['current_metrics']['throughput_rows_per_second'] is None
                        assert supervision['publication_veto']
                        # Stop fixture polling so next scenario can own the shared synthetic target.
                        with session.begin() as db:
                            db.execute(text("UPDATE execution_runs SET status='supervision_passed' WHERE id=:id"), {'id': row['id']})
                    else:
                        assert row['status'] == 'supervision_passed' and not row['execution']['supervision']['publication_veto']
                    # A new HTTP Client reads the same persisted facts; no observation on GET.
                    again = client.call('GET', scope + '/runs/' + row['id'])
                    assert again['execution']['supervision'] == row['execution']['supervision']
                    passed.append({'scenario': name, 'status': row['status'], 'reason': expected_reason,
                                   'run_id': row['id'], 'supervision': row['execution']['supervision']})
                assert len(get('/ledger')['jobs']) == len(cases)
                report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'level': 'L1', 'passed': passed,
                          'worker_private_signing_key': 'EVIDENCE_SIGNING_KEY' in environment,
                          'root_deployment_untouched': True, 'physical_limits_or_zero_overshoot_verified': False}
                Path(output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
                print(json.dumps({'passed': [case['scenario'] for case in passed], 'engine_jobs': len(cases), 'level': 'L1'}))
            finally:
                for process in (worker, fixture):
                    if process:
                        process.terminate()
                        process.wait(timeout=15)
                with celery.connection_for_write() as connection:
                    connection.channel().queue_delete(schema)


if __name__ == '__main__':
    run(sys.argv[1])
