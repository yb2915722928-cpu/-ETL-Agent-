"""Private real-PG/Redis/Celery regression process; invoked by the host driver.

The schema, queue, worker and SQLite engine belong only to this fixture. The
production task runs unchanged, with public-key-only worker environment.
"""
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
from verify_execution_http import accept


def until(predicate, seconds=40):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.1)
    raise AssertionError('database retry fixture timed out')


def run():
    control = Path(sys.argv[1])
    passed, failures = [], []
    with isolated_api('db_retry', 18010) as (session, schema), tempfile.TemporaryDirectory() as directory:
        directory = Path(directory)
        with session.begin() as db:
            # Only this disposable clone: release cloned targets, suppress cloned
            # publications. Original public rows/Engine ledger remain untouched.
            db.execute(text('DELETE FROM target_ownership'))
            db.execute(text("UPDATE execution_outbox SET status='sent'"))
        environment = {key: value for key, value in os.environ.items() if key != 'EVIDENCE_SIGNING_KEY'}
        environment['DATABASE_URL'] = engine.url.update_query_dict(
            {'options': f'-csearch_path={schema}'}).render_as_string(False)
        environment['L1_ENGINE_URL'] = 'http://127.0.0.1:8011'
        environment['L1_LEDGER'] = str(directory / 'jobs.sqlite3')
        config = directory / 'engine.json'
        config.write_text('{}')
        environment['L1_FIXTURE_CONFIG'] = str(config)
        celery.conf.task_default_queue = schema
        worker_log = directory / 'worker.log'
        engine_log = directory / 'engine.log'
        worker = fixture = None
        with worker_log.open('w') as worker_output, engine_log.open('w') as engine_output:
            try:
                fixture = subprocess.Popen([sys.executable, '/tests/l1_engine_fixture.py'],
                    env=environment, stdout=engine_output, stderr=subprocess.STDOUT)
                def jobs():
                    with urllib.request.urlopen(environment['L1_ENGINE_URL'] + '/ledger', timeout=5) as response:
                        return json.load(response)['jobs']
                def engine_ready():
                    try:
                        jobs()
                        return True
                    except OSError:
                        return False
                until(engine_ready)
                # Isolated publisher executes the real Outbox function against
                # the clone and unique queue, while no consumer exists yet.
                with patch.object(outbox_publisher, 'Session', session), patch.object(
                        execution_runtime, 'engine', session.kw['bind']):
                    client, scope, accepted = accept('http://127.0.0.1:18010', Path('/run/identity'), 'metadata.read')
                    assert outbox_publisher.publish_once() == 1
                run_path = scope + '/runs/' + accepted['id']
                assert client.call('GET', run_path)['outbox']['status'] == 'sent'
                control.joinpath('outage-ready').touch()
                until(lambda: control.joinpath('postgres-stopped').exists())
                worker = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'app.celery_app:celery',
                    'worker', '-Q', schema, '--hostname=' + schema, '--loglevel=INFO', '--concurrency=1',
                    '--without-gossip', '--without-mingle', '--without-heartbeat'], env=environment,
                    stdout=worker_output, stderr=subprocess.STDOUT)
                until(lambda: 'OperationalError' in worker_log.read_text())
                control.joinpath('outage-observed').touch()
                until(lambda: control.joinpath('postgres-restored').exists())
                time.sleep(.5)
                deadline = time.monotonic() + 22
                while time.monotonic() < deadline:
                    outage_row = client.call('GET', run_path)
                    if outage_row['status'] == 'metadata_checked':
                        break
                    time.sleep(.2)
                if outage_row['status'] != 'metadata_checked':
                    failures.append('sent Outbox command remained queued after PG restore; ordinary worker failure ACKed')
                else:
                    assert outage_row['id'] == accepted['id'] and outage_row['execution']['attempt'] is None
                    assert outage_row['outbox']['status'] == 'sent' and outage_row['outbox']['attempts'] == 1
                    assert not jobs() and worker.poll() is None
                    passed.append('same accepted metadata Run resumes automatically after actual PostgreSQL outage with healthy worker and sent Outbox')
                # A real deferred PostgreSQL transaction error AFTER external
                # POST: original Attempt transaction already committed.
                with session.begin() as db:
                    before_consumptions = db.scalar(text('SELECT count(*) FROM capability_consumptions'))
                    db.execute(text("CREATE FUNCTION fail_result() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                        "IF NEW.action='execution.submitted' THEN RAISE EXCEPTION 'review transient result commit' "
                        "USING ERRCODE='40001'; END IF; RETURN NEW; END $$"))
                    db.execute(text('CREATE CONSTRAINT TRIGGER fail_result AFTER INSERT ON evidence_events '
                        'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_result()'))
                with patch.object(outbox_publisher, 'Session', session), patch.object(
                        execution_runtime, 'engine', session.kw['bind']):
                    client, scope, submitted = accept('http://127.0.0.1:18010', Path('/run/identity'))
                    assert outbox_publisher.publish_once() == 1
                until(lambda: len(jobs()) == 1)
                until(lambda: 'review transient result commit' in worker_log.read_text())
                pending = client.call('GET', scope + '/runs/' + submitted['id'])
                assert pending['status'] == 'submitting' and pending['engine_job_id'] is None
                attempt = pending['execution']['attempt']
                original_job = jobs()[0]
                assert attempt['correlation_id'] == original_job['correlation_id']
                assert attempt['request_sha256'] == original_job['request_sha256']
                assert attempt['request'] == original_job['request'] and pending['execution']['ownership']['retained']
                assert not any(event['action'] == 'execution.submitted' for event in pending['execution']['control_facts'])
                with session.begin() as db:
                    db.execute(text('DROP TRIGGER fail_result ON evidence_events'))
                    db.execute(text('DROP FUNCTION fail_result()'))
                deadline = time.monotonic() + 22
                while time.monotonic() < deadline:
                    final = client.call('GET', scope + '/runs/' + submitted['id'])
                    if final['status'] == 'submitted':
                        break
                    time.sleep(.2)
                if final['status'] != 'submitted':
                    failures.append('post-POST SQLSTATE40001 left durable Attempt submitting; task ACKed without discovery')
                else:
                    assert final['engine_job_id'] == original_job['id'] and jobs() == [original_job]
                    assert final['execution']['attempt']['request'] == attempt['request']
                    assert final['execution']['attempt']['request_sha256'] == attempt['request_sha256']
                    assert final['execution']['attempt']['correlation_id'] == attempt['correlation_id']
                    assert final['execution']['ownership'] == pending['execution']['ownership']
                    assert any(event['action'] == 'execution.submission_unknown' for event in final['execution']['control_facts'])
                    with session() as db:
                        assert db.scalar(text('SELECT count(*) FROM capability_consumptions')) == before_consumptions + 1
                    passed.append('real deferred SQLSTATE40001 after POST rolls back result only; original Attempt/request/correlation/ownership survive and retry discovers exactly one original job without reconsumption')
                # Log excerpts omit SQL/parameters, credentials and deployment env.
                lines = [line for line in worker_log.read_text().splitlines() if
                    ('retry:' in line or 'succeeded in' in line or 'raised unexpected:' in line)]
                report = {'passed': passed, 'failures': failures, 'worker_log': lines,
                    'isolated_engine_jobs': len(jobs()), 'public_engine_ledger_untouched': True,
                    'worker_private_signing_key': 'EVIDENCE_SIGNING_KEY' in environment,
                    'outage_run_status': outage_row['status'], 'post_submit_status': final['status']}
                control.joinpath('report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
                print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
                return report
            finally:
                for process in (worker, fixture):
                    if process:
                        process.terminate()
                        process.wait(timeout=15)
                # Child worker consumed its own unique queue; remove only fixture
                # queue state. Redis unacked belongs to the ordinary deployment.
                with celery.connection_for_write() as connection:
                    connection.channel().queue_delete(schema)


if __name__ == '__main__':
    result = run()
    sys.exit(bool(result['failures']))
