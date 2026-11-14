"""Few public L1 tracers: durable stop/termination/final facts and managed cleanup.

Only a disposable schema, queue and SQLite HTTP fixture are mutated. Never dumps
runtime credentials, encrypted secrets, or business rows into the evidence file.
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
from uuid import uuid4

from sqlalchemy import text
from app import outbox_publisher
from app.celery_app import celery
from app.db import engine
from isolated_api_fixture import isolated_api
from verify_execution_http import accept, wait_run
from verify_identity import Client
from verify_supervision_l1 import until


def run(output):
    basic = dict(source_read=100, evaluated=100, valid=100, rejected=0, final_output=100, sink_bytes=1000, elapsed_seconds=1)
    with isolated_api('control', 18012) as (session, schema), tempfile.TemporaryDirectory() as directory:
        directory = Path(directory)
        with session.begin() as db:
            db.execute(text('DELETE FROM target_ownership'))
            db.execute(text("UPDATE execution_outbox SET status='sent'"))
            # Baseline runs must not be resumed by this isolated publisher.
            db.execute(text("UPDATE execution_runs SET status='denied'"))
        environment = {key: value for key, value in os.environ.items() if key != 'EVIDENCE_SIGNING_KEY'}
        environment['DATABASE_URL'] = engine.url.update_query_dict({'options': f'-csearch_path={schema}'}).render_as_string(False)
        environment.update(L1_ENGINE_URL='http://127.0.0.1:8011', L1_LEDGER=str(directory / 'jobs.sqlite3'))
        os.environ['L1_ENGINE_URL'] = environment['L1_ENGINE_URL']
        config = directory / 'engine.json'
        config.write_text(json.dumps({'__metric_scenarios': [[basic, {**basic, 'source_read': 100001},
            {**basic, 'source_read': 100004}, {**basic, 'source_read': 100009, 'state': 'finished'}]],
            '__control': {'terminal_after_stop_polls': 2, 'final_after_terminal_polls': 2}}))
        environment['L1_FIXTURE_CONFIG'] = str(config)
        celery.conf.task_default_queue = schema
        fixture = worker = None
        with (directory / 'worker.log').open('w') as worker_output, (directory / 'engine.log').open('w') as engine_output:
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
                url = 'http://127.0.0.1:18012'
                maker, operator = Client(url, []), Client(url, [])
                maker.login(credentials, 'maker'); operator.login(credentials, 'operator')
                scope = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
                original = maker.call('GET', scope + '/versions')[0]
                original = maker.call('GET', scope + '/versions/' + original['id'])
                plan = deepcopy(original['frozen']['normalized_plan'])
                for side in ('source', 'target'):
                    profile = maker.call('POST', scope + '/connections/' + plan[side]['connection_id'] + '/probe', {}, 201)
                    plan[side]['profile_id'] = profile['id']
                candidate = maker.call('POST', scope + '/studio/candidates', {'name': 'V11 控制事实验证', 'plan': plan}, 201)
                version = maker.call('POST', scope + '/versions', {'candidate_id': candidate['id'], 'expected_revision': candidate['revision']}, 201)
                with patch.object(outbox_publisher, 'Session', session):
                    client, scope, accepted = accept(url, Path('/run/identity'), version_id=version['id'])
                    assert outbox_publisher.publish_once() == 1
                path = scope + '/runs/' + accepted['id']
                row = wait_run(client, scope, accepted['id'], lambda r: r['status'] == 'submitted', seconds=30)
                body = {'idempotency_key': str(uuid4()), 'reason': 'Operator 请求停止核对'}
                client.call('POST', path + '/stop', body, 403)
                operator.call('POST', path + '/stop', body, 202)
                ack = wait_run(client, scope, accepted['id'], lambda r: r['execution']['supervision']['stop_request'] and
                    r['execution']['supervision']['stop_request']['delivery_status'] == 'acknowledged', seconds=30)
                assert not ack['execution']['control']['termination']['confirmed']
                operator.call('POST', path + '/cleanup', {'idempotency_key': str(uuid4()), 'reason': '终止前不得清理'}, 409)
                assert get('/effects')['cleanup'] == []
                terminal = wait_run(client, scope, accepted['id'], lambda r: r['status'] == 'final_metrics_pending', seconds=30)
                assert terminal['execution']['control']['termination']['confirmed']
                assert terminal['execution']['control']['final_metrics']['status'] == 'pending'
                final = wait_run(client, scope, accepted['id'], lambda r: r['status'] == 'terminated', seconds=30)
                control = final['execution']['control']
                assert final['execution']['supervision']['publication_veto']
                assert control['final_metrics']['status'] == 'known'
                assert control['final_metrics']['overshoot']['source_read'] == 9
                assert control['final_metrics']['post_detection_delta']['source_read'] > 0
                stop = final['execution']['supervision']['stop_request']
                assert stop['detected_at'] <= stop['dispatched_at'] <= stop['acknowledged_at'] <= control['termination']['confirmed_at']
                assert control['termination']['terminated_at'] is not None
                clean_body = {'idempotency_key': str(uuid4()), 'reason': '清理本次未发布对象'}
                effect = operator.call('POST', path + '/cleanup', clean_body, 202)
                repeated = operator.call('POST', path + '/cleanup', clean_body, 202)
                assert effect['effect_id'] == repeated['effect_id']
                with patch.object(outbox_publisher, 'Session', session):
                    outbox_publisher.publish_once()
                cleaned = wait_run(client, scope, accepted['id'], lambda r: r['status'] == 'cleaned', seconds=30)
                assert cleaned['execution']['ownership'] is None
                assert len(get('/ledger')['jobs']) == 1
                assert len(get('/effects')['cleanup']) == 1
                assert cleaned['execution']['control']['effects'][-1]['phase'] == 'confirmed'
                # GET from a separate HTTP session reads the same durable facts.
                fresh = Client(url, []); fresh.login(credentials, 'auditor')
                assert fresh.call('GET', path)['execution']['control'] == cleaned['execution']['control']
                report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'level': 'L1',
                    'passed': ['public Operator stop; ACK is not terminal; late final metrics; actual overshoot; independent managed cleanup'],
                    'run_id': cleaned['id'], 'control': cleaned['execution']['control'],
                    'stop': stop, 'root_deployment_untouched': True, 'worker_private_signing_key': False,
                    'zero_overshoot_or_real_etl_or_rollback_verified': False}
                Path(output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
                print(json.dumps({'passed': report['passed'], 'level': 'L1'}))
            finally:
                for process in (worker, fixture):
                    if process:
                        process.terminate(); process.wait(timeout=15)
                with celery.connection_for_write() as connection:
                    connection.channel().queue_delete(schema)


if __name__ == '__main__':
    run(sys.argv[1])
