"""Public L1 tracer: revoked duties, lost credentials, unknown calls, lease recovery.

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
    with isolated_api('recovery', 18013) as (session, schema), tempfile.TemporaryDirectory() as directory:
        directory = Path(directory)
        with session.begin() as db:
            db.execute(text('DELETE FROM target_ownership'))
            db.execute(text("UPDATE execution_outbox SET status='sent'"))
            db.execute(text("UPDATE execution_runs SET status='denied'"))
            db.execute(text('UPDATE run_controls SET wake_at=NULL'))
        environment = {key: value for key, value in os.environ.items() if key != 'EVIDENCE_SIGNING_KEY'}
        environment['DATABASE_URL'] = engine.url.update_query_dict({'options': f'-csearch_path={schema}'}).render_as_string(False)
        environment.update(L1_ENGINE_URL='http://127.0.0.1:8011', L1_LEDGER=str(directory / 'jobs.sqlite3'))
        os.environ['L1_ENGINE_URL'] = environment['L1_ENGINE_URL']
        config = directory / 'engine.json'
        # Both managed calls lose their receipt, so every outcome must be discovered
        # from the durable effect record instead of being replayed.
        config.write_text(json.dumps({'__metric_scenarios': [[basic, {**basic, 'source_read': 100001},
            {**basic, 'source_read': 100004}, {**basic, 'source_read': 100009, 'state': 'finished'}]],
            '__control': {'terminal_after_stop_polls': 2, 'final_after_terminal_polls': 2,
                'stop_response': 'withhold', 'cleanup_response': 'withhold'}}))
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
                url = 'http://127.0.0.1:18013'
                admin, maker = Client(url, []), Client(url, [])
                admin.login(credentials, 'admin'); maker.login(credentials, 'maker')
                scope = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
                member = next(row for row in admin.call('GET', scope + '/members')
                              if row['id'] == credentials['accounts']['operator']['id'])
                # Revocation keeps the account and session enrolled without the current
                # Operator duty, so the denial is observable through the public API.
                revoked = [role for role in member['roles'] if role != 'operator'] or ['auditor']
                membership = f"{scope}/members/{member['id']}"

                def prepare(name):
                    original = maker.call('GET', scope + '/versions')[0]
                    original = maker.call('GET', scope + '/versions/' + original['id'])
                    plan = deepcopy(original['frozen']['normalized_plan'])
                    for side in ('source', 'target'):
                        profile = maker.call('POST', scope + '/connections/' + plan[side]['connection_id'] + '/probe', {}, 201)
                        plan[side]['profile_id'] = profile['id']
                    candidate = maker.call('POST', scope + '/studio/candidates', {'name': name, 'plan': plan}, 201)
                    return maker.call('POST', scope + '/versions', {'candidate_id': candidate['id'],
                                           'expected_revision': candidate['revision']}, 201)

                def commit(name):
                    version = prepare(name)
                    with patch.object(outbox_publisher, 'Session', session):
                        client, _, accepted = accept(url, Path('/run/identity'), version_id=version['id'])
                        assert outbox_publisher.publish_once() == 1
                    return client, accepted

                def denial(client, run_id, needle):
                    reasons = [event['facts']['reason'] for event in client.call('GET', scope + '/evidence')['events']
                               if event['action'] == 'execution.denied' and event.get('run_id') == run_id]
                    assert reasons and needle in reasons[-1], reasons

                # A queued command whose Operator duty was revoked is refused before any
                # engine job exists, and the denial is durable evidence.
                client, accepted = commit('V12 撤销后不得提交')
                path = scope + '/runs/' + accepted['id']
                admin.call('PUT', membership, {'roles': revoked})
                wait_run(client, scope, accepted['id'], lambda r: r['status'] == 'denied', seconds=30)
                assert get('/ledger')['jobs'] == [], '撤销 Operator 职责后不得提交任何引擎作业'
                denial(client, accepted['id'], '职责')
                admin.call('PUT', membership, {'roles': member['roles']})

                # A queued command whose original bound credential can no longer be opened
                # is refused too; it never switches to another credential. The reference
                # row stays immutable, so only the sealed ciphertext is disturbed.
                client, accepted = commit('V12 原绑定凭据不可用')
                resources = accepted['managed_command']['payload']['resources']
                reference = next(asset['secret_reference_id'] for asset in resources.values() if asset.get('secret_reference_id'))
                with session.begin() as db:
                    sealed = db.scalar(text('SELECT ciphertext FROM secret_references WHERE id=:id'), {'id': reference})
                    db.execute(text('UPDATE secret_references SET ciphertext=:broken WHERE id=:id'),
                               {'broken': b'not-a-sealed-credential', 'id': reference})
                wait_run(client, scope, accepted['id'], lambda r: r['status'] == 'denied', seconds=30)
                assert get('/ledger')['jobs'] == [], '原绑定凭据不可用时不得提交任何引擎作业'
                denial(client, accepted['id'], '凭据')
                with session.begin() as db:
                    db.execute(text('UPDATE secret_references SET ciphertext=:sealed WHERE id=:id'),
                               {'sealed': sealed, 'id': reference})

                # A running command whose Operator duty was revoked stops itself under the
                # original signed contract, and a lost Worker never releases the target.
                client, accepted = commit('V12 运行中撤销与失联恢复')
                path = scope + '/runs/' + accepted['id']
                command_id = accepted['managed_command']['payload']['command_id']
                row = wait_run(client, scope, accepted['id'], lambda r: r['status'] == 'submitted', seconds=30)
                assert row['engine_job_id'] and row['execution']['ownership']['retained'] is True
                admin.call('PUT', membership, {'roles': revoked})
                operator = Client(url, []); operator.login(credentials, 'operator')
                dispatched = wait_run(client, scope, accepted['id'], lambda r: bool(r['execution']['control'].get('inflight')), seconds=30)
                inflight = dispatched['execution']['control']['inflight']
                assert inflight['kind'] == 'stop'
                assert inflight['request']['correlation_id'] and inflight['request']['request_sha256']
                assert inflight['request']['unpublished_object'] == 'l1-unpublished:' + command_id
                assert dispatched['execution']['ownership']['retained'] is True
                operator.call('POST', path + '/stop', {'idempotency_key': str(uuid4()), 'reason': '撤销后不得再授权停止'}, 403)
                operator.call('POST', path + '/cleanup', {'idempotency_key': str(uuid4()), 'reason': '撤销后不得清理'}, 403)
                # The Worker died with that call in flight: its lease is stale, and the
                # persisted call is discovered from the engine record, never replayed.
                with session.begin() as db:
                    db.execute(text('UPDATE run_controls SET lease_expires_at=:expired WHERE command_id=:command'),
                               {'expired': datetime(2000, 1, 1, tzinfo=timezone.utc), 'command': command_id})
                terminated = wait_run(client, scope, accepted['id'], lambda r: r['status'] == 'terminated', seconds=90)
                control = terminated['execution']['control']
                durable = {(event['action'], event['facts'].get('ownership_retained')) for event in
                           client.call('GET', scope + '/evidence')['events'] if event.get('run_id') == accepted['id']}
                actions = {action for action, _ in durable}
                assert ('control.authorization_veto', True) in durable, durable
                assert ('control.effect_unknown', True) in durable, durable
                assert ('control.lease_reconciled', True) in durable, durable
                assert 'control.termination_confirmed' in actions, durable
                stop = terminated['execution']['supervision']['stop_request']
                assert stop['reasons'] == ['authorization_revoked_or_secret_unavailable']
                assert stop['delivery_status'] == 'acknowledged'
                assert control['termination']['confirmed'] and control['termination']['basis'] == 'engine_terminal'
                assert control['termination']['terminated_at'] is not None
                assert control['final_metrics']['status'] == 'known'
                assert control['final_metrics']['overshoot']['source_read'] == 9
                assert terminated['execution']['supervision']['publication_veto']
                # Exactly one stop reached the engine and the two denied runs created none.
                assert len(get('/ledger')['jobs']) == 1
                assert len(get('/effects')['stop']) == 1
                derived = control['effects'][0]
                assert derived['kind'] == 'stop' and derived['actor_id'] == accepted['subject']
                assert derived['envelope']['derivation']['request_id'] == stop['id']
                assert derived['phase'] == 'confirmed'
                admin.call('PUT', membership, {'roles': member['roles']})
                clean_body = {'idempotency_key': str(uuid4()), 'reason': '核对后清理本次未发布对象'}
                operator.call('POST', path + '/cleanup', clean_body, 202)
                with patch.object(outbox_publisher, 'Session', session):
                    outbox_publisher.publish_once()
                # The cleanup receipt is lost too: the target stays owned until discovery.
                pending = wait_run(client, scope, accepted['id'], lambda r: bool(r['execution']['control'].get('inflight')), seconds=30)
                assert pending['execution']['control']['inflight']['kind'] == 'cleanup'
                assert pending['execution']['ownership']['retained'] is True
                operator.call('POST', path + '/cleanup', {'idempotency_key': str(uuid4()), 'reason': '在途结果未核对'}, 409)
                cleaned = wait_run(client, scope, accepted['id'], lambda r: r['status'] == 'cleaned', seconds=60)
                assert cleaned['execution']['ownership'] is None
                assert len(get('/effects')['cleanup']) == 1
                report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'level': 'L1', 'run_id': cleaned['id'],
                    'passed': ['queued revocation and lost bound credential deny submission with no engine job',
                        'running revocation latches a derived stop; lost stop receipt stays unknown and retains ownership',
                        'expired lease is reconciled without replaying the persisted call',
                        'late final metrics confirm real overshoot and publication veto; lost cleanup receipt still releases only after discovery'],
                    'control': cleaned['execution']['control'], 'stop': stop,
                    'root_deployment_untouched': True, 'worker_private_signing_key': False,
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
