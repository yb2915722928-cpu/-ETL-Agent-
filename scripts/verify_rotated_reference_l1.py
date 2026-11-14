"""One public credential-rotation tracer over isolated PG/Redis/Celery/HTTP L1.

Run inside the API test container with backend and /tests mounted read-only and
an output path mounted writable. Only a disposable schema, unique queue and
local engine ledger are mutated. Reuses the original secret in memory to test
reference rotation, not an external database password change. No SeaTunnel L2
claim. No worker, drive, authorization or HTTP implementation is mocked.
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

from sqlalchemy import select, text
from app import main, outbox_publisher
from app.celery_app import celery
from app.db import engine
from app.models import Connection, SecretReference
from isolated_api_fixture import isolated_api
from verify_execution_http import accept, wait_run
from verify_identity import Client
from verify_supervision_l1 import until


def run(output):
    basic = dict(source_read=100, evaluated=100, valid=100, rejected=0,
                 final_output=100, sink_bytes=1000, elapsed_seconds=1)
    previous_queue = celery.conf.task_default_queue
    previous_engine = os.environ.get('L1_ENGINE_URL')
    previous_adapter = os.environ.get('EXECUTION_ADAPTER')
    fixture = worker = None
    try:
        os.environ['EXECUTION_ADAPTER'] = 'l1'
        with isolated_api('rotation_bound', 18017) as (session, schema), tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            with session.begin() as db:
                db.execute(text('DELETE FROM target_ownership'))
                db.execute(text("UPDATE execution_outbox SET status='sent'"))
                db.execute(text("UPDATE execution_runs SET status='denied'"))
                db.execute(text('UPDATE run_controls SET wake_at=NULL'))
            environment = {key: value for key, value in os.environ.items() if key != 'EVIDENCE_SIGNING_KEY'}
            environment['DATABASE_URL'] = engine.url.update_query_dict(
                {'options': f'-csearch_path={schema}'}).render_as_string(False)
            environment.update(EXECUTION_ADAPTER='l1', L1_ENGINE_URL='http://127.0.0.1:8011',
                               L1_LEDGER=str(directory / 'jobs.sqlite3'))
            os.environ['L1_ENGINE_URL'] = environment['L1_ENGINE_URL']
            config = directory / 'engine.json'
            # Stable healthy counters cannot themselves trigger a stop. Terminal
            # and final metrics only follow the authorization-derived stop.
            config.write_text(json.dumps({'__metric_scenarios': [[basic]],
                '__control': {'terminal_after_stop_polls': 2, 'final_after_terminal_polls': 2}}), encoding='utf-8')
            environment['L1_FIXTURE_CONFIG'] = str(config)
            celery.conf.task_default_queue = schema
            with (directory / 'worker.log').open('w') as worker_log, (directory / 'engine.log').open('w') as engine_log:
                try:
                    fixture = subprocess.Popen([sys.executable, '/tests/l1_engine_fixture.py'],
                        env=environment, stdout=engine_log, stderr=subprocess.STDOUT)

                    def get(path):
                        with urllib.request.urlopen(environment['L1_ENGINE_URL'] + path, timeout=5) as response:
                            return json.load(response)

                    until(lambda: get('/health'))
                    worker = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'app.celery_app:celery', 'worker',
                        '-Q', schema, '--hostname=' + schema, '--loglevel=WARNING', '--concurrency=1',
                        '--without-gossip', '--without-mingle', '--without-heartbeat'],
                        env=environment, stdout=worker_log, stderr=subprocess.STDOUT)
                    credentials = json.loads(Path('/run/identity/bootstrap-demo.json').read_text(encoding='utf-8'))
                    url = 'http://127.0.0.1:18017'
                    scope = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
                    clients = {role: Client(url, []) for role in ('maker', 'operator', 'data_checker', 'security_checker')}
                    for role, client in clients.items():
                        client.login(credentials, role)
                    maker, operator = clients['maker'], clients['operator']
                    original = maker.call('GET', scope + '/versions')[0]
                    original = maker.call('GET', scope + '/versions/' + original['id'])
                    plan = deepcopy(original['frozen']['normalized_plan'])

                    def freeze(name):
                        for side in ('source', 'target'):
                            profile = maker.call('POST', scope + '/connections/' + plan[side]['connection_id'] + '/probe', {}, 201)
                            plan[side]['profile_id'] = profile['id']
                        candidate = maker.call('POST', scope + '/studio/candidates', {'name': name, 'plan': plan}, 201)
                        return maker.call('POST', scope + '/versions', {'candidate_id': candidate['id'],
                            'expected_revision': candidate['revision']}, 201)

                    version = freeze('V20 原凭据运行绑定')
                    # Only route the existing publisher to this disposable schema.
                    # It still delivers a real task over Redis to a real Celery worker.
                    with patch.object(outbox_publisher, 'Session', session):
                        auditor, _, accepted = accept(url, Path('/run/identity'), version_id=version['id'], credentials=credentials)
                        assert outbox_publisher.publish_once() == 1
                    run_id = accepted['id']
                    command_id = accepted['managed_command']['payload']['command_id']
                    old_preparation_id = accepted['preparation_id']
                    submitted = wait_run(auditor, scope, run_id, lambda row: row['status'] == 'submitted', seconds=45)
                    job_id = submitted['engine_job_id']
                    assert job_id and submitted['execution']['ownership']['retained']
                    original_binding = deepcopy(submitted['managed_command']['payload']['resources']['target'])
                    reference_id, old_version = original_binding['secret_reference_id'], original_binding['secret_version']
                    connection_id = plan['target']['connection_id']
                    with session() as db:
                        reference = db.get(SecretReference, reference_id)
                        assert reference.revoked_at is None
                        candidate_secret = main.app.state.secret_provider.open(reference)
                    # Input remains only in memory. Every later API response is
                    # checked for the exact candidate plaintext by Client.
                    for client in [*clients.values(), auditor]:
                        client.secrets.append(candidate_secret)
                    prepared = operator.call('POST', scope + '/connections/' + connection_id + '/credential-rotations',
                        {'slot': 'probe_readonly', 'credential': candidate_secret}, 201)
                    rotation_path = scope + '/preparations/' + prepared['id']
                    assert prepared['version_id'] is None and prepared['risk_level'] == 'P1'
                    assert prepared['validation']['current_eligibility']['operator']
                    for role, slot in (('data_checker', 'data'), ('security_checker', 'security')):
                        clients[role].call('POST', rotation_path + '/decisions', {'slot': slot, 'decision': 'approve'})
                    envelope = operator.call('POST', rotation_path + '/capabilities', {}, 201)['capability']
                    assert envelope['payload']['expires_at'] - envelope['payload']['issued_at'] == 300
                    rotated = operator.call('POST', rotation_path + '/commit',
                        {'capability': envelope, 'idempotency_key': str(uuid4())}, 201)
                    assert rotated['status'] == 'succeeded' and rotated['outbox']['status'] == 'applied_inline'
                    with session() as db:
                        current = db.get(Connection, connection_id)
                        new_reference_id, new_version = current.secret_reference_id, current.secret_version
                        assert new_reference_id != reference_id and new_version == old_version + 1
                        assert db.get(SecretReference, reference_id).revoked_at is not None
                    old_prepare = maker.call('GET', scope + '/preparations/' + old_preparation_id)
                    assert old_prepare['validation']['state'] == 'invalidated'
                    # The same external credential still validates for this new
                    # reference. Fresh profiling/freezing/Prepare must bind it.
                    fresh_version = freeze('V20 新引用可重新准备')
                    fresh_prepare = maker.call('POST', scope + '/preparations',
                        {'version_id': fresh_version['id'], 'tool': 'pipeline.publish'}, 201)
                    fresh_binding = fresh_prepare['frozen']['execution_facts']['target']
                    assert fresh_binding['secret_reference_id'] == new_reference_id
                    assert fresh_binding['secret_version'] == new_version
                    assert fresh_prepare['validation']['state'] == 'pending'
                    terminated = wait_run(auditor, scope, run_id, lambda row: row['status'] == 'terminated', seconds=90)
                    assert terminated['engine_job_id'] == job_id
                    assert terminated['managed_command']['payload']['command_id'] == command_id
                    assert terminated['managed_command']['payload']['resources']['target'] == original_binding
                    assert terminated['execution']['attempt']['engine_job_id'] == job_id
                    assert terminated['execution']['ownership']['retained'] is True
                    supervision, control = terminated['execution']['supervision'], terminated['execution']['control']
                    assert supervision['publication_veto'] is True
                    assert supervision['stop_request']['reasons'] == ['authorization_revoked_or_secret_unavailable']
                    assert supervision['stop_request']['delivery_status'] == 'acknowledged'
                    assert control['termination']['confirmed'] and control['final_metrics']['status'] == 'known'
                    derived = [effect for effect in control['effects'] if effect['kind'] == 'stop']
                    assert len(derived) == 1 and derived[0]['phase'] == 'confirmed'
                    assert derived[0]['envelope']['derivation']['command_id'] == command_id
                    assert derived[0]['envelope']['derivation']['request_id'] == supervision['stop_request']['id']
                    evidence = auditor.call('GET', scope + '/evidence')['events']
                    vetoes = [event for event in evidence if event.get('run_id') == run_id and
                              event['action'] == 'control.authorization_veto']
                    assert vetoes and '凭据' in vetoes[-1]['facts']['reason']
                    effects = get('/effects')
                    assert len(effects['stop']) == 1 and effects['stop'][0]['job_id'] == job_id
                    assert effects['stop'][0]['unpublished_object'] == 'l1-unpublished:' + command_id
                    assert len(get('/ledger')['jobs']) == 1, 'rotation must never submit a replacement job'
                    # Even confirmed termination is insufficient to grant cleanup
                    # through an unavailable original credential. Keep ownership.
                    denial = operator.call('POST', scope + '/runs/' + run_id + '/cleanup',
                        {'idempotency_key': str(uuid4()), 'reason': '撤销旧绑定后禁止清理'}, 409)
                    assert '凭据' in denial['detail']
                    retained = auditor.call('GET', scope + '/runs/' + run_id)
                    assert retained['execution']['ownership']['retained'] and not get('/effects')['cleanup']
                    report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'level': 'L1',
                        'run_id': run_id, 'command_id': command_id, 'engine_job_id': job_id,
                        'rotation_run_id': rotated['id'], 'rotation_preparation_id': prepared['id'],
                        'connection_id': connection_id, 'slot': 'probe_readonly',
                        'previous_reference_id': reference_id, 'previous_version': old_version,
                        'new_reference_id': new_reference_id, 'new_version': new_version,
                        'old_preparation_state': old_prepare['validation']['state'],
                        'fresh_preparation_state': fresh_prepare['validation']['state'],
                        'stop_delivery': supervision['stop_request']['delivery_status'],
                        'derived_stop_effect_id': derived[0]['id'], 'termination_confirmed': True,
                        'publication_veto': True, 'ownership_retained': True, 'cleanup_denied': True,
                        'original_engine_job_unchanged': True, 'original_command_binding_unchanged': True,
                        'passed': ['public pipeline Prepare/dualApprove/Capability/Commit starts one real Celery HTTP L1 job',
                            'public Operator P1 rotation independently dual-approved; new reference/version and old revocation persist',
                            'old pipeline Prepare drifts; new profile/version/Prepare binds the replacement reference',
                            'running old binding latches authorization veto and one derived stop against the original engine job',
                            'terminal confirmation retains ownership; unavailable original reference denies cleanup without engine cleanup'],
                        'credential_plaintext_absent': True, 'reference_rotation_only': True,
                        'external_database_password_changed': False, 'root_deployment_untouched': True,
                        'real_seatunnel_l2_verified': False}
                    serialized = json.dumps(report, ensure_ascii=False, indent=2)
                    assert candidate_secret not in serialized
                    worker_log.flush(); engine_log.flush()
                    for log in (directory / 'worker.log', directory / 'engine.log'):
                        assert candidate_secret not in log.read_text(encoding='utf-8', errors='replace'), 'Sensitive plaintext in fixture logs'
                    destination = Path(output)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_text(serialized, encoding='utf-8')
                    print(json.dumps({'passed': report['passed'], 'level': 'L1', 'real_seatunnel_l2_verified': False}))
                finally:
                    for process in (worker, fixture):
                        if process and process.poll() is None:
                            process.terminate()
                            try:
                                process.wait(timeout=15)
                            except subprocess.TimeoutExpired:
                                process.kill(); process.wait(timeout=5)
                    with celery.connection_for_write() as connection:
                        connection.channel().queue_delete(schema)
    finally:
        celery.conf.task_default_queue = previous_queue
        for key, previous in (('L1_ENGINE_URL', previous_engine), ('EXECUTION_ADAPTER', previous_adapter)):
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


if __name__ == '__main__':
    run(sys.argv[1] if len(sys.argv) > 1 else '/evidence/rotation-bound-l1.json')
