"""Public approval/Celery tracer in one owned gateway, never a mocked L2 result.

Private inputs stay under /app/l2. Durable output contains IDs, hashes, counters
and finite outcomes only. The isolated PostgreSQL schema is deliberately kept
for diagnosis if the external outcome is unknown; no Doris table is dropped.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import urlsplit
from uuid import uuid4

PRIVATE = Path('/app/l2')
sys.path[:0] = ['/app', '/app/tests']
os.environ.update(json.loads((PRIVATE / 'environment.json').read_text(encoding='utf-8')))
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from app import main
from app.connector_policy import ResourcePolicy, fingerprint
from app.db import Base, engine
from app.evidence_signing import EvidenceSigner
from app.managed_secrets import SecretProvider
from app.execution import ExecutionRun, ManagedCommand
from app.execution_runtime import EngineAttempt, TargetOwnership
from app.run_control import RunControl
from app.supervision_runtime import RuntimeSupervision
from app.quality_runtime import QualityRun
from app.real_runtime import RealObjectIntent
from app.managed_load import LoadRun, LoadBatch
from app.models import Connection, SecretReference
from app.publication import PublicationIntent, PublishedSnapshot
from verify_identity import Client
import uvicorn
import threading
import pymysql


def wait(predicate, seconds=90):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        result = predicate()
        if result:
            return result
        time.sleep(.2)
    raise RuntimeError('bounded_wait_expired')


def reconciliation_summary(row):
    # Keep public facts and the signed-envelope digest, not a second copy of the
    # entire frozen plan/resource metadata in the exported protocol evidence.
    result = {key: row.get(key) for key in ('record_id', 'command_id', 'phase', 'reason', 'basis', 'result')}
    envelope = row.get('evidence') or {}
    result['evidence_sha256'] = hashlib.sha256(json.dumps(envelope, sort_keys=True,
        separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    return result


def rotation_acceptance(report):
    """Accept isolation only; unresolved Doris loads are never full managed L2."""
    rotation = report.get('rotation_public_api') or {}
    termination = (report.get('control') or {}).get('termination') or {}
    closure = report.get('ingress_closure') or {}
    route_commands = closure.get('route_commands') or {}
    routes = closure.get('routes') or {}
    original_id = rotation.get('original_engine_job_id')
    command_id = report.get('command_id')
    job_name = 'etl_agent_real_' + (command_id or '').replace('-', '')
    closed = (bool(command_id) and bool(report.get('run_id'))
        and closure.get('command_id') == command_id
        and closure.get('run_id') == report.get('run_id')
        and closure.get('engine_job_id') == original_id and closure.get('closed') is True
        and set(route_commands) == {'valid', 'rejected'}
        and route_commands == report.get('original_route_commands')
        and all(isinstance(value, str) and value for value in route_commands.values())
        and len(set(route_commands.values())) == 2
        and set(routes) == {'valid', 'rejected'}
        and all(routes[name].get('command_id') == route_commands[name]
                and routes[name].get('run_id') == report.get('run_id')
                and routes[name].get('closed') is True and bool(routes[name].get('close_id'))
                for name in ('valid', 'rejected')))
    terminal = (bool(original_id) and original_id == report.get('engine_job_id')
        and termination.get('confirmed') is True and termination.get('basis') == 'engine_terminal'
        and termination.get('engine_state') in ('FINISHED', 'CANCELED', 'FAILED')
        and termination.get('engine_job_id') == original_id and termination.get('job_name') == job_name)
    versions = (isinstance(rotation.get('old_version'), int)
        and not isinstance(rotation.get('old_version'), bool)
        and isinstance(rotation.get('new_version'), int)
        and not isinstance(rotation.get('new_version'), bool)
        and rotation['new_version'] == rotation['old_version'] + 1)
    accepted = (report.get('scenario') == 'rotation' and report.get('public_approval_path_tested') is True
        and rotation.get('run_status_before_rotation') is not None
        and rotation['run_status_before_rotation'] not in
            ('supervision_passed', 'terminated', 'final_metrics_pending', 'published', 'cleaned')
        and rotation.get('old_revoked') is True and bool(rotation.get('old_reference_id'))
        and bool(rotation.get('new_reference_id'))
        and rotation['new_reference_id'] != rotation['old_reference_id'] and versions
        and rotation.get('fresh_preparation_reference_id') == rotation['new_reference_id']
        and rotation.get('old_preparation_state') == 'invalidated'
        and rotation.get('original_signed_writer_unchanged') is True
        and rotation.get('cleanup_denied') is True
        and (report.get('supervision') or {}).get('publication_veto') is True
        and report.get('ownership_retained') is True and terminal and closed)
    batches = report.get('load_batches')
    pending = (not closed or not isinstance(batches, list) or any(
        batch.get('phase') != 'receipt' or (batch.get('receipt') or {}).get('Status') != 'Success'
        for batch in batches))
    return {'rotation_safety_accepted': bool(accepted), 'rotation_path_accepted': bool(accepted),
        'rotation_acceptance_scope': 'safety_isolation_only', 'load_settlement_pending': pending,
        'managed_l2_accepted': False}


def run():
    schema = 'managed_l2_' + uuid4().hex
    scenario = os.environ.get('REAL_L2_SCENARIO', 'happy')
    target_table = os.environ['REAL_L2_TARGET_TABLE']
    report = {'schema': 'real-managed-l2-tracer/v1', 'checked_at': datetime.now(timezone.utc).isoformat(),
              'level': 'real_managed_path_probe', 'managed_l2_accepted': False,
              'public_approval_path_tested': False, 'postgresql_schema': schema, 'scenario': scenario,
              'target_table': target_table, 'phase': 'schema_setup'}
    output = PRIVATE / 'result.json'
    def save():
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    save()
    isolated = create_engine(engine.url, hide_parameters=True, pool_pre_ping=True,
                             connect_args={'options': '-csearch_path=' + schema})
    sessions = sessionmaker(isolated, expire_on_commit=False)
    worker = publisher = server = thread = None
    try:
        with engine.begin() as db:
            db.execute(text(f'CREATE SCHEMA {schema}'))
            Base.metadata.create_all(db.execution_options(schema_translate_map={None: schema}))
            # Clone only baseline identity; all ETL objects below use public APIs.
            for name in ('tenants', 'accounts', 'projects', 'project_memberships'):
                table = Base.metadata.tables[name]
                columns = ','.join('"' + col.name + '"' for col in table.columns)
                db.execute(text(f'INSERT INTO {schema}."{name}" ({columns}) SELECT {columns} FROM public."{name}"'))
        (PRIVATE / 'schema.json').write_text(json.dumps({'schema': schema}), encoding='utf-8')
        signer = EvidenceSigner()
        os.environ['COMMAND_PUBLIC_KEY'] = signer.public['public_key']
        environment = dict(os.environ)
        environment['DATABASE_URL'] = engine.url.update_query_dict({'options': '-csearch_path=' + schema}).render_as_string(False)
        # Gateway server reads these exact application keys and this isolated schema.
        envfile = json.loads((PRIVATE / 'environment.json').read_text(encoding='utf-8'))
        envfile['COMMAND_PUBLIC_KEY'] = os.environ['COMMAND_PUBLIC_KEY']
        (PRIVATE / 'environment.json').write_text(json.dumps(envfile), encoding='utf-8')
        (PRIVATE / 'gateway-takeover').write_text('owned local process takeover', encoding='ascii')
        wait(lambda: (PRIVATE / 'ingress-ready').exists(), 15)
        def database():
            with sessions.begin() as db:
                yield db
        main.app.dependency_overrides[main.database] = database
        main.app.state.secret_provider = SecretProvider()
        main.app.state.evidence_signer = signer
        main.app.state.connector_policy = ResourcePolicy()
        server = uvicorn.Server(uvicorn.Config(main.app, host='127.0.0.1', port=18017,
                                lifespan='off', log_level='critical', access_log=False))
        thread = threading.Thread(target=server.run, daemon=True); thread.start()
        wait(lambda: server.started, 10)
        credentials = json.loads((PRIVATE / 'bootstrap.json').read_text(encoding='utf-8'))
        config = json.loads((PRIVATE / 'probe.json').read_text(encoding='utf-8'))
        readonly = json.loads((PRIVATE / 'readonly.json').read_text(encoding='utf-8'))
        source_secret = config['source'][0]['pass' + 'word']
        writer_secret = config['sink'][0]['pass' + 'word']
        target_secret = readonly['pass' + 'word']
        all_secrets = [source_secret, writer_secret, target_secret,
                       *[v['pass' + 'word'] for v in credentials['accounts'].values()]]
        clients = {role: Client('http://127.0.0.1:18017', all_secrets)
                   for role in ('maker', 'operator', 'data_checker', 'security_checker', 'auditor')}
        for role, client in clients.items(): client.login(credentials, role)
        scope = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
        maker = clients['maker']
        report['phase'] = 'public_connections'; save()
        refs = {}
        for side, secret in (('source', source_secret), ('target', target_secret)):
            connection = maker.call('POST', scope + '/connections',
                {'name': '真实受管L2-' + side + '-' + schema[-8:], 'resource_id': 'l2-' + side,
                 'pass' + 'word': secret}, 201)
            if side == 'target':
                maker.call('POST', scope + '/connections/' + connection['id'] + '/credentials/execution_writer',
                           {'pass' + 'word': writer_secret}, 201)
            profile = maker.call('POST', scope + '/connections/' + connection['id'] + '/probe', {}, 201)
            refs[side] = {'connection_id': connection['id'], 'profile_id': profile['id'],
                          'table': 'customers' if side == 'source' else target_table}
        plan = {'version': 'etl-plan-v1', 'mode': 'bounded_full', **refs,
                'mappings': [{'source': 'id', 'target': 'id', 'operation': 'direct'},
                             {'source': 'full_name', 'target': 'full_name', 'operation': 'direct'}],
                'quality': [], 'budget': {'max_rows': 10, 'max_bytes': 1048576, 'max_seconds': 60,
                                         'max_amplification': 2.0, 'max_reject_ratio': 0.2}}
        if scenario == 'quality':
            plan.update(cleaning=[{'field': 'full_name', 'kind': 'trim'}],
                quality=[{'kind': 'not_null', 'field': 'full_name'},
                         {'kind': 'numeric_range', 'field': 'id', 'minimum': 1, 'maximum': 1000000}],
                filters=[{'field': 'id', 'equals': 2}])
        candidate = maker.call('POST', scope + '/studio/candidates', {'name': '真实受管L2', 'plan': plan}, 201)
        version = maker.call('POST', scope + '/versions', {'candidate_id': candidate['id'],
                            'expected_revision': candidate['revision']}, 201)
        report['phase'] = 'public_prepare'; save()
        preparation = maker.call('POST', scope + '/preparations',
                                  {'version_id': version['id'], 'tool': 'pipeline.publish' if scenario == 'publish' else 'pipeline.trial'}, 201)
        path = scope + '/preparations/' + preparation['id']
        for role, slot in (('data_checker', 'data'), ('security_checker', 'security')):
            clients[role].call('POST', path + '/decisions', {'slot': slot, 'decision': 'approve'})
        capability = clients['operator'].call('POST', path + '/capabilities', {}, 201)['capability']
        queue = schema
        workerlog = (PRIVATE / 'worker.log').open('w')
        worker = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'app.celery_app:celery', 'worker',
            '-Q', queue, '--hostname=' + queue, '--loglevel=WARNING', '--concurrency=1', '--without-gossip',
            '--without-mingle', '--without-heartbeat'], env=environment, stdout=workerlog, stderr=subprocess.STDOUT)
        time.sleep(2)
        accepted = clients['operator'].call('POST', path + '/commit',
            {'capability': capability, 'idempotency_key': str(uuid4())}, 201)
        report.update(phase='public_commit_accepted', public_approval_path_tested=True,
            run_id=accepted['id'], command_id=accepted['managed_command']['payload']['command_id'],
            version_id=version['id'], preparation_id=preparation['id'])
        save()
        code = "from app.celery_app import celery; celery.conf.task_default_queue=" + repr(queue) + "; from app.outbox_publisher import publish_once; print(publish_once())"
        published = subprocess.run([sys.executable, '-c', code], env=environment, capture_output=True, text=True)
        if published.returncode: raise RuntimeError('publisher_failed')
        report['outbox_dispatched'] = True; save()
        # Keep the real standalone publisher alive for later public control wakes,
        # including cleanup accepted after the execute task reached a terminal Run.
        publisher_code = ("import time; from app.celery_app import celery; celery.conf.task_default_queue="
            + repr(queue) + "; from app.outbox_publisher import publish_once\nwhile True:\n publish_once()\n time.sleep(.5)\n")
        publisher = subprocess.Popen([sys.executable, '-c', publisher_code], env=environment,
            stdout=workerlog, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 100
        history = []
        stop_requested = False
        reconcile_requested = False
        rotation_requested = False
        while time.monotonic() < deadline:
            row = clients['auditor'].call('GET', scope + '/runs/' + report['run_id'])
            status = row['status']
            if scenario == 'unknown' and status == 'submission_unknown' and not reconcile_requested:
                receipt = clients['operator'].call('POST', scope + '/runs/' + report['run_id'] + '/reconcile',
                    {'idempotency_key': str(uuid4()), 'reason': 'submission_unknown'}, 202)
                report['reconciliation_public_api'] = {'accepted': True, 'receipt': reconciliation_summary(receipt)}
                reconcile_requested = True
                save()
            if scenario == 'rotation' and not rotation_requested:
                with sessions() as db:
                    original_attempt = db.get(EngineAttempt, report['command_id'])
                    dispatched = bool(original_attempt and original_attempt.phase == 'submitted')
                    original_id = original_attempt.engine_job_id if original_attempt else None
                    connection = db.get(Connection, refs['target']['connection_id'])
                    old_reference, old_version = connection.writer_secret_reference_id, connection.writer_secret_version
                if dispatched:
                    before_rotation = clients['auditor'].call('GET', scope + '/runs/' + report['run_id'])
                    prepared = clients['operator'].call('POST', scope + '/connections/' + refs['target']['connection_id']
                        + '/credential-rotations', {'slot': 'execution_writer', 'credential': writer_secret}, 201)
                    rotation_path = scope + '/preparations/' + prepared['id']
                    for role, slot in (('data_checker', 'data'), ('security_checker', 'security')):
                        clients[role].call('POST', rotation_path + '/decisions', {'slot': slot, 'decision': 'approve'})
                    rotation_cap = clients['operator'].call('POST', rotation_path + '/capabilities', {}, 201)['capability']
                    rotated = clients['operator'].call('POST', rotation_path + '/commit',
                        {'capability': rotation_cap, 'idempotency_key': str(uuid4())}, 201)
                    with sessions() as db:
                        connection = db.get(Connection, refs['target']['connection_id'])
                        new_reference, new_version = connection.writer_secret_reference_id, connection.writer_secret_version
                        revoked = db.get(SecretReference, old_reference).revoked_at is not None
                    report['rotation_public_api'] = {'run_status_before_rotation': before_rotation['status'],
                        'original_engine_job_id': original_id, 'rotation_run_id': rotated['id'],
                        'preparation_id': prepared['id'], 'risk_level': prepared['risk_level'],
                        'old_reference_id': old_reference, 'old_version': old_version, 'old_revoked': revoked,
                        'new_reference_id': new_reference, 'new_version': new_version,
                        'inline_status': rotated['status'], 'slot': 'execution_writer'}
                    old_prepare = maker.call('GET', scope + '/preparations/' + preparation['id'])
                    report['rotation_public_api']['old_preparation_state'] = old_prepare['validation']['state']
                    fresh_candidate = maker.call('POST', scope + '/studio/candidates',
                        {'name': '轮转后新绑定真实验证', 'plan': plan}, 201)
                    fresh_version = maker.call('POST', scope + '/versions', {'candidate_id': fresh_candidate['id'],
                        'expected_revision': fresh_candidate['revision']}, 201)
                    fresh_prepare = maker.call('POST', scope + '/preparations',
                        {'version_id': fresh_version['id'], 'tool': 'pipeline.trial'}, 201)
                    fresh_writer = fresh_prepare['frozen']['execution_facts']['target']['execution_writer']
                    report['rotation_public_api']['fresh_preparation_reference_id'] = fresh_writer['secret_reference_id']
                    rotation_requested = True
                    save()
            if scenario == 'cancel' and not stop_requested:
                with sessions() as db:
                    original_attempt = db.get(EngineAttempt, report['command_id'])
                    dispatched = bool(original_attempt and original_attempt.phase == 'submitted')
                    original_id = original_attempt.engine_job_id if original_attempt else None
                if dispatched:
                    clients['operator'].call('POST', scope + '/runs/' + report['run_id'] + '/stop',
                        {'idempotency_key': str(uuid4()), 'reason': '真实受管L2原作业取消核对'}, 202)
                    stop_requested = True
                    report['stop_public_api'] = {'accepted': True, 'original_engine_job_id': original_id}
                    save()
            if not history or history[-1]['status'] != status:
                history.append({'status': status, 'at': datetime.now(timezone.utc).isoformat()})
                report['status_history'] = history; report['phase'] = status; save()
            if status in ('supervision_passed', 'denied', 'supervision_unavailable', 'blocked_target',
                          'terminated', 'final_metrics_pending'):
                break
            time.sleep(.25)
        # A timed-out worker is not proof that its engine or sink stopped. Close
        # managed admission, request stop publicly and retain an unconfirmed
        # namespace for diagnosis instead of treating docker removal as success.
        if status not in ('supervision_passed', 'denied', 'supervision_unavailable',
                          'blocked_target', 'terminated', 'final_metrics_pending'):
            clients['operator'].call('POST', scope + '/runs/' + report['run_id'] + '/stop',
                {'idempotency_key': str(uuid4()), 'reason': '真实受管L2有界等待超时后停止原作业'}, 202)
            from app.real_runtime import ingress_service
            ingress_service(sessions=sessions).close(report['command_id'], 'ingress_closed')
            report['timeout_stop_requested'] = True
            report['phase'] = 'bounded_wait_timeout'
            time.sleep(3)
        with sessions() as db:
            attempt = db.get(EngineAttempt, report['command_id'])
            supervision = db.get(RuntimeSupervision, report['command_id'])
            control = db.get(RunControl, report['command_id'])
            quality = db.get(QualityRun, report['run_id'])
            report['engine_job_id'] = attempt.engine_job_id if attempt else None
            report['attempt_phase'] = attempt.phase if attempt else None
            report['supervision'] = {'status': supervision.status, 'reasons': supervision.reasons,
                'current_metrics': supervision.current_metrics, 'publication_veto': supervision.publication_veto,
                'stop_request': supervision.stop_request} if supervision else None
            report['control'] = control.facts if control else None
            report['quality'] = quality.report if quality else None
            original_request = dict(attempt.request) if attempt else None
            routes = attempt.request.get('real_routes', {}) if attempt else {}
            report['object_identities'] = {name: route['object_identity'] for name, route in routes.items()}
            batches = list(db.scalars(select(LoadBatch)))
            report['load_batches'] = [{'phase': b.phase, 'label': b.label, 'receipt': b.receipt,
                                      'response_sha256': b.response_sha256} for b in batches]
        if original_request:
            from app.engine_adapter import SeaTunnelClient, job_name
            try:
                terminal_proof = SeaTunnelClient(original_request['engine_url'], timeout=1.5).observe(
                    report['engine_job_id'], job_name(report['command_id']))
                report['final_engine_observation'] = {k: terminal_proof[k] for k in
                    ('job_id', 'job_name', 'engine_state', 'engine_terminal')} if terminal_proof else None
            except Exception:
                report['final_engine_observation'] = None
        if routes:
            target = main.app.state.connector_policy.get('l2-target')
            with pymysql.connect(host=target['host'], port=target['port'], user=config['sink'][0]['username'],
                                  **{'pass' + 'word': writer_secret}, database=target['database'],
                                  connect_timeout=3, read_timeout=3) as connection:
                with connection.cursor() as cursor:
                    table = routes['valid']['object_identity']['table']
                    cursor.execute('SELECT id,full_name FROM `' + table + '` ORDER BY id')
                    actual = [list(r) for r in cursor.fetchall()]
                    source = main.app.state.connector_policy.get('l2-source')
                    with pymysql.connect(host=source['host'], port=source['port'], user=source['username'],
                        **{'pass' + 'word': source_secret}, database=source['database']) as reader:
                        with reader.cursor() as source_cursor:
                            source_cursor.execute('SELECT id,full_name FROM customers ORDER BY id')
                            expected = [list(r) for r in source_cursor.fetchall()]
                    report['queried_rows'] = len(actual)
                    if scenario == 'quality':
                        expected_valid = [[r[0], r[1].strip() if r[1] is not None else None]
                            for r in expected if r[0] != 2 and r[1] is not None]
                        cursor.execute('SELECT record_key,error_codes,field_names,record_sha256,run_id,contract_sha256 '
                            'FROM `' + routes['rejected']['object_identity']['table'] + '`')
                        errors = cursor.fetchall()
                        report['quality_actual_routes'] = {'source_rows': len(expected), 'valid_rows': len(actual),
                            'rejected_rows': len(errors), 'cleaned_valid_equal': actual == expected_valid,
                            'error_codes': sorted({code for row in errors for code in json.loads(row[1])}),
                            'error_scope_matches': all(row[4] == report['run_id'] for row in errors)}
                        report['data_equal'] = actual == expected_valid
                    else:
                        report['data_equal'] = actual == expected
                    report['result_sha256'] = hashlib.sha256(json.dumps(actual, ensure_ascii=False).encode()).hexdigest()
        if scenario == 'publish' and report['phase'] == 'supervision_passed' and report.get('data_equal') is True:
            from app.doris_objects import DorisObjects
            objects = DorisObjects(main.app.state.connector_policy.get('l2-target'), target_secret)
            shadow_table = routes['valid']['object_identity']['table']
            before = objects.observe_publication(target_table, shadow_table)
            publication = clients['operator'].call('POST', scope + '/runs/' + report['run_id'] + '/publish',
                {'idempotency_key': str(uuid4())}, 202)
            report['publication_public_api'] = {'accepted': True, 'intent_id': publication['intent_id'],
                'phase_before_publish': report['phase'], 'before': before}
            save()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                with sessions() as db:
                    intent = db.get(PublicationIntent, publication['intent_id'])
                    snapshot = db.scalar(select(PublishedSnapshot).where(PublishedSnapshot.intent_id == intent.id))
                    report['publication_public_api']['intent_phase'] = intent.phase
                    report['publication_public_api']['intent_reason'] = intent.reason
                    if snapshot:
                        report['publication_public_api']['snapshot'] = {'id': snapshot.id,
                            'confirmed_at': snapshot.confirmed_at.isoformat(), 'retain_until': snapshot.retain_until.isoformat(),
                            'retention_seconds': (snapshot.retain_until - snapshot.confirmed_at).total_seconds(),
                            'identities': snapshot.identities}
                        break
                    if intent.phase in ('unknown', 'denied'):
                        break
                time.sleep(.25)
            after = objects.observe_publication(target_table, shadow_table)
            report['publication_public_api']['after'] = after
            identity_keys = ('physical_cluster_id', 'database', 'marker', 'partition_ids', 'schema', 'schema_sha256')
            swapped = (all(after['stable'][k] == before['shadow'][k] for k in identity_keys)
                and all(after['shadow'][k] == before['stable'][k] for k in identity_keys))
            with pymysql.connect(host=target['host'], port=target['port'], user=target['username'],
                **{'pass' + 'word': target_secret}, database=target['database']) as reader:
                with reader.cursor() as cursor:
                    cursor.execute('SELECT id,full_name FROM `' + target_table + '` ORDER BY id')
                    formal = [list(r) for r in cursor.fetchall()]
                    cursor.execute('SELECT COUNT(*) FROM `' + shadow_table + '`')
                    previous_rows = cursor.fetchone()[0]
            report['publication_public_api'].update(identities_swapped=swapped,
                formal_result_equal=formal == expected, retained_previous_rows=previous_rows)
            row = clients['auditor'].call('GET', scope + '/runs/' + report['run_id'])
            report['phase'] = row['status']
            retained = report['publication_public_api'].get('snapshot', {}).get('retention_seconds', 0)
            report['publication_path_accepted'] = (report['phase'] == 'published' and swapped
                and formal == expected and previous_rows == 0 and retained >= 7 * 86400)
        report['happy_path_accepted'] = scenario == 'happy' and report['phase'] == 'supervision_passed' and report.get('data_equal') is True
        split = report.get('quality_actual_routes') or {}
        report['quality_split_accepted'] = (scenario == 'quality' and split.get('source_rows') == 3
            and split.get('valid_rows') == 2 and split.get('rejected_rows') == 1
            and split.get('cleaned_valid_equal') is True and split.get('error_scope_matches') is True
            and (report.get('supervision') or {}).get('publication_veto') is True)
        termination = (report.get('control') or {}).get('termination') or {}
        report['unknown_path_accepted'] = (scenario == 'unknown' and report['phase'] == 'supervision_passed'
            and report.get('data_equal') is True and any(h['status'] == 'submission_unknown' for h in history))
        report['cancel_path_accepted'] = (scenario == 'cancel' and stop_requested
            and termination.get('engine_state') == 'CANCELED'
            and termination.get('engine_job_id') == report.get('engine_job_id')
            and (report.get('supervision') or {}).get('publication_veto') is True)
        report['reconciliation_final'] = [reconciliation_summary(item) for item in
            (row.get('execution') or {}).get('reconciliation') or []]
        with sessions() as db:
            report['ownership_retained'] = db.scalar(select(TargetOwnership).where(
                TargetOwnership.command_id == report['command_id'])) is not None
        if scenario == 'rotation' and rotation_requested:
            binding = accepted['managed_command']['payload']['resources']['target']['execution_writer']
            with sessions() as db:
                original = db.get(ManagedCommand, report['command_id'])
                unchanged = original.envelope['payload']['resources']['target']['execution_writer'] == binding
            report['rotation_public_api']['original_signed_writer_unchanged'] = unchanged
            denial = clients['operator'].call('POST', scope + '/runs/' + report['run_id'] + '/cleanup',
                {'idempotency_key': str(uuid4()), 'reason': '原绑定已撤销禁止清理'}, 409)
            report['rotation_public_api']['cleanup_denied'] = isinstance(denial.get('detail'), str)
            # Read persisted closure after the public denial; do not infer it from
            # veto, engine termination, or external_sink.admission_closed.
            from app.real_ingress import IngressBinding
            with sessions() as db:
                ingress = db.get(IngressBinding, report['command_id'])
                report['original_route_commands'] = {name: route['command_id'] for name, route in routes.items()}
                if ingress:
                    facts = ingress.facts
                    closed_routes = {}
                    for name, route_command in facts.get('route_commands', {}).items():
                        load = db.get(LoadRun, route_command)
                        if load:
                            closed_routes[name] = {'command_id': load.command_id,
                                'run_id': load.binding.get('run_id'), 'closed': load.closed,
                                'close_id': load.close_id}
                    report['ingress_closure'] = {'command_id': ingress.command_id,
                        'run_id': facts.get('run_id'), 'engine_job_id': facts.get('engine_job_id'),
                        'closed': ingress.closed, 'route_commands': facts.get('route_commands'),
                        'routes': closed_routes}
                batches = list(db.scalars(select(LoadBatch).where(
                    LoadBatch.command_id.in_(report['original_route_commands'].values()))))
                report['load_batches'] = [{'phase': b.phase, 'label': b.label, 'receipt': b.receipt,
                    'response_sha256': b.response_sha256} for b in batches]
                report['ownership_retained'] = db.scalar(select(TargetOwnership).where(
                    TargetOwnership.command_id == report['command_id'])) is not None
            report.update(rotation_acceptance(report))
        if scenario == 'cancel' and report['cancel_path_accepted']:
            clients['operator'].call('POST', scope + '/runs/' + report['run_id'] + '/cleanup',
                {'idempotency_key': str(uuid4()), 'reason': '真实受管L2原作业终止后的两影子清理'}, 202)
            cleanup_deadline = time.monotonic() + 35
            while time.monotonic() < cleanup_deadline:
                cleaned = clients['auditor'].call('GET', scope + '/runs/' + report['run_id'])
                if cleaned['status'] == 'cleaned':
                    break
                time.sleep(.5)
            with sessions() as db:
                after_control = db.get(RunControl, report['command_id'])
                report['cleanup_public_api'] = {'run_status': cleaned['status'],
                    'ownership_released': db.scalar(select(TargetOwnership).where(
                        TargetOwnership.command_id == report['command_id'])) is None,
                    'cleanup': after_control.facts.get('cleanup') if after_control else None}
            with pymysql.connect(host=target['host'], port=target['port'], user=config['sink'][0]['username'],
                    **{'pass' + 'word': writer_secret}, database=target['database'], connect_timeout=3) as connection:
                with connection.cursor() as cursor:
                    cursor.execute('SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s '
                        'AND TABLE_NAME IN (%s,%s,%s)', (target['database'],
                        routes['valid']['object_identity']['table'], routes['rejected']['object_identity']['table'],
                        target['tables'][0]))
                    remaining = {row[0] for row in cursor.fetchall()}
            report['cleanup_public_api']['two_original_shadows_absent'] = not any(
                route['object_identity']['table'] in remaining for route in routes.values())
            report['cleanup_public_api']['schema_placeholder_preserved'] = target['tables'][0] in remaining
            report['cleanup_path_accepted'] = (cleaned['status'] == 'cleaned'
                and report['cleanup_public_api']['ownership_released']
                and report['cleanup_public_api']['two_original_shadows_absent']
                and report['cleanup_public_api']['schema_placeholder_preserved'])
        # This single scenario does not establish cancellation, unknown recovery
        # or cleanup. The parent accepts the full issue only after those cases.
        report['managed_l2_accepted'] = False
        report['published'] = report['phase'] == 'published'
        report['postgresql_schema_retained'] = True
        save()
        # No plaintext is allowed in the exported safe report or worker log.
        serialized = output.read_text(encoding='utf-8')
        workerlog.flush()
        if any(secret and secret in serialized for secret in all_secrets): raise RuntimeError('report_secret_detected')
        return 0 if any(report.get(k) for k in ('happy_path_accepted', 'cancel_path_accepted', 'unknown_path_accepted', 'quality_split_accepted', 'rotation_path_accepted', 'publication_path_accepted')) else 1
    except Exception as error:
        report['failed_phase'] = report['phase']; report['phase'] = 'runner_failed'
        report['error_type'] = type(error).__name__
        # Client assertions can contain schema/gate findings but never credentials;
        # retain them privately, export only a finite error class.
        (PRIVATE / 'failure.txt').write_text(str(error), encoding='utf-8')
        save()
        return 1
    finally:
        for process in (publisher, worker):
            if process and process.poll() is None:
                process.terminate()
                try: process.wait(timeout=10)
                except subprocess.TimeoutExpired: process.kill(); process.wait()
        if server: server.should_exit = True
        if thread: thread.join(timeout=5)
        isolated.dispose()


if __name__ == '__main__':
    raise SystemExit(run())
