"""Real PostgreSQL isolated-schema recovery fixtures; protocol peer is a mock.

No deployment secret file is opened, no real engine job is submitted, no L2
qualification is claimed. Run in the API image with backend/scripts mounted:
python /tests/verify_reconciliation_fixtures.py --postgres
"""
import asyncio
import base64
import json
import os
import sys
import threading
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Annotated
from unittest.mock import patch

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

from app import execution_runtime as runtime
from app import reconciliation as recovery
from app.command_signing import COMMAND_FORMAT, sign_envelope
from app.db import Base
from app.engine_adapter import job_name
from app.evidence_signing import EvidenceSigner
from app.execution import ExecutionRun, ManagedCommand, authorization_bindings
from app.models import Account, EvidenceEvent, Membership, Project, Tenant
from app.preparations import Preparation
from app.run_control import RunControl
from app.studio import Candidate
from app.supervision_runtime import RuntimeSupervision
from app.versions import PipelineVersion


class Routes:
    def post(self, path, **kwargs):
        def capture(handler):
            self.handler = handler
            return handler
        return capture


class Peer(BaseHTTPRequestHandler):
    calls = []
    reply = {}
    before_get = None

    def log_message(self, *args):
        pass

    def do_GET(self):
        type(self).calls.append(('GET', self.path))
        if type(self).before_get:
            type(self).before_get()
        encoded = json.dumps(type(self).reply).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_POST(self):
        type(self).calls.append(('POST', self.path))
        self.send_error(405)


def run():
    name = 'reconciliation_fixture_' + uuid.uuid4().hex
    assert name.startswith('reconciliation_fixture_') and name.replace('_', '').isalnum()
    admin = create_engine(os.environ['DATABASE_URL'], hide_parameters=True)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA {name}'))
    isolated = create_engine(os.environ['DATABASE_URL'], hide_parameters=True,
        connect_args={'options': '-csearch_path=' + name})
    sessions = sessionmaker(isolated, expire_on_commit=False)
    peer = ThreadingHTTPServer(('127.0.0.1', 0), Peer)
    thread = threading.Thread(target=peer.serve_forever, daemon=True)
    thread.start()
    url = 'http://127.0.0.1:' + str(peer.server_port)
    seed = base64.b64encode(bytes(range(32))).decode()
    env = patch.dict(os.environ, {'EVIDENCE_SIGNING_KEY': seed})
    env.start()
    signer = EvidenceSigner()
    actors = {key: SimpleNamespace(id=key, tenant_id='tenant') for key in ('operator', 'maker')}
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(evidence_signer=signer)))
    routes = Routes()

    def scope(db, actor, tenant_id, project_id):
        if tenant_id != actor.tenant_id or not db.get(Membership, (project_id, actor.id)):
            raise HTTPException(403, 'fixture_scope_denied')

    recovery.register_reconciliation_routes(routes, object, object, scope)

    def accept(run_id, key='same', reason='operator_requested', actor='operator', tenant='tenant', project='project'):
        tasks = BackgroundTasks()
        with sessions.begin() as db:
            result = routes.handler(tenant, project, run_id,
                recovery.ReconcileInput(idempotency_key=key, reason=reason), request, tasks, db, actors[actor])
        return result, tasks

    def new_run(engine_id='123', with_attempt=True):
        run_id, command_id, preparation_id = [str(uuid.uuid4()) for _ in range(3)]
        now = datetime.now(timezone.utc)
        frozen = {'execution_sha256': 'a' * 64, 'bundle_sha256': 'b' * 64,
            'artifact_hashes': {'job': 'c' * 64}, 'execution_facts': {
                'source': {'resource_id': 'source', 'secret_reference_id': 'missing-original-reference', 'secret_version': 1},
                'target': {'resource_id': 'target'}},
            'authorized_operation': {'data_plane_mutation': True}, 'budget': {},
            'runtime_supervision_contract': {'format': 'fixture'}, 'policy_version': 'fixture',
            'compiler_versions': {}, 'plan': {}}
        with sessions.begin() as db:
            prep = Preparation(id=preparation_id, tenant_id='tenant', project_id='project',
                maker_id='maker', version_id='version', tool='pipeline.trial', environment='local',
                risk_level='P2', frozen=frozen, frozen_sha256=recovery.digest(frozen),
                created_at=now, expires_at=now + timedelta(minutes=5))
            db.add(prep)
            db.flush()
            run = ExecutionRun(id=run_id, tenant_id='tenant', project_id='project', preparation_id=prep.id,
                subject='operator', status='submission_unknown', engine_job_id=None, accepted_at=now)
            db.add(run)
            db.flush()
            payload = {'format': COMMAND_FORMAT, 'command_id': command_id, 'run_id': run_id,
                'accepted_at': now.isoformat(), **authorization_bindings(prep, 'operator')}
            command = ManagedCommand(id=command_id, tenant_id='tenant', project_id='project', run_id=run_id,
                envelope=sign_envelope(signer, payload), created_at=now)
            db.add(command)
            db.flush()
            if with_attempt:
                original = {'command_id': command_id, 'run_id': run_id, 'tenant_id': 'tenant',
                    'project_id': 'project', 'correlation_id': str(uuid.uuid4()), 'engine_url': url,
                    'adapter': 'real', **{k: payload[k] for k in ('tool', 'environment', 'budget',
                        'authorized_operation', 'runtime_supervision_contract', 'bundle_sha256', 'artifact_hashes')}}
                db.add(runtime.EngineAttempt(command_id=command_id, correlation_id=original['correlation_id'],
                    request_sha256=recovery.digest(original), request=original, phase='submission_unknown',
                    engine_job_id=engine_id, created_at=now, updated_at=now))
            db.add(runtime.TargetOwnership(target_key=recovery.digest(command_id), identity={'fixture': True},
                command_id=command_id, acquired_at=now))
            db.add(RuntimeSupervision(command_id=command_id, contract={}, publication_veto=True,
                stop_request={'id': 'original-stop', 'delivery_status': 'unknown'}, updated_at=now))
            db.add(RunControl(command_id=command_id, facts={'inflight': {'kind': 'stop', 'status': 'unknown'}},
                lease_owner='dead-worker', lease_token='expired-token', lease_expires_at=now - timedelta(days=1)))
        return run_id, command_id

    def counts():
        with sessions() as db:
            return [db.scalar(select(func.count()).select_from(model)) for model in
                (ExecutionRun, ManagedCommand, runtime.EngineAttempt, runtime.TargetOwnership)]

    def last(command_id):
        with sessions() as db:
            return recovery.reconciliation_view(db, command_id)[-1]

    passed = []
    try:
        with isolated.begin() as connection:
            Base.metadata.create_all(connection)
        with sessions.begin() as db:
            db.add(Tenant(id='tenant', slug='tenant', name='Fixture'))
            db.flush()
            db.add(Project(id='project', tenant_id='tenant', slug='project', name='Fixture'))
            for key in actors:
                db.add(Account(id=key, tenant_id='tenant', username=key, display_name=key,
                    **{'pass' + 'word_hash': 'unused'}))
            db.flush()
            for key in actors:
                db.add(Membership(project_id='project', account_id=key, tenant_id='tenant', roles=[key]))
            db.add(Candidate(id='candidate', tenant_id='tenant', project_id='project', owner_id='maker',
                name='Fixture', plan={}, assessment={}))
            db.flush()
            db.add(PipelineVersion(id='version', tenant_id='tenant', project_id='project', owner_id='maker',
                candidate_id='candidate', candidate_revision=1, name='Fixture', execution_sha256='a' * 64,
                bundle_sha256='b' * 64, frozen={}, artifacts={}, artifact_hashes={}))
        with patch.object(recovery, 'Session', sessions), patch.object(runtime, 'engine', isolated), \
                patch.dict(os.environ, {'COMMAND_PUBLIC_KEY': signer.public['public_key'],
                                       'REAL_ENGINE_URL': 'http://replacement.invalid'}):
            run_id, command_id = new_run()
            before = counts()
            # Exercise actual FastAPI body validation before any intent/evidence write.
            def fixture_db():
                with sessions.begin() as db:
                    yield db

            app = FastAPI()
            app.state.evidence_signer = signer
            recovery.register_reconciliation_routes(app, Annotated[object, Depends(fixture_db)],
                Annotated[object, Depends(lambda: actors['operator'])], scope)
            sentinel = 'fixture_request_secret_SENTINEL'
            with sessions() as db:
                event_count = db.scalar(select(func.count()).select_from(EvidenceEvent))
                record_count = db.scalar(select(func.count()).select_from(recovery.ReconciliationRecord))
                evidence_sequence = db.get(Tenant, 'tenant').evidence_seq
            async def rejected_request():
                body = json.dumps({'idempotency_key': 'invalid-free-text', 'reason': sentinel}).encode()
                messages = []

                async def receive():
                    return {'type': 'http.request', 'body': body, 'more_body': False}

                async def send(message):
                    messages.append(message)

                await app({'type': 'http', 'asgi': {'version': '3.0'}, 'http_version': '1.1',
                    'method': 'POST', 'scheme': 'http', 'path': f'/api/tenants/tenant/projects/project/runs/{run_id}/reconcile',
                    'query_string': b'', 'headers': [(b'content-type', b'application/json')],
                    'client': ('127.0.0.1', 1), 'server': ('fixture', 80), 'root_path': ''}, receive, send)
                assert next(item['status'] for item in messages if item['type'] == 'http.response.start') == 422

            asyncio.run(rejected_request())
            with sessions() as db:
                assert db.scalar(select(func.count()).select_from(EvidenceEvent)) == event_count
                assert db.scalar(select(func.count()).select_from(recovery.ReconciliationRecord)) == record_count
                assert db.get(Tenant, 'tenant').evidence_seq == evidence_sequence
                assert sentinel not in json.dumps([event.facts for event in db.scalars(select(EvidenceEvent))])
            assert not Peer.calls and counts() == before
            for code in ('submission_unknown', 'worker_lost', 'lease_expired', 'stop_unconfirmed', 'operator_requested'):
                assert recovery.ReconcileInput(idempotency_key='validate-only', reason=code).reason == code
            passed.append('free-text sentinel rejected by real HTTP 422 before any reconciliation/evidence write; only five reason codes accepted')
            accepted, tasks = accept(run_id)
            assert accepted['phase'] == 'accepted' and len(tasks.tasks) == 1 and not Peer.calls
            duplicate, _ = accept(run_id)
            assert duplicate == accepted
            assert accept(run_id, reason='submission_unknown')[0].status_code == 409
            assert accept(run_id, actor='maker')[0].status_code == 403
            assert accept(run_id, tenant='other')[0].status_code == 403
            assert accept(run_id, project='other')[0].status_code == 403
            assert accept('missing')[0].status_code == 404
            assert counts() == before
            passed.append('durable intent; exact replay; 409 conflict; current role and tenant/project boundaries')
            Peer.reply = {'jobId': '123', 'jobName': job_name(command_id), 'jobStatus': 'CANCELED',
                          'metrics': {'secret': 'do-not-persist'}}

            def durable_get():
                with sessions() as db:
                    row = db.get(recovery.ReconciliationRecord, accepted['record_id'])
                    assert row.phase == 'observing'
                    assert db.scalar(select(func.count()).select_from(EvidenceEvent).where(
                        EvidenceEvent.action == 'reconciliation.get_intent')) >= 1
            Peer.before_get = durable_get
            # Fault between durable read intent and GET result: restart may only GET.
            with patch.object(recovery.SeaTunnelClient, 'observe', side_effect=SystemExit('fixture crash')):
                try:
                    recovery.advance_reconciliation(command_id)
                except SystemExit:
                    pass
                else:
                    raise AssertionError('fault not exercised')
            assert last(command_id)['phase'] == 'observing'
            assert recovery.advance_reconciliation(command_id) == 'observed'
            result = last(command_id)
            assert result['result']['engine_terminal'] is True and result['result']['sink_quiescent'] is None
            assert result['result']['cleanup_authorized'] is False
            assert 'do-not-persist' not in json.dumps(result, default=str)
            assert accept(run_id)[0] == result
            assert recovery.advance_reconciliation(command_id) == 'idle'
            assert counts() == before
            passed.append('injected worker death resumes original GET; frozen URL wins over changed configuration; missing current secret reference allows read')
            with sessions() as db:
                control = db.get(RunControl, command_id)
                supervision = db.get(RuntimeSupervision, command_id)
                assert control.facts == {'inflight': {'kind': 'stop', 'status': 'unknown'}}
                assert supervision.publication_veto and supervision.stop_request['id'] == 'original-stop'
                assert control.lease_owner == 'dead-worker' and control.lease_token == 'expired-token'
                assert db.scalar(select(runtime.TargetOwnership).where(runtime.TargetOwnership.command_id == command_id))
            passed.append('engine CANCELED does not confirm sink quiescence/cleanup; stop, veto, ownership and expired lease remain intact')
            Peer.before_get = None
            for engine_id, with_attempt in [(None, True), (None, False)]:
                missing_run, missing_command = new_run(engine_id, with_attempt)
                call_count = len(Peer.calls)
                accept(missing_run)
                assert recovery.advance_reconciliation(missing_command) == 'unknown'
                assert last(missing_command)['basis'] == 'original_job_id_missing'
                assert len(Peer.calls) == call_count
            passed.append('missing original ID or Attempt is durable unknown and causes no network call')
            for reply in ({'jobId': '123'}, {'jobId': '999', 'jobName': job_name(command_id), 'jobStatus': 'CANCELED'},
                          {'jobId': '123', 'jobName': 'wrong', 'jobStatus': 'RUNNING'},
                          {'jobId': '123', 'jobName': job_name(command_id), 'jobStatus': 'NOT_A_STATE'}):
                Peer.reply = reply
                key = str(uuid.uuid4())
                accept(run_id, key)
                assert recovery.advance_reconciliation(command_id) == 'unknown'
                assert last(command_id)['basis'] == 'original_job_unconfirmed'
            passed.append('unknown echo, wrong ID, wrong name and invalid status cannot establish original job facts')
            # A deferred DB failure after successful GET rolls back only result.
            Peer.reply = {'jobId': '123', 'jobName': job_name(command_id), 'jobStatus': 'RUNNING'}
            accept(run_id, 'result-rollback')
            with isolated.begin() as connection:
                connection.execute(text("CREATE FUNCTION fail_result() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                    "IF NEW.action='reconciliation.observed' THEN RAISE EXCEPTION 'fixture result commit fault' "
                    "USING ERRCODE='40001'; END IF; RETURN NEW; END $$"))
                connection.execute(text('CREATE CONSTRAINT TRIGGER fail_result AFTER INSERT ON evidence_events '
                    'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_result()'))
            from sqlalchemy.exc import DBAPIError
            try:
                recovery.advance_reconciliation(command_id)
            except DBAPIError:
                pass
            else:
                raise AssertionError('deferred result failure not exercised')
            assert last(command_id)['phase'] == 'observing'
            with isolated.begin() as connection:
                connection.execute(text('DROP TRIGGER fail_result ON evidence_events'))
                connection.execute(text('DROP FUNCTION fail_result()'))
            assert recovery.advance_reconciliation(command_id) == 'observed'
            passed.append('real deferred PostgreSQL SQLSTATE40001 rolls back result; restart repeats only original GET')
            # A changed persisted ID cannot redirect an already signed intent.
            accept(run_id, 'changed-attempt')
            with sessions.begin() as db:
                db.get(runtime.EngineAttempt, command_id).engine_job_id = '999'
            call_count = len(Peer.calls)
            assert recovery.advance_reconciliation(command_id) == 'unknown'
            assert last(command_id)['basis'] == 'authorization_or_binding_unavailable'
            assert len(Peer.calls) == call_count
            with sessions.begin() as db:
                db.get(runtime.EngineAttempt, command_id).engine_job_id = '123'
            tampered, _ = accept(run_id, 'tampered-signature')
            with sessions.begin() as db:
                row = db.get(recovery.ReconciliationRecord, tampered['record_id'])
                envelope = json.loads(json.dumps(row.envelope))
                envelope['payload']['reason'] = 'worker_lost'
                row.envelope = envelope
            assert recovery.advance_reconciliation(command_id) == 'unknown'
            assert len(Peer.calls) == call_count
            passed.append('changed original ID and forged signed intent are durable unknown with no redirected GET')
            # Revocation after acceptance is enforced by the recovery worker too.
            accept(run_id, 'role-revoked')
            with sessions.begin() as db:
                db.get(Membership, ('project', 'operator')).roles = []
            call_count = len(Peer.calls)
            assert recovery.advance_reconciliation(command_id) == 'unknown'
            assert len(Peer.calls) == call_count
            assert accept(run_id)[0].status_code == 403
            passed.append('role revoked after acceptance blocks worker read and direct replay')
            assert all(method == 'GET' and path == '/job-info/123' for method, path in Peer.calls)
            assert counts() == [3, 3, 2, 3]
            passed.append('all external calls are original GET /job-info/123; no new Run/job/submission or ownership release')
        print(json.dumps({'passed': passed, 'postgresql_isolated_schema': True,
                          'protocol_peer': 'mock HTTP; control-plane evidence only',
                          'external_requests': Peer.calls, 'real_l2_claim': False}, ensure_ascii=False, indent=2))
    finally:
        Peer.before_get = None
        peer.shutdown()
        peer.server_close()
        env.stop()
        isolated.dispose()
        # name is fixture-generated and validated above, never a public schema.
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA {name} CASCADE'))
        admin.dispose()


if __name__ == '__main__':
    if '--postgres' not in sys.argv:
        raise SystemExit('Use --postgres with a reachable PostgreSQL DATABASE_URL; no SQLite fallback')
    run()
