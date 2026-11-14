"""PG product reserve/control/ingress integration with fake external I/O; NOT L2."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from sqlalchemy import func, literal, select
from unittest.mock import patch

from sqlalchemy.orm import sessionmaker
from verify_real_runtime_fixtures import database, seed, Objects, Engine
from verify_quality_fixtures import RowAdapter
from app.execution import ExecutionRun, ManagedCommand, Outbox
from uuid import uuid4
from app.execution_runtime import EngineAttempt
from app.managed_load import ManagedLoad
from app.models import Membership, SecretReference
from app.real_control import advance_real_control
from app.real_ingress import IngressBinding, IngressService
from app.real_job import HTTP_CONNECTOR
from app.real_runtime import submit_real, _token
from app.run_control import RunControl
from app.reconciliation import ReconciliationRecord
from app.publication import PublicationIntent
from app.supervision_runtime import RuntimeSupervision
from verify_real_control_fixtures import accepted_stop


class AdmissionClock:
    def __init__(self, instant):
        self.instant = instant

    def clock_timestamp(self):
        return literal(self.instant)

    def __getattr__(self, name):
        return getattr(func, name)


def verify(sessions, signer):
    receipt = json.dumps({'schema': 'managed-network-receipt/v1', 'resource_state': 'kept',
        'verification_phase': 'isolated_network_observed',
        'engine': {'ip': '192.0.2.4', 'rest_port': 8080, 'file_encoding': 'UTF-8'},
        'gateway': {'ip': '192.0.2.5', 'ingress_port': 8080},
        'http_connector_sha256': HTTP_CONNECTOR['sha256']}, sort_keys=True, separators=(',', ':')).encode()
    passed = []
    with TemporaryDirectory(prefix='startup-fixture-') as directory:
        path = Path(directory) / 'receipt.json'
        path.write_bytes(receipt)
        with patch.dict(os.environ, {'REAL_NETWORK_RECEIPT_FILE': str(path), 'REAL_DEPLOYMENT_MANIFEST_FILE': ''}), \
             patch('app.preparations.current_execution_facts', side_effect=lambda db, policy, version: deepcopy(version.frozen['execution_facts'])), \
             patch('app.real_runtime._external_authorization'):
            def fixture():
                cid, policy, provider, manifest = seed(sessions, signer, receipt)
                objects, engine = Objects(sessions), Engine(sessions, cid)
                engine.lost_reply = True
                def submit():
                    return submit_real(cid, sessions=sessions, policy=policy, secrets=provider, manifest=manifest,
                        client_factory=lambda _: engine, network_verifier=lambda entry, raw: raw == receipt,
                        objects_factory=objects.factory)
                assert submit() == 'submission_unknown' and engine.posts == 1
                with sessions() as db:
                    marker = db.get(RunControl, cid).facts.get('real_submission')
                    assert marker and marker.get('reserved_at'), 'product reserve must commit fixed startup clock before POST'
                    origin = marker['reserved_at']
                adapter = RowAdapter()
                loads = ManagedLoad(adapter, sessions)
                # External credential/transport seam only; signed control authorization is real.
                ingress = IngressService(lambda _: loads, lambda _: None, sessions)
                engine.visible, engine.gets, engine.stops = False, 0, 0
                def observe(identifier, name):
                    assert identifier == engine.original['engine_job_id'] and name == engine.original['engine_job_name']
                    engine.gets += 1
                    if not engine.visible:
                        return {'job_id': identifier, 'job_name': name, 'engine_state': 'PENDING', 'metrics': {}}
                    binding = ingress._binding(cid)
                    if binding.last_complete_observed_at:
                        with sessions() as db:
                            if db.get(RuntimeSupervision, cid).last_trusted is None:
                                assert not ingress._admission_window(binding), 'journal-only barrier opened admission before trusted commit'
                    return {'job_id': identifier, 'job_name': name, 'engine_state': 'RUNNING',
                        'metrics': {'SourceReceivedCount': '0'}}
                def stop(identifier):
                    assert identifier == engine.original['engine_job_id'] and ingress._binding(cid).closed
                    engine.stops += 1
                    return {'accepted': True}
                engine.observe, engine.stop = observe, stop
                def control(worker=sessions):
                    return advance_real_control(cid, sessions=worker, ingress_factory=lambda _: ingress,
                        client_factory=lambda _: engine)
                return cid, ingress, engine, adapter, submit, control, origin

            cid, ingress, engine, adapter, submit, control, origin = fixture()
            assert control() == 'submission_unknown' and engine.gets == 1
            with sessions() as db:
                due = db.get(RunControl, cid).wake_at
                assert db.get(RuntimeSupervision, cid).last_trusted is None
                assert db.get(IngressBinding, cid).last_complete_observed_at is None
            assert control(sessionmaker(sessions.kw['bind'], expire_on_commit=False)) == 'submission_unknown'
            assert engine.gets == 1 and engine.stops == adapter.calls == 0
            # Runtime redelivery only discovers original ID; no POST or origin renewal.
            assert submit() == 'submitted' and engine.posts == 1
            with sessions() as db:
                assert db.get(RunControl, cid).facts['real_submission']['reserved_at'] == origin
            engine.visible = True
            with patch('app.real_control._now', return_value=due):
                assert control() == 'submitted'
            with sessions() as db:
                sample_time = datetime.fromisoformat(db.get(RuntimeSupervision, cid).last_trusted['observed_at'])
            # Freeze only the admission clock to remove host scheduling delays;
            # the product observer/controller created every journal and trusted fact.
            admission_time = ingress._binding(cid).last_complete_observed_at + timedelta(milliseconds=1)
            assert (admission_time - sample_time).total_seconds() <= .2
            admission_clock = AdmissionClock(admission_time)
            with patch('app.real_ingress.func', admission_clock):
                assert ingress._admission_window(ingress._binding(cid))
                assert ingress.post(cid, _token(cid), b'{"id":1}')['accepted'] and adapter.calls == 1
            passed.append('actual_single_submit_lost_reply_echo_then_visible_cross_worker_no_write_before_trusted_commit')

            cid, ingress, engine, adapter, submit, control, origin = fixture()
            assert ingress.observe(cid)['complete']
            assert not ingress._admission_window(ingress._binding(cid)), 'journal without any trusted sample must deny POST'
            try:
                ingress.post(cid, _token(cid), b'{"id":1}')
            except Exception as error:
                assert getattr(error, 'code', None) == 'ingress_admission_timeout'
            else:
                raise AssertionError('untrusted journal allowed POST')
            assert adapter.calls == 0
            passed.append('journal_only_observation_cannot_admit_first_post')

            # Real publisher -> Celery task -> drive -> original-adapter routing.
            from app import execution_runtime, real_runtime, real_control, outbox_publisher
            from app.celery_app import execute
            cid, ingress, engine, adapter, submit, control, origin = fixture()
            assert control() == 'submission_unknown'
            with sessions.begin() as db:
                due = db.get(RunControl, cid).wake_at
                for other in db.scalars(select(RunControl).where(RunControl.command_id != cid)):
                    other.wake_at = None
                command = db.get(ManagedCommand, cid)
                db.add(Outbox(id=str(uuid4()), tenant_id=command.tenant_id, project_id=command.project_id,
                    command_id=cid, status='pending', attempts=0, created_at=datetime.now(timezone.utc)))
            retries = []
            class RetryFixture(Exception):
                pass
            def retry(**kwargs):
                retries.append(kwargs['countdown'])
                return RetryFixture()
            original_advance = real_control.advance_real_control
            def with_engine(command_id, **kwargs):
                return original_advance(command_id, client_factory=lambda _: engine, **kwargs)
            def delivery(*, args):
                assert args == [cid]
                try:
                    execute.run(cid)
                except RetryFixture:
                    pass
            with patch.object(execution_runtime, 'Session', sessions), \
                 patch.object(execution_runtime, 'engine', sessions.kw['bind']), \
                 patch.object(execution_runtime, '_submit', side_effect=lambda command_id: submit()), \
                 patch.object(real_runtime, 'ingress_service', return_value=ingress), \
                 patch.object(real_control, 'advance_real_control', side_effect=with_engine), \
                 patch.object(outbox_publisher, 'Session', sessions), \
                 patch.object(execute, 'apply_async', side_effect=delivery), \
                 patch.object(execute, 'retry', side_effect=retry):
                assert outbox_publisher.publish_once() == 1
            assert retries == [2], 'real startup Celery retry must fit two-second discovery cadence'
            with sessions() as db:
                assert db.get(RunControl, cid).wake_at == due, 'delivery must restore due over publisher fifteen-second reservation'
                assert db.get(RuntimeSupervision, cid).last_trusted is None
            assert engine.posts == 1 and adapter.calls == engine.stops == 0
            passed.append('publisher_celery_actual_drive_routing_preserves_two_second_pending_due_and_single_submit')

            # Repeated drive must hand established supervision state to control even
            # after current authorization is revoked; submission may never replay.
            for revocation in ('role', 'writer_secret'):
                cid, ingress, engine, adapter, submit, control, origin = fixture()
                engine.visible = True
                assert submit() == 'submitted'
                with sessions.begin() as db:
                    if revocation == 'role':
                        db.get(Membership, ('project', 'operator')).roles = ['auditor']
                    else:
                        reference = engine.original['real_routes']['valid']['writer_secret_reference_id']
                        db.get(SecretReference, reference).revoked_at = datetime.now(timezone.utc)
                try:
                    for state in ('submitted', 'stop_requested', 'stop_delivery_unknown', 'final_metrics_pending', 'terminated'):
                        with sessions.begin() as db:
                            command = db.get(ManagedCommand, cid)
                            db.get(ExecutionRun, command.run_id).status = state
                        def redelivery(command_id):
                            assert command_id == cid
                            result = submit()
                            assert result == state, 'revoked established attempt regressed to submission_unknown'
                            return result
                        with patch.object(execution_runtime, 'Session', sessions), \
                             patch.object(execution_runtime, 'engine', sessions.kw['bind']), \
                             patch.object(execution_runtime, '_submit', side_effect=redelivery), \
                             patch('app.run_control.advance_control', side_effect=lambda _: control()), \
                             patch('app.real_finalization.finalize_real_quality_diagnostics'):
                            execution_runtime.drive(cid)
                    assert engine.posts == 1 and engine.stops == 1 and ingress._binding(cid).closed
                    assert adapter.calls == 0
                finally:
                    with sessions.begin() as db:
                        db.get(Membership, ('project', 'operator')).roles = ['operator']
            passed.append('role_and_writer_secret_revoked_established_attempt_repeated_drive_preserves_state_single_submit_and_stop')

            # State preservation grants no authority to a corrupted original.
            for mutation in ('signature', 'attempt_digest', 'original_id'):
                cid, ingress, engine, adapter, submit, control, origin = fixture()
                assert submit() == 'submitted'
                before = engine.gets
                with sessions.begin() as db:
                    command = db.get(ManagedCommand, cid)
                    run = db.get(ExecutionRun, command.run_id)
                    run.status = 'final_metrics_pending'
                    db.get(Membership, ('project', 'operator')).roles = ['auditor']
                    if mutation == 'signature':
                        command.envelope = {**command.envelope, 'signature': 'invalid'}
                    elif mutation == 'attempt_digest':
                        db.get(EngineAttempt, cid).request_sha256 = '0' * 64
                    else:
                        db.get(EngineAttempt, cid).engine_job_id = '456'
                try:
                    assert submit() == 'submission_unknown'
                    assert engine.posts == 1 and engine.gets == before and engine.stops == adapter.calls == 0
                finally:
                    with sessions.begin() as db:
                        db.get(Membership, ('project', 'operator')).roles = ['operator']
            passed.append('revoked_corrupt_original_signature_digest_or_job_id_cannot_preserve_trust_or_perform_io')

            for trigger in ('operator_stop', 'role_revoked', 'duration', 'deadline'):
                cid, ingress, engine, adapter, submit, control, origin = fixture()
                assert control() == 'submission_unknown'
                if trigger == 'operator_stop':
                    accepted_stop(sessions, signer, cid)
                elif trigger == 'role_revoked':
                    with sessions.begin() as db:
                        db.get(Membership, ('project', 'operator')).roles = ['auditor']
                engine.visible = True
                instant = datetime.now(timezone.utc)
                if trigger == 'deadline':
                    instant = datetime.fromisoformat(origin) + timedelta(seconds=6)
                elif trigger == 'duration':
                    with sessions.begin() as db:
                        attempt = db.get(EngineAttempt, cid)
                        attempt.created_at = instant - timedelta(seconds=601)
                try:
                    with patch('app.real_control._now', return_value=instant):
                        state = control()
                        assert state == 'stop_requested', (trigger, state, engine.stops)
                    assert ingress._binding(cid).closed and engine.stops == 1 and adapter.calls == 0
                    control()
                    assert engine.stops == 1
                finally:
                    with sessions.begin() as db:
                        db.get(Membership, ('project', 'operator')).roles = ['operator']
            passed.append('pending_startup_stop_revocation_duration_and_fixed_six_second_deadline_preempt_cadence')

            cid, ingress, engine, adapter, submit, control, origin = fixture()
            engine.visible = True
            start = datetime.fromisoformat(origin)
            times = iter([start + timedelta(seconds=5), start + timedelta(seconds=6),
                          start + timedelta(seconds=6), start + timedelta(seconds=6)])
            with patch('app.real_control._now', side_effect=lambda db: next(times)):
                assert control() == 'stop_requested'
            assert ingress._binding(cid).closed and engine.stops == 1 and adapter.calls == 0
            passed.append('slow_first_source_get_cannot_cross_fixed_startup_deadline')

            cid, ingress, engine, adapter, submit, control, origin = fixture()
            with sessions.begin() as db:
                db.get(RunControl, cid).facts = {}
            assert control() == 'stop_requested' and engine.stops == 1
            assert ingress._binding(cid).closed and adapter.calls == 0
            passed.append('historical_missing_marker_keeps_original_fail_closed_behavior')
    return passed


if __name__ == '__main__':
    with database() as (sessions, signer):
        results = verify(sessions, signer)
    print(json.dumps({'level': 'postgresql_real_startup_fixture', 'real_l2_qualified': False, 'passed': results}))
