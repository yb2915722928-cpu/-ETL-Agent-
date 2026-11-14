"""Random-schema PostgreSQL finalization fixtures, NOT actual network/Doris L2.

Actual signatures, current authorization, ingress, quality and load journals run;
Doris metadata/source observations and network qualification are explicit seams.
No deployment migrations, publication, cleanup or owner release occur.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from sqlalchemy import func, literal, delete, select, text
from verify_quality_fixtures import RowAdapter
from verify_real_runtime_fixtures import database, seed, Objects, Engine, digest
from app.command_signing import COMMAND_FORMAT, sign_envelope
from app.execution import ExecutionRun, ManagedCommand, authorization_bindings
from app.execution_runtime import EngineAttempt, TargetOwnership
from app.managed_load import LoadBatch, LoadRun, ManagedLoad
from app.models import EvidenceEvent, Membership, SecretReference, Tenant
from app.preparations import ApprovalDecision, Preparation
from app.quality_runtime import QualityRun
from app.real_contract import real_execution_contract
from app.real_control import advance_real_control
from app.real_finalization import finalize_real
from app.real_ingress import IngressBinding, IngressService
from app.real_job import HTTP_CONNECTOR
from app.real_resources import RealResourceResolver
from app.real_runtime import _token, submit_real
from app.run_control import RunControl
from app.supervision_runtime import RuntimeSupervision
from app.versions import PipelineVersion


class _SqlClock:
    """Freeze one SQL clock while delegating aggregates to real SQLAlchemy."""
    def __init__(self, observed_at):
        self.observed_at = observed_at

    def clock_timestamp(self):
        return literal(self.observed_at)

    def __getattr__(self, name):
        return getattr(func, name)


class FinishedEngine(Engine):
    def observe(self, identifier, name):
        self.gets += 1
        assert self.original['engine_job_id'] == identifier and self.original['engine_job_name'] == name
        return {'job_id': identifier, 'job_name': name, 'engine_state': 'FINISHED',
                'metrics': {'SourceReceivedCount': '5'}}


def quality_plan(sessions, signer, command_id):
    """Authorize a five-record source with four valid records and one reject."""
    with sessions.begin() as db:
        command = db.get(ManagedCommand, command_id)
        run = db.get(ExecutionRun, command.run_id)
        prep = db.get(Preparation, run.preparation_id)
        frozen = deepcopy(prep.frozen)
        plan = frozen['plan']
        plan['quality'] = [{'kind': 'numeric_range', 'field': 'id', 'minimum': 0, 'maximum': 100}]
        facts = frozen['execution_facts']
        facts['plan'] = deepcopy(plan)
        facts['real_execution_contract'] = real_execution_contract(plan, facts['source']['schema'], facts['target']['schema'])
        frozen['execution_sha256'] = digest(facts)
        prep.frozen, prep.frozen_sha256 = frozen, digest(frozen)
        version = db.get(PipelineVersion, prep.version_id)
        version.frozen = {'execution_facts': deepcopy(facts), 'normalized_plan': deepcopy(plan)}
        version.execution_sha256 = digest(facts)
        for approval in db.scalars(select(ApprovalDecision).where(ApprovalDecision.preparation_id == prep.id)):
            approval.frozen_sha256 = prep.frozen_sha256
        command.envelope = sign_envelope(signer, {'format': COMMAND_FORMAT, 'command_id': command.id,
            'run_id': run.id, 'accepted_at': run.accepted_at.isoformat(), **authorization_bindings(prep, run.subject)})


def verify(sessions, signer):
    passed = []
    receipt = json.dumps({'schema': 'managed-network-receipt/v1', 'resource_state': 'kept',
        'verification_phase': 'isolated_network_observed', 'http_connector_sha256': HTTP_CONNECTOR['sha256'],
        'engine': {'ip': '192.0.2.4', 'rest_port': 8080, 'file_encoding': 'UTF-8'},
        'gateway': {'ip': '192.0.2.5', 'ingress_port': 8080},
        'fixture_only': True}).encode()
    with TemporaryDirectory(prefix='real-finalization-fixture-') as directory:
        path = Path(directory) / 'network-receipt.json'
        path.write_bytes(receipt)
        with patch.dict(os.environ, {'REAL_NETWORK_RECEIPT_FILE': str(path)}), \
             patch('app.preparations.current_execution_facts', side_effect=lambda db, policy, version:
                   deepcopy(version.frozen['execution_facts'])), patch('app.real_runtime._external_authorization'):
            def fixture():
                cid, policy, provider, manifest = seed(sessions, signer, receipt)
                quality_plan(sessions, signer, cid)
                objects, engine = Objects(sessions), FinishedEngine(sessions, cid)
                assert submit_real(cid, sessions=sessions, policy=policy, secrets=provider, manifest=manifest,
                    client_factory=lambda url: engine, network_verifier=lambda entry, raw: raw == receipt,
                    objects_factory=objects.factory) == 'submitted'
                resolver = RealResourceResolver(sessions=sessions, policy=policy, secrets=provider, manifest=manifest)
                loads = ManagedLoad(RowAdapter(), sessions)
                ingress = IngressService(lambda facts: loads, resolver.authorize, sessions)
                # The real controller must commit the original job's measured
                # RUNNING sample before any POST. Only engine I/O is a seam.
                def running(identifier, name):
                    assert identifier == engine.original['engine_job_id'] and name == engine.original['engine_job_name']
                    return {'job_id': identifier, 'job_name': name, 'engine_state': 'RUNNING',
                            'metrics': {'SourceReceivedCount': '0'}}
                with patch.object(engine, 'observe', side_effect=running):
                    assert advance_real_control(cid, sessions=sessions, ingress_factory=lambda _: ingress,
                        client_factory=lambda _: engine) == 'submitted'
                with sessions() as db:
                    supervision = db.get(RuntimeSupervision, cid)
                    assert supervision.status == 'observing' and supervision.stop_request is None
                    sample_time = datetime.fromisoformat(supervision.last_trusted['observed_at'])
                admission_time = max(sample_time, ingress._binding(cid).last_complete_observed_at) + timedelta(milliseconds=1)
                assert 0 <= (admission_time - sample_time).total_seconds() <= .2
                # Freeze only the SQL admission clock for a short source burst;
                # all journal/trusted facts above came from product transactions.
                with (patch('app.real_ingress.func', _SqlClock(admission_time)),
                      patch('app.managed_load.func', _SqlClock(admission_time))):
                    for identifier in (1, 2, 3, 4, 1000):
                        assert ingress.observe(cid)['complete']
                        assert ingress.post(cid, _token(cid), json.dumps({'id': identifier}).encode())['accepted']
                with patch('app.real_control._now', return_value=max(datetime.now(timezone.utc), sample_time + timedelta(seconds=2))):
                    assert advance_real_control(cid, sessions=sessions, ingress_factory=lambda original: ingress,
                        client_factory=lambda url: engine) == 'engine_finished_awaiting_quality'
                with sessions() as db:
                    command = db.get(ManagedCommand, cid)
                    request = deepcopy(db.get(EngineAttempt, cid).request)
                    run_id = command.run_id
                    assert db.get(RuntimeSupervision, cid).last_trusted['source_read'] == 5
                inspection, during_inspection, during_observation = [], [None], [None]
                def inspect(target, object_run, command):
                    # A separate connection takes the tenant lock during actual
                    # inspection, proving no finalizer transaction holds it.
                    with sessions.begin() as db:
                        db.execute(text("SET LOCAL lock_timeout = '250ms'"))
                        db.execute(select(Tenant).where(Tenant.id == 'tenant').with_for_update())
                    inspection.append(object_run)
                    result = objects.inspect_shadow(target, object_run, command)
                    if during_inspection[0]:
                        callback, during_inspection[0] = during_inspection[0], None
                        callback()
                    return result
                def observed(handle):
                    sample = ingress.observe(handle)
                    if during_observation[0]:
                        callback, during_observation[0] = during_observation[0], None
                        callback()
                    return sample
                def finish(**overrides):
                    arguments = {'sessions': sessions, 'policy': policy, 'secrets': provider, 'manifest': manifest,
                        'ingress_factory': lambda original: SimpleNamespace(observe=observed),
                        'network_verifier': lambda entry, raw: raw == receipt}
                    arguments.update(overrides)
                    with patch('app.real_finalization.DorisObjects', side_effect=lambda resource, credential:
                               SimpleNamespace(inspect_shadow=inspect)):
                        return finalize_real(cid, **arguments)
                return SimpleNamespace(cid=cid, run_id=run_id, request=request, finish=finish,
                    objects=objects, inspection=inspection, during_inspection=during_inspection,
                    during_observation=during_observation)

            def veto(f, *, source_empty=True):
                assert f.finish() == 'engine_finished_awaiting_quality'
                with sessions() as db:
                    assert db.get(RuntimeSupervision, f.cid).publication_veto
                    assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == f.cid)) is not None
                    assert not db.get(RunControl, f.cid).facts.get('load_quiescence')
                    if source_empty:
                        assert db.get(QualityRun, f.run_id).source_facts == {}

            f = fixture()
            assert f.finish() == 'supervision_passed'
            with sessions() as db:
                quality, control = db.get(QualityRun, f.run_id), db.get(RunControl, f.cid)
                assert quality.source_facts == {'source_read': 5, 'parsed': 5, 'complete': True}
                assert quality.finalized and not quality.report['publication_veto']
                assert quality.report['final_written'] == {'valid': 4, 'rejected': 1}
                assert not db.get(RuntimeSupervision, f.cid).publication_veto and control.wake_at is None
                assert control.facts['real_finalization']['publication_eligible'] is True
                assert control.facts['real_finalization']['published'] is False
                assert control.facts['load_quiescence'] == {'complete': True, 'admission_closed': True,
                    'load_quiescent': True, 'external_route_verified': True, 'object_identity_verified': True,
                    'engine_job_id': f.request['engine_job_id'], 'route_command_ids': {
                        name: route['command_id'] for name, route in f.request['real_routes'].items()}}
                assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == f.cid)) is not None
            assert len(f.inspection) == 2 and f.finish() == 'supervision_passed' and len(f.inspection) == 2
            passed.append('real_pg_quality_both_route_original_journals_clear_only_eligibility_owner_retained')
            with sessions.begin() as db:
                db.get(EngineAttempt, f.cid).created_at = datetime.now(timezone.utc) - timedelta(hours=2)
            def unexpected_io(*args):
                raise AssertionError('completed duplicate delivery must not observe external systems')
            assert advance_real_control(f.cid, sessions=sessions, ingress_factory=unexpected_io,
                                        client_factory=unexpected_io) == 'supervision_passed'
            with sessions() as db:
                supervision, control = db.get(RuntimeSupervision, f.cid), db.get(RunControl, f.cid)
                assert not supervision.stop_request and not supervision.publication_veto and control.wake_at is None
            passed.append('duplicate_completed_delivery_keeps_final_counters_and_does_not_accrue_duration_or_reopen_ingress')

            f = fixture()
            route_id = f.request['real_routes']['rejected']['command_id']
            with sessions.begin() as db:
                db.execute(delete(LoadBatch).where(LoadBatch.command_id == route_id))
                db.execute(delete(LoadRun).where(LoadRun.command_id == route_id))
            veto(f)
            passed.append('missing_second_route_does_not_create_source_report_or_quiescence')

            f = fixture()
            with sessions.begin() as db:
                batch = db.scalar(select(LoadBatch).where(LoadBatch.command_id == f.request['real_routes']['valid']['command_id']))
                batch.response_sha256 = None
            veto(f)
            passed.append('parsed_success_without_original_response_digest_remains_vetoed')

            for phase in ('before', 'during'):
                f = fixture()
                reference = f.request['real_routes']['valid']['writer_secret_reference_id']
                def revoke():
                    with sessions.begin() as db:
                        db.get(SecretReference, reference).revoked_at = datetime.now(timezone.utc)
                if phase == 'before':
                    revoke()
                else:
                    f.during_inspection[0] = revoke
                veto(f, source_empty=phase == 'before')
            passed.append('revoked_original_writer_before_or_during_read_window_cannot_clear_veto')

            f = fixture()
            f.objects.identities[f.run_id]['partition_ids'] = ['replacement-partition']
            veto(f)
            passed.append('actual_current_object_identity_replacement_vetoes_before_source_progress')

            f = fixture()
            assert f.finish(network_verifier=None) == 'engine_finished_awaiting_quality'
            with sessions() as db:
                assert db.get(RuntimeSupervision, f.cid).publication_veto
                assert db.get(QualityRun, f.run_id).source_facts == {}
            assert not f.inspection
            passed.append('deployment_file_hash_and_passed_flags_never_replace_network_verifier')

            f = fixture()
            def change_original():
                with sessions.begin() as db:
                    attempt = db.get(EngineAttempt, f.cid)
                    attempt.request = {**attempt.request, 'engine_job_id': '999'}
                    attempt.request_sha256 = digest(attempt.request)
            f.during_observation[0] = change_original
            veto(f, source_empty=False)
            passed.append('rehashing_original_attempt_during_barrier_cannot_clear_for_another_job')

            f = fixture()
            with sessions.begin() as db:
                row = db.get(RuntimeSupervision, f.cid)
                row.current_metrics = {**row.current_metrics, 'source_read': 6}
            veto(f)
            passed.append('independent_source_current_and_original_final_sample_must_match')

            f = fixture()
            with sessions.begin() as db:
                db.get(QualityRun, f.run_id).source_facts = {'source_read': 6, 'parsed': 5, 'complete': False}
            veto(f, source_empty=False)
            with sessions() as db:
                quality = db.get(QualityRun, f.run_id)
                assert quality.finalized and quality.report['publication_veto']
                assert 'source_counter_reset' in quality.report['reasons']
                assert db.get(RunControl, f.cid).wake_at is not None
            passed.append('actual_quality_report_source_counter_reset_veto_never_clears_or_stops_retry_schedule')

            with sessions() as db:
                persisted = json.dumps(list(db.scalars(select(EvidenceEvent.facts))) +
                    list(db.scalars(select(RunControl.facts))), default=str)
                assert 'synthetic-only-' not in persisted and 'x-etl-ingress-token' not in persisted
    return passed


if __name__ == '__main__':
    with database() as (sessions, signer):
        checks = verify(sessions, signer)
    print(json.dumps({'level': 'postgresql_real_finalization_fixture', 'postgresql_random_schema': True,
                      'real_l2_qualified': False, 'passed': checks}, ensure_ascii=False))
