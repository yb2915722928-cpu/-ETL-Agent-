"""Terminal diagnostics on real PostgreSQL; explicit engine/load seams, NOT L2."""
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
from sqlalchemy import func, literal, delete, select
from verify_quality_fixtures import RowAdapter
from verify_real_runtime_fixtures import database, seed, Objects, Engine, digest
from app.command_signing import COMMAND_FORMAT, sign_envelope
from app.execution import ExecutionRun, ManagedCommand, authorization_bindings
from app.execution_runtime import EngineAttempt, TargetOwnership
from app.managed_load import LoadBatch, ManagedLoad
from app.models import Membership, SecretReference
from app.preparations import ApprovalDecision, Preparation
from app.quality_runtime import QualityRun, QualityRuntime
from app.real_contract import real_execution_contract
from app.real_control import advance_real_control
from app.real_finalization import finalize_real_quality_diagnostics
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


def approve_rules(sessions, signer, cid):
    with sessions.begin() as db:
        command = db.get(ManagedCommand, cid)
        run = db.get(ExecutionRun, command.run_id)
        prep = db.get(Preparation, run.preparation_id)
        frozen = deepcopy(prep.frozen)
        plan = frozen['plan']
        plan['quality'] = [{'kind': 'numeric_range', 'field': 'id', 'minimum': 0, 'maximum': 100}]
        # A single rejected record gets two codes, but only one error-row write.
        plan['filters'] = [{'field': 'id', 'equals': 1000}]
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
        command.envelope = sign_envelope(signer, {'format': COMMAND_FORMAT, 'command_id': cid,
            'run_id': run.id, 'accepted_at': run.accepted_at.isoformat(), **authorization_bindings(prep, run.subject)})


def controls(sessions, cid):
    with sessions() as db:
        command = db.get(ManagedCommand, cid)
        run, supervision, control = db.get(ExecutionRun, command.run_id), db.get(RuntimeSupervision, cid), db.get(RunControl, cid)
        owner = db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid))
        return deepcopy({'run': [run.status, run.engine_job_id],
            'supervision': [supervision.status, supervision.publication_veto, supervision.stop_request,
                supervision.reasons, supervision.last_trusted, supervision.current_metrics],
            'control': [control.facts, control.wake_at], 'owner': [owner.command_id, owner.identity]})


def verify(sessions, signer):
    checks = []
    receipt = json.dumps({'schema': 'managed-network-receipt/v1', 'resource_state': 'kept',
        'verification_phase': 'isolated_network_observed', 'http_connector_sha256': HTTP_CONNECTOR['sha256'],
        'engine': {'ip': '192.0.2.4', 'rest_port': 8080, 'file_encoding': 'UTF-8'},
        'gateway': {'ip': '192.0.2.5', 'ingress_port': 8080}, 'fixture_only': True}).encode()
    with TemporaryDirectory(prefix='quality-diagnostic-fixture-') as directory:
        path = Path(directory) / 'network-receipt.json'
        path.write_bytes(receipt)
        with patch.dict(os.environ, {'REAL_NETWORK_RECEIPT_FILE': str(path)}), \
             patch('app.preparations.current_execution_facts', side_effect=lambda db, policy, version:
                   deepcopy(version.frozen['execution_facts'])), patch('app.real_runtime._external_authorization'):
            def fixture(ids=(1, 2, 1000), *, count='3', state='FINISHED'):
                cid, policy, provider, manifest = seed(sessions, signer, receipt)
                approve_rules(sessions, signer, cid)
                objects = Objects(sessions)
                class ObservedEngine(Engine):
                    def observe(self, identifier, name):
                        assert self.original['engine_job_id'] == identifier and self.original['engine_job_name'] == name
                        return {'job_id': identifier, 'job_name': name, 'engine_state': state,
                            'metrics': {'SourceReceivedCount': count} if count is not None else {}}
                engine = ObservedEngine(sessions, cid)
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
                    for value in ids:
                        assert ingress.observe(cid)['complete']
                        assert ingress.post(cid, _token(cid), json.dumps({'id': value}).encode())['accepted']
                with patch('app.real_control._now', return_value=max(datetime.now(timezone.utc), sample_time + timedelta(seconds=2))):
                    advance_real_control(cid, sessions=sessions, ingress_factory=lambda original: ingress,
                        client_factory=lambda url: engine)
                with sessions() as db:
                    command = db.get(ManagedCommand, cid)
                    return SimpleNamespace(cid=cid, run_id=command.run_id, ingress=ingress,
                        request=deepcopy(db.get(EngineAttempt, cid).request))

            def diagnose(f):
                before = controls(sessions, f.cid)
                def forbidden(*args, **kwargs):
                    raise AssertionError('diagnostics must use no external authorization, secret or object adapter')
                with patch('app.real_finalization._authorized', side_effect=forbidden), \
                     patch('app.real_finalization.ResourcePolicy', side_effect=forbidden), \
                     patch('app.real_finalization.SecretProvider', side_effect=forbidden), \
                     patch('app.real_finalization.DorisObjects', side_effect=forbidden):
                    report = finalize_real_quality_diagnostics(f.cid, sessions=sessions)
                assert controls(sessions, f.cid) == before
                return report

            f = fixture()
            with sessions() as db:
                assert db.get(RuntimeSupervision, f.cid).publication_veto
                assert db.get(RunControl, f.cid).facts['final_metrics']['status'] == 'known'
            report = diagnose(f)
            assert report['source_read'] == report['parsed'] == report['evaluated'] == 3
            assert report['valid'] == 2 and report['rejected'] == 1
            assert report['final_written'] == {'valid': 2, 'rejected': 1}
            assert report['rule_hits'] == {'id:FILTERED': 1, 'id:OUT_OF_RANGE': 1}
            assert report['publication_veto'] and 'real_supervision_veto' in report['reasons']
            assert diagnose(f) == report
            checks.append('reject_ratio_veto_retains_actual_2_valid_1_rejected_multicode_once_and_owner')

            f = fixture(state='CANCELED')
            report = diagnose(f)
            assert report['source_read'] == 3 and report['parsed'] == 3
            assert report['publication_veto'] and 'source_report_incomplete' in report['reasons']
            with sessions() as db:
                assert db.get(QualityRun, f.run_id).source_facts['complete'] is False
            checks.append('canceled_original_job_can_report_actual_counts_without_claiming_complete_source')

            f = fixture(ids=(), count='0')
            report = diagnose(f)
            assert report['source_read'] == report['parsed'] == report['evaluated'] == 0
            assert report['final_written'] == {'valid': 0, 'rejected': 0} and report['publication_veto']
            checks.append('trusted_empty_final_source_is_real_zero_not_unknown')

            for mode in ('missing_source', 'pending_final', 'current_mismatch', 'invalid_sample_timestamp'):
                f = fixture(count=None if mode == 'missing_source' else '3')
                with sessions.begin() as db:
                    if mode == 'pending_final':
                        control = db.get(RunControl, f.cid)
                        control.facts = {**control.facts, 'final_metrics': {'status': 'pending'}}
                    if mode == 'current_mismatch':
                        row = db.get(RuntimeSupervision, f.cid)
                        row.current_metrics = {**row.current_metrics, 'source_read': 999}
                    if mode == 'invalid_sample_timestamp':
                        row, control = db.get(RuntimeSupervision, f.cid), db.get(RunControl, f.cid)
                        sample = {**row.last_trusted, 'observed_at': 'invalid'}
                        row.last_trusted = sample
                        control.facts = {**control.facts, 'final_metrics': {
                            **control.facts['final_metrics'], 'sample': sample}}
                report = diagnose(f)
                assert report['source_read'] is None and report['parsed'] == 3 and report['publication_veto']
                with sessions() as db:
                    assert db.get(QualityRun, f.run_id).source_facts['complete'] is False
            checks.append('missing_pending_or_mismatched_original_source_stays_null_with_actual_parsed_count')

            f = fixture()
            with sessions.begin() as db:
                route = f.request['real_routes']['rejected']['command_id']
                batch = db.scalar(select(LoadBatch).where(LoadBatch.command_id == route))
                batch.response_sha256 = None
            report = diagnose(f)
            assert report['final_written'] == {'valid': 2, 'rejected': 0} and report['publication_veto']
            assert 'real_original_receipt_unverified' in report['reasons']
            checks.append('unverifiable_original_receipt_is_not_counted_as_actual_written')

            f = fixture()
            with sessions.begin() as db:
                db.execute(delete(Membership).where(Membership.tenant_id == 'tenant'))
                ref = db.get(SecretReference, f.request['real_routes']['valid']['writer_secret_reference_id'])
                ref.revoked_at = datetime.now(timezone.utc)
            assert diagnose(f)['source_read'] == 3
            checks.append('revoked_current_role_or_original_writer_does_not_block_readonly_diagnostics')

            # Restore fixture role memberships for the remaining independent runs.
            with sessions.begin() as db:
                for role, subject in (('maker', 'maker'), ('operator', 'operator'),
                                      ('data_checker', 'data_checker'), ('security_checker', 'security_checker')):
                    db.add(Membership(tenant_id='tenant', project_id='project', account_id=subject, roles=[role]))

            for invalid in ('running', 'open_ingress', 'wrong_terminal_id', 'wrong_attempt_hash', 'ingress_busy'):
                f = fixture(state='RUNNING' if invalid == 'running' else 'FINISHED')
                with sessions.begin() as db:
                    if invalid == 'open_ingress':
                        db.get(IngressBinding, f.cid).closed = False
                    elif invalid == 'wrong_terminal_id':
                        control = db.get(RunControl, f.cid)
                        control.facts = {**control.facts, 'termination': {
                            **control.facts['termination'], 'engine_job_id': '999'}}
                    elif invalid == 'wrong_attempt_hash':
                        db.get(EngineAttempt, f.cid).request_sha256 = '0' * 64
                if invalid == 'ingress_busy':
                    with f.ingress._serial(f.cid):
                        assert diagnose(f) is None
                else:
                    assert diagnose(f) is None
                with sessions() as db:
                    assert not db.get(QualityRun, f.run_id).finalized
            checks.append('running_open_busy_or_unbound_original_terminal_never_finalizes_report')

            f = fixture(ids=(1, 2), count='2')
            with sessions.begin() as db:
                quality = db.get(QualityRun, f.run_id)
                quality.source_facts = {'source_read': 2, 'parsed': 2, 'complete': True}
                links = deepcopy(db.get(IngressBinding, f.cid).links)
            original = QualityRuntime(sessions).report(f.run_id, links)
            assert not original['publication_veto']
            assert diagnose(f) == original
            checks.append('already_finalized_success_report_is_never_replaced_by_diagnostics')
    return checks


if __name__ == '__main__':
    with database() as (sessions, signer):
        passed = verify(sessions, signer)
    print(json.dumps({'level': 'postgresql_real_quality_diagnostics_fixture', 'real_l2_qualified': False,
        'postgresql_random_schema': True, 'passed': passed}))
