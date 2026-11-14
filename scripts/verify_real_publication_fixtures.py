"""Real PostgreSQL signed-P1 publication gates and first-swap fault fixtures.

Actual DB transactions, command signatures, independent approvals, journals and
quality report execute. Only external metadata/network/SeaTunnel/Doris I/O are
explicit fixture seams; this is control-plane evidence, never Doris L2.
"""
import asyncio
from typing import Annotated
from fastapi import FastAPI, Depends, HTTPException
from copy import deepcopy
from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from sqlalchemy import event, func, literal, select, text
from verify_quality_fixtures import RowAdapter
from verify_real_runtime_fixtures import database, seed, Objects, Engine, digest
from app.command_signing import COMMAND_FORMAT, sign_envelope
from app.execution import ExecutionRun, ManagedCommand, authorization_bindings
from app.execution_runtime import EngineAttempt, TargetOwnership
from app.managed_load import LoadBatch, ManagedLoad
from app.models import Account, Membership, SecretReference
from app.versions import PipelineVersion
from app.preparations import ApprovalDecision, Preparation
from app.publication import PublicationIntent, PublicationService, PublishedSnapshot
from app.quality_runtime import QualityRun
from app.reconciliation import ReconciliationRecord  # Register publisher table before fixture schema creation.
from app.real_control import advance_real_control
from app.real_finalization import finalize_real
from app.real_ingress import IngressDenied, IngressService
from app.real_job import HTTP_CONNECTOR
from app.connector_policy import fingerprint
from app.real_engine import real_runtime_pin
from app.real_publication import DorisPublicationAdapter
from app.real_publication import PUBLICATION_VERSION, check_publication_grants, publication_view
from app.real_resources import RealResourceResolver
from app.real_runtime import _token, submit_real
from app.run_control import RunControl
from app.supervision_runtime import RuntimeSupervision


class FinishedEngine(Engine):
    state, source_rows = 'FINISHED', 2

    def observe(self, identifier, name):
        assert self.original['engine_job_id'] == identifier and self.original['engine_job_name'] == name
        return {'job_id': identifier, 'job_name': name, 'engine_state': self.state,
                'metrics': {'SourceReceivedCount': str(self.source_rows)}}


class SwapAdapter:
    def __init__(self, before, sessions):
        self.before, self.current, self.sessions = deepcopy(before), deepcopy(before), sessions
        self.swaps, self.lose_response, self.fail_commit, self.authorized = 0, False, False, True
        self.commit_fault_armed = False

    def observe(self, cluster, database, target, shadow):
        return deepcopy(self.current)

    def swap(self, cluster, database, target, shadow):
        check_publication_grants([{'Roles': '', 'DatabasePrivs': 'internal.' + database + ': ' +
            ('Select_priv,Load_priv,Create_priv,Alter_priv' if self.authorized else 'Select_priv,Load_priv,Create_priv'),
            'WorkloadGroupPrivs': 'normal: Usage_priv'}], database, ['Alter_priv'])
        # A separate connection sees dispatch reservation BEFORE actual external SQL.
        with self.sessions() as db:
            assert db.scalar(select(PublicationIntent.phase).where(
                PublicationIntent.run_id == self.run_id)) == 'dispatching'
        self.swaps += 1
        self.current = {'stable': {**deepcopy(self.before['shadow']), 'table': target},
                        'shadow': {**deepcopy(self.before['stable']), 'table': shadow}}
        if self.fail_commit:
            self.commit_fault_armed = True
        if self.lose_response:
            raise OSError('fixture swap response lost')


def approve_publish(sessions, signer, cid):
    with sessions.begin() as db:
        command = db.get(ManagedCommand, cid)
        run = db.get(ExecutionRun, command.run_id)
        prep = db.get(Preparation, run.preparation_id)
        frozen = deepcopy(prep.frozen)
        schema = [{'name': 'id', 'type': 'int', 'nullable': False}]
        stable = {'physical_cluster_id': frozen['execution_facts']['target']['schema'].get('cluster', 'fixture-cluster'),
            'database': 'will-be-set', 'table': 'approved_table', 'marker': None,
            'partition_ids': ['stable-partition'], 'schema': schema, 'schema_sha256': digest(schema)}
        prep.tool, prep.risk_level = 'pipeline.publish', 'P1'
        frozen['tool'], frozen['risk_level'] = prep.tool, prep.risk_level
        # The resource database is returned separately by the seed policy.
        frozen['publication_contract'] = {'version': PUBLICATION_VERSION, 'mode': 'bounded_full',
            'stable_identity': stable, 'expected_schema_sha256': stable['schema_sha256'],
            'allow_empty': False, 'retain_days': 7}
        prep.frozen = frozen
        return run.id


def verify_mutation_boundary(receipt_path):
    """Actual adapter + actual DorisObjects._cursor, fake SQL connection only."""
    resource = {'kind': 'doris', 'physical_cluster_id': 'fixture-cluster', 'database': 'fixture_db',
        'tables': ['stable'], 'host': '192.0.2.2', 'port': 9030, 'username': 'writer',
        'lifecycle_privileges': ['Alter_priv']}
    pin = real_runtime_pin()
    deployment = {'runtime_overview': {'projectVersion': pin['engine']['version'],
        'gitCommitAbbrev': pin['engine']['git_commit_abbrev']},
        'connector_digests': {item['artifact']: item['sha1'] for item in pin['connectors']},
        'driver_digest': pin['driver']['sha1'], 'http_connector_sha256': HTTP_CONNECTOR['sha256'],
        'resources': {'target': {'resource_fingerprint': 'approved-resource',
            'writer_resource_fingerprint': fingerprint(resource), 'physical_cluster_id': 'fixture-cluster'}}}
    request = {'real_manifest_sha256': digest(deployment),
        'resources': {'target': {'resource_id': 'target', 'resource_fingerprint': 'approved-resource'}}}
    before = {'stable': {'table': 'stable', 'partition_ids': ['old']},
              'shadow': {'table': 'shadow_' + 'a' * 32, 'partition_ids': ['new']}}
    for failure in ('show_grants', 'ddl_connection', 'stable_identity', 'shadow_identity', 'none'):
        state = {'network': True, 'connections': 0, 'qualifications': 0, 'ddl': [], 'statements': []}
        class Cursor:
            description = [('Roles',), ('DatabasePrivs',)]
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def execute(self, statement):
                state['statements'].append(statement)
                if statement == 'SHOW GRANTS' and failure == 'show_grants':
                    state['network'] = False
                if statement.startswith('ALTER TABLE '):
                    state['ddl'].append(statement)
            def fetchall(self):
                return [('', 'internal.fixture_db: Select_priv,Load_priv,Create_priv,Alter_priv')]
        class Connection:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def cursor(self):
                return Cursor()
        def connect(**kwargs):
            state['connections'] += 1
            if state['connections'] == 2 and failure == 'ddl_connection':
                state['network'] = False
            return Connection()
        def network(entry, raw):
            state['qualifications'] += 1
            return state['network']
        def observe(target, shadow):
            observed = deepcopy(before)
            if state['connections'] >= 2 and failure in ('stable_identity', 'shadow_identity'):
                observed[failure.removesuffix('_identity')]['partition_ids'] = ['replaced']
            return observed
        adapter = DorisPublicationAdapter(resource, 'synthetic-only', request, deployment)
        with patch('app.doris_objects.pymysql.connect', side_effect=connect), \
             patch.object(adapter.objects, 'observe_publication', side_effect=observe), \
             patch('app.real_publication.verify_live_network', side_effect=network):
            assert adapter.observe('fixture-cluster', 'fixture_db', 'stable', 'shadow_' + 'a' * 32) == before
            if failure == 'none':
                adapter.swap('fixture-cluster', 'fixture_db', 'stable', 'shadow_' + 'a' * 32)
                assert len(state['ddl']) == 1
            else:
                try:
                    adapter.swap('fixture-cluster', 'fixture_db', 'stable', 'shadow_' + 'a' * 32)
                    raise AssertionError('network drift reached DDL')
                except ValueError as error:
                    assert str(error) == ('publication_before_identity_changed'
                        if failure in ('stable_identity', 'shadow_identity') else 'publication_network_unqualified')
                assert not state['ddl']
        assert state['connections'] == 2
        assert state['qualifications'] == (2 if failure in ('stable_identity', 'shadow_identity') else 3)
        assert state['statements'][:3] == ['SET query_timeout=3', 'SHOW GRANTS', 'SET query_timeout=3']
    return 'actual_adapter_rechecks_network_and_both_original_identities_after_ddl_connection_before_swap_sql'


def verify(sessions, signer):
    passed = []
    receipt = json.dumps({'schema': 'managed-network-receipt/v1', 'resource_state': 'kept',
        'verification_phase': 'isolated_network_observed', 'http_connector_sha256': HTTP_CONNECTOR['sha256'],
        'engine': {'ip': '192.0.2.4', 'rest_port': 8080, 'file_encoding': 'UTF-8'},
        'gateway': {'ip': '192.0.2.5', 'ingress_port': 8080}, 'fixture_only': True}).encode()
    with TemporaryDirectory(prefix='publication-boundary-') as folder:
        receipt_path = Path(folder) / 'receipt.json'
        receipt_path.write_bytes(receipt)
        with patch.dict(os.environ, {'REAL_NETWORK_RECEIPT_FILE': str(receipt_path)}), \
             patch('app.preparations.current_execution_facts', side_effect=lambda db, policy, version:
                   deepcopy(version.frozen['execution_facts'])), patch('app.real_runtime._external_authorization'):
            passed.append(verify_mutation_boundary(receipt_path))
            def fixture(*, allow_empty=False, empty=False, expect_ready=True, missing_load_bytes=False):
                cid, policy, provider, manifest = seed(sessions, signer, receipt)
                run_id = approve_publish(sessions, signer, cid)
                with sessions.begin() as db:
                    command, prep = db.get(ManagedCommand, cid), db.get(Preparation, db.get(ExecutionRun, run_id).preparation_id)
                    frozen = deepcopy(prep.frozen)
                    frozen['publication_contract']['stable_identity']['database'] = policy.get('target')['database']
                    frozen['publication_contract']['allow_empty'] = allow_empty
                    if allow_empty:
                        frozen['plan']['allow_empty'] = True
                        frozen['execution_facts']['plan']['allow_empty'] = True
                        from app.runtime_supervision import supervision_contract
                        contract = supervision_contract(frozen['budget'], allow_empty=True)
                        frozen['runtime_supervision_contract'] = contract
                        frozen['execution_facts']['runtime_supervision_contract'] = contract
                        db.get(RuntimeSupervision, cid).contract = contract
                        frozen['execution_sha256'] = digest(frozen['execution_facts'])
                        version = db.get(PipelineVersion, prep.version_id)
                        version.frozen = {**version.frozen, 'execution_facts': deepcopy(frozen['execution_facts']),
                                          'normalized_plan': deepcopy(frozen['plan'])}
                        version.execution_sha256 = frozen['execution_sha256']
                    prep.frozen, prep.frozen_sha256 = frozen, digest(frozen)
                    for approval in db.scalars(select(ApprovalDecision).where(ApprovalDecision.preparation_id == prep.id)):
                        approval.frozen_sha256 = prep.frozen_sha256
                    command.envelope = sign_envelope(signer, {'format': COMMAND_FORMAT, 'command_id': cid,
                        'run_id': run_id, 'accepted_at': db.get(ExecutionRun, run_id).accepted_at.isoformat(),
                        **authorization_bindings(prep, 'operator')})
                objects, engine = Objects(sessions), FinishedEngine(sessions, cid)
                engine.state, engine.source_rows = ('FINISHED', 0) if empty else ('RUNNING', 0)
                assert submit_real(cid, sessions=sessions, policy=policy, secrets=provider, manifest=manifest,
                    client_factory=lambda url: engine, network_verifier=lambda entry, raw: raw == receipt,
                    objects_factory=objects.factory) == 'submitted'
                resolver = RealResourceResolver(sessions=sessions, policy=policy, secrets=provider, manifest=manifest)
                class MissingBytes(RowAdapter):
                    def send(self, frozen, label, body):
                        receipt = super().send(frozen, label, body)
                        raw = json.loads(receipt.raw)
                        del raw['LoadBytes']
                        return replace(receipt, raw=json.dumps(raw).encode())
                loads = ManagedLoad(MissingBytes() if missing_load_bytes else RowAdapter(), sessions)
                ingress = IngressService(lambda facts: loads, resolver.authorize, sessions)
                def observe_control():
                    with sessions() as db:
                        previous = db.get(RuntimeSupervision, cid).last_trusted
                    due = datetime.fromisoformat(previous['observed_at']) + timedelta(seconds=2) if previous else None
                    clock = patch('app.real_control._now', return_value=due) if due else nullcontext()
                    with clock:
                        return advance_real_control(cid, sessions=sessions, ingress_factory=lambda original: ingress,
                            client_factory=lambda url: engine)

                def admission_clock():
                    # Freeze only the SQL admission clock, never the actual controller's trusted facts.
                    with sessions() as db:
                        sample = db.get(RuntimeSupervision, cid).last_trusted
                    assert sample and sample['state'] == 'running' and sample['byte_kind'] == 'measured'
                    instant = ingress._binding(cid).last_complete_observed_at + timedelta(milliseconds=1)
                    assert abs((instant - datetime.fromisoformat(sample['observed_at'])).total_seconds()) <= .2
                    class AdmissionClock:
                        def clock_timestamp(self):
                            return literal(instant)

                        def __getattr__(self, name):
                            return getattr(func, name)

                    return patch('app.real_ingress.func', AdmissionClock())

                if missing_load_bytes:
                    engine.source_rows = 1
                    assert observe_control() == 'submitted'
                    try:
                        with admission_clock():
                            ingress.post(cid, _token(cid), b'{"id":1}')
                        raise AssertionError('receipt without original LoadBytes admitted')
                    except IngressDenied as error:
                        assert error.code == 'ingress_record_or_load_failed'
                    observation = ingress.observe(cid)
                    assert observation['complete'] is False and observation['sink_bytes'] is None
                    engine.state = 'FINISHED'
                    status = observe_control()
                    with sessions.begin() as db:
                        assert db.get(RuntimeSupervision, cid).publication_veto
                        assert db.get(QualityRun, run_id).finalized is False
                        assert db.scalar(select(PublicationIntent).where(PublicationIntent.command_id == cid)) is None
                        db.get(RunControl, cid).wake_at = None
                    return SimpleNamespace(cid=cid, run_id=run_id, status=status)
                for identifier in (() if empty else (1, 2)):
                    engine.source_rows = identifier
                    assert observe_control() == 'submitted'
                    with admission_clock():
                        assert ingress.post(cid, _token(cid), json.dumps({'id': identifier}).encode())['accepted']
                engine.state = 'FINISHED'
                status = observe_control()
                if not expect_ready:
                    with sessions.begin() as db:
                        supervision = db.get(RuntimeSupervision, cid)
                        assert supervision.publication_veto and 'empty_result' in supervision.reasons
                        assert db.get(QualityRun, run_id).finalized is False
                        # Isolate this veto case from the later publisher recovery test.
                        db.get(RunControl, cid).wake_at = None
                    return SimpleNamespace(cid=cid, run_id=run_id, status=status)
                assert status == 'engine_finished_awaiting_quality'
                with patch('app.real_finalization.DorisObjects', side_effect=lambda resource, credential:
                           SimpleNamespace(inspect_shadow=objects.inspect_shadow)):
                    assert finalize_real(cid, sessions=sessions, policy=policy, secrets=provider, manifest=manifest,
                        ingress_factory=lambda original: ingress,
                        network_verifier=lambda entry, raw: raw == receipt) == 'supervision_passed'
                with sessions() as db:
                    prep = db.get(Preparation, db.get(ExecutionRun, run_id).preparation_id)
                    before = {'stable': prep.frozen['publication_contract']['stable_identity'],
                              'shadow': db.get(EngineAttempt, cid).request['real_routes']['valid']['object_identity']}
                adapter = SwapAdapter(before, sessions)
                adapter.run_id = run_id
                service = PublicationService(sessions, adapter, policy=policy)
                return SimpleNamespace(cid=cid, run_id=run_id, policy=policy, service=service, adapter=adapter)

            denied = fixture(empty=True, expect_ready=False)
            passed.append('default_empty_input_vetoes_before_finalization_or_swap')
            permitted_empty = fixture(allow_empty=True, empty=True)
            empty_intent = permitted_empty.service.prepare(permitted_empty.cid, 'allow-empty-empty')
            assert permitted_empty.service.publish(empty_intent)['phase'] == 'confirmed'
            assert permitted_empty.adapter.swaps == 1
            with sessions() as db:
                metrics = db.get(RuntimeSupervision, permitted_empty.cid).current_metrics
                assert metrics['sink_bytes'] == 0 and metrics['amplification'] is None and metrics['reject_ratio'] is None
            passed.append('signed_two_approval_empty_full_chain_confirmed_with_closed_zero_batch_journals')

            permitted = fixture(allow_empty=True)
            permitted_intent = permitted.service.prepare(permitted.cid, 'allow-empty-nonempty')
            assert permitted.service.publish(permitted_intent)['phase'] == 'confirmed'
            assert permitted.adapter.swaps == 1
            passed.append('allow_empty_permission_still_accepts_nonempty_snapshot')

            fixture(allow_empty=True, missing_load_bytes=True)
            passed.append('allow_empty_missing_original_loadbytes_remains_unknown_and_vetoed')
            for failure in ('approval_missing', 'empty_policy_unfrozen', 'bytes_unknown', 'bytes_estimated'):
                f = fixture(allow_empty=True, empty=True)
                with sessions.begin() as db:
                    if failure == 'approval_missing':
                        prep_id = db.get(ExecutionRun, f.run_id).preparation_id
                        db.delete(db.scalar(select(ApprovalDecision).where(
                            ApprovalDecision.preparation_id == prep_id, ApprovalDecision.slot == 'security')))
                    else:
                        supervision = db.get(RuntimeSupervision, f.cid)
                        if failure == 'empty_policy_unfrozen':
                            supervision.contract = {**supervision.contract, 'empty_policy': 'veto_publication'}
                        elif failure == 'bytes_unknown':
                            supervision.last_trusted = {**supervision.last_trusted, 'sink_bytes': None}
                        else:
                            supervision.last_trusted = {**supervision.last_trusted, 'byte_kind': 'estimated'}
                        if failure.startswith('bytes_'):
                            control = db.get(RunControl, f.cid)
                            facts = deepcopy(control.facts)
                            facts['final_metrics']['sample'] = deepcopy(supervision.last_trusted)
                            control.facts = facts
                try:
                    f.service.prepare(f.cid, failure)
                    raise AssertionError('untrusted empty publication passed: ' + failure)
                except (ValueError, HTTPException):
                    pass
                assert f.adapter.swaps == 0
            passed.append('empty_missing_approval_unfrozen_permission_and_untrusted_bytes_fail_closed')

            # Same-db lifecycle policy is explicit and cannot relax metadata.read.
            check_publication_grants([{'Roles': '', 'DatabasePrivs':
                'internal.scope: Select_priv,Load_priv,Create_priv,Alter_priv,Drop_priv'}],
                'scope', ['Alter_priv', 'Drop_priv'])
            for grants, configured in [
                ([{'DatabasePrivs': 'internal.other: Alter_priv'}], ['Alter_priv']),
                ([{'GlobalPrivs': 'Alter_priv'}], ['Alter_priv']),
                ([{'DatabasePrivs': 'internal.scope: Alter_priv'}], [])]:
                try:
                    check_publication_grants(grants, 'scope', configured)
                    raise AssertionError('unapproved lifecycle grants accepted')
                except ValueError:
                    pass
            passed.append('explicit_same_database_lifecycle_rights_only_no_global_or_implicit_alter')

            from app import real_publication as boundary
            f = fixture()
            identifier = f.service.prepare(f.cid, 'durable-recovery')
            other_cid, _, _, _ = seed(sessions, signer, receipt)
            with sessions() as db:
                intent = db.get(PublicationIntent, identifier)
                assert intent.recovery_at == intent.created_at
            with patch.object(boundary, '_service') as factory, \
                 patch('app.real_runtime._command_lock') as command_lock:
                try:
                    boundary.advance_publication(other_cid, identifier, sessions)
                    raise AssertionError('wrong command accepted original publication intent')
                except ValueError as error:
                    assert str(error) == 'publication_command_mismatch'
                factory.assert_not_called()
                command_lock.assert_not_called()
            with sessions() as db:
                assert db.get(PublicationIntent, identifier).phase == 'prepared'
            def assert_due():
                with sessions() as db:
                    intent = db.get(PublicationIntent, identifier)
                    remaining = (intent.recovery_at - db.scalar(select(func.clock_timestamp()))).total_seconds()
                    assert 0 < remaining <= 15, remaining
            with patch('app.real_runtime._command_lock', return_value=nullcontext(False)), \
                 patch.object(boundary, '_service') as factory:
                assert boundary.advance_publication(f.cid, identifier, sessions) == 'busy'
                factory.assert_not_called()
            assert_due()
            with patch.object(boundary, '_service', side_effect=OSError('fixture delivery failure')):
                assert boundary.advance_publication(f.cid, identifier, sessions) == 'publication_pending'
            assert_due()
            with sessions.begin() as db:
                db.get(PublicationIntent, identifier).phase = 'dispatching'
            with patch.object(boundary, '_service', return_value=f.service):
                result = boundary.advance_publication(f.cid, identifier, sessions)
                assert result['phase'] == 'unknown' and f.adapter.swaps == 0
                assert result['reason'] == 'before_state_observed_no_replay'
                assert_due()
                with patch.object(f.adapter, 'observe', side_effect=OSError('fixture observation failure')):
                    assert boundary.advance_publication(f.cid, identifier, sessions)['phase'] == 'unknown'
                assert_due()
            passed.append('original_intent_wrong_command_denied_before_lock_or_adapter_and_busy_transport_failure_dispatching_keep_durable_due_without_swap')

            # A real publisher iteration commits the next due before calling the
            # broker. Delivery succeeds but the broker ACK is lost; due dispatch
            # later resumes only the original intent's read-only reconciliation.
            from app import outbox_publisher as publisher
            def make_due():
                with sessions.begin() as db:
                    db.get(PublicationIntent, identifier).recovery_at = db.scalar(
                        select(func.clock_timestamp())) - timedelta(seconds=1)
            def delivered(args):
                assert args == [f.cid, identifier]
                result = boundary.advance_publication(*args, sessions=sessions)
                assert result['phase'] == 'unknown' and f.adapter.swaps == 0
            def lost_ack(args):
                delivered(args)
                raise OSError('fixture broker ACK lost after delivery')
            with patch.object(publisher, 'Session', sessions), \
                 patch.object(publisher, 'command_lock', side_effect=lambda key: nullcontext(True)), \
                 patch.object(publisher.execute, 'apply_async'), \
                 patch.object(publisher.reconcile, 'apply_async'), \
                 patch.object(boundary, '_service', return_value=f.service), \
                 patch.object(publisher.publish_snapshot, 'apply_async', side_effect=lost_ack) as publish_task:
                make_due()
                try:
                    publisher.publish_once()
                    raise AssertionError('fixture ACK fault did not execute')
                except OSError as error:
                    assert str(error) == 'fixture broker ACK lost after delivery'
                assert_due()
                publish_task.assert_called_once_with(args=[f.cid, identifier])
                publish_task.reset_mock()
                publisher.publish_once()
                publish_task.assert_not_called()
                make_due()
                publish_task.side_effect = delivered
                publisher.publish_once()
                publish_task.assert_called_once_with(args=[f.cid, identifier])
                assert_due()
            passed.append('actual_publisher_lost_broker_ack_rearms_original_intent_and_repeated_delivery_only_reads_without_second_swap')

            f = fixture()
            identifier = f.service.prepare(f.cid, 'first-publication')
            result = f.service.publish(identifier)
            assert result['phase'] == 'confirmed' and result['ownership_retained'] is False and f.adapter.swaps == 1
            assert f.service.prepare(f.cid, 'first-publication') == identifier
            assert f.service.publish(identifier)['phase'] == 'confirmed' and f.adapter.swaps == 1
            with sessions() as db:
                snapshot = db.scalar(select(PublishedSnapshot).where(PublishedSnapshot.intent_id == identifier))
                assert snapshot.retain_until - snapshot.confirmed_at >= timedelta(days=7)
                assert snapshot.identities['after'] == f.adapter.current
                assert db.get(ExecutionRun, f.run_id).status == 'published'
                assert publication_view(db, f.cid)['published'] is True
                assert db.get(PublicationIntent, identifier).recovery_at is None
                assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == f.cid)) is None
                target_key = snapshot.target_key
                previous_target = deepcopy(db.get(PublicationIntent, identifier).inputs['before']['stable'])
                from app.real_cleanup import _contains_object, real_cleanup_binding
                assert _contains_object(snapshot.identities, previous_target)
                try:
                    real_cleanup_binding(db, db.get(ManagedCommand, f.cid), db.get(ExecutionRun, f.run_id),
                                         db.get(RunControl, f.cid))
                    raise AssertionError('published old objects allowed prepublication cleanup')
                except ValueError:
                    pass
            # A different real managed command can acquire the same unique key;
            # snapshot retention does not reserve the execution serialization.
            next_cid, _, _, _ = seed(sessions, signer, receipt)
            with sessions.begin() as db:
                target = {key: previous_target[key] for key in ('physical_cluster_id', 'database', 'table')}
                db.add(TargetOwnership(target_key=target_key, command_id=next_cid,
                    identity=target, acquired_at=datetime.now(timezone.utc)))
            with sessions() as db:
                assert db.get(TargetOwnership, target_key).command_id == next_cid
                assert db.scalar(select(PublishedSnapshot).where(PublishedSnapshot.intent_id == identifier))
            assert f.service.publish(identifier)['ownership_retained'] is False
            with patch.object(boundary, '_service') as factory:
                assert boundary.advance_publication(f.cid, identifier, sessions)['phase'] == 'confirmed'
                factory.assert_not_called()
            passed.append('signed_p1_exact_confirm_releases_target_for_next_command_retains_seven_day_snapshot_and_cleanup_protection')

            f = fixture()
            identifier = f.service.prepare(f.cid, 'lost-first-reply')
            f.adapter.lose_response = True
            assert f.service.publish(identifier)['phase'] == 'unknown' and f.adapter.swaps == 1
            assert f.service.publish(identifier)['phase'] == 'unknown' and f.adapter.swaps == 1
            with sessions() as db:
                assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == f.cid))
                assert not db.scalar(select(PublishedSnapshot).where(PublishedSnapshot.intent_id == identifier))
            assert f.service.reconcile(identifier)['phase'] == 'confirmed' and f.adapter.swaps == 1
            passed.append('first_swap_lost_reply_unknown_no_replay_readonly_exact_identity_reconciliation')

            # Actual PostgreSQL deferred FK failure at COMMIT after external swap.
            with sessions.begin() as db:
                db.execute(text('CREATE TABLE publication_fault_parent (id integer PRIMARY KEY)'))
                db.execute(text('CREATE TABLE publication_fault_child (id integer REFERENCES '
                    'publication_fault_parent(id) DEFERRABLE INITIALLY DEFERRED)'))
            f = fixture()
            identifier = f.service.prepare(f.cid, 'first-result-commit-fault')
            f.adapter.fail_commit = True
            def fail_after_swap(db):
                if f.adapter.commit_fault_armed:
                    f.adapter.commit_fault_armed = False
                    db.execute(text('INSERT INTO publication_fault_child VALUES (999)'))
            event.listen(sessions.class_, 'before_commit', fail_after_swap)
            try:
                assert f.service.publish(identifier)['phase'] == 'unknown'
            finally:
                event.remove(sessions.class_, 'before_commit', fail_after_swap)
            assert f.adapter.swaps == 1 and f.service.publish(identifier)['phase'] == 'unknown'
            with sessions() as db:
                assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == f.cid))
                assert not db.scalar(select(PublishedSnapshot).where(PublishedSnapshot.intent_id == identifier))
            assert f.service.reconcile(identifier)['phase'] == 'confirmed' and f.adapter.swaps == 1
            passed.append('first_swap_actual_pg_deferred_commit_fault_retains_intent_and_never_repeats_sql')

            # A COMMIT fault in the exact confirmation transaction must roll back
            # snapshot, published status and ownership deletion together.
            f = fixture()
            identifier = f.service.prepare(f.cid, 'confirmation-commit-fault')
            fault = {'armed': True}
            def fail_confirmation(db, *args):
                if fault['armed'] and any(isinstance(row, PublishedSnapshot) for row in db.new):
                    fault['armed'] = False
                    db.execute(text('INSERT INTO publication_fault_child VALUES (1000)'))
            event.listen(sessions.class_, 'before_flush', fail_confirmation)
            try:
                try:
                    f.service.publish(identifier)
                    raise AssertionError('confirmation COMMIT did not fail')
                except Exception as error:
                    assert getattr(getattr(error, 'orig', None), 'sqlstate', None) == '23503'
            finally:
                event.remove(sessions.class_, 'before_flush', fail_confirmation)
            with sessions() as db:
                assert db.get(PublicationIntent, identifier).phase == 'dispatching'
                assert db.get(ExecutionRun, f.run_id).status != 'published'
                assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == f.cid))
                assert not db.scalar(select(PublishedSnapshot).where(PublishedSnapshot.intent_id == identifier))
            assert f.service.publish(identifier)['ownership_retained'] is True and f.adapter.swaps == 1
            confirmed = f.service.reconcile(identifier)
            assert confirmed['phase'] == 'confirmed' and confirmed['ownership_retained'] is False and f.adapter.swaps == 1
            passed.append('confirmation_pg_commit_fault_rolls_back_snapshot_run_and_owner_release_atomically')

            f = fixture()
            f.adapter.authorized = False
            identifier = f.service.prepare(f.cid, 'no-alter-grant')
            assert f.service.publish(identifier)['phase'] == 'unknown' and f.adapter.swaps == 0
            passed.append('execution_select_load_create_does_not_imply_publication_alter_authority')

            # Execute the actual public HTTP route and its post-commit background
            # work. Only the external adapter factory is substituted; signature,
            # subject, P1 gates, transaction and reservation all stay real.
            from app import real_publication as boundary
            f = fixture()
            with sessions() as db:
                actor = db.get(Account, 'operator')
            def fixture_database():
                with sessions.begin() as db:
                    yield db
            def scope(db, actor, tenant, project):
                if actor.tenant_id != tenant or project != 'project':
                    raise HTTPException(403, 'fixture_project_scope_denied')
            app = FastAPI()
            app.state.connector_policy = f.policy
            boundary.register_publication_routes(app, Annotated[object, Depends(fixture_database)],
                Annotated[object, Depends(lambda: actor)], scope)
            responses = []
            async def request(key, extra=None):
                messages = []
                body = json.dumps({'idempotency_key': key, **(extra or {})}).encode()
                async def receive():
                    return {'type': 'http.request', 'body': body, 'more_body': False}
                async def send(message):
                    messages.append(message)
                await app({'type': 'http', 'asgi': {'version': '3.0'}, 'http_version': '1.1',
                    'method': 'POST', 'scheme': 'http',
                    'path': f'/api/tenants/tenant/projects/project/runs/{f.run_id}/publish',
                    'query_string': b'', 'headers': [(b'content-type', b'application/json')],
                    'client': ('127.0.0.1', 1), 'server': ('fixture', 80), 'root_path': ''}, receive, send)
                status = next(item['status'] for item in messages if item['type'] == 'http.response.start')
                responses.append(json.loads(b''.join(item.get('body', b'') for item in messages
                    if item['type'] == 'http.response.body')))
                return status
            f.adapter.lose_response = True
            with patch.object(boundary, '_service', side_effect=lambda bound, cid: f.service):
                assert asyncio.run(request('http-publication')) == 202
                assert f.adapter.swaps == 1
                with sessions() as db:
                    intent = db.scalar(select(PublicationIntent).where(PublicationIntent.command_id == f.cid))
                    identifier = intent.id
                    assert intent.phase == 'unknown' and intent.recovery_at is not None
                    assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == f.cid))
                assert asyncio.run(request('http-publication')) == 202 and f.adapter.swaps == 1
                with sessions() as db:
                    assert db.get(PublicationIntent, identifier).phase == 'confirmed', 'public retry did not reconcile lost swap response'
                    assert db.get(PublicationIntent, identifier).recovery_at is None
                    assert db.scalar(select(PublishedSnapshot).where(PublishedSnapshot.intent_id == identifier))
                    assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == f.cid)) is None
                with patch.object(boundary, '_service') as terminal_factory:
                    assert asyncio.run(request('http-publication')) == 202
                    assert responses[-1]['phase'] == 'confirmed' and responses[-1]['published'] is True
                    assert responses[-1]['ownership_retained'] is False
                    terminal_factory.assert_not_called()
                assert asyncio.run(request('other-key')) == 409 and f.adapter.swaps == 1
                assert asyncio.run(request('bad-extra', {'passed': True})) == 422 and f.adapter.swaps == 1
            passed.append('public_asgi_first_swap_lost_reply_same_intent_post_retry_confirms_snapshot_releases_ownership_clears_due_with_one_swap')

            for failure in ('missing_approval', 'secret_revoked', 'route_missing', 'quality_veto', 'old_write_unconfirmed', 'schema_drift'):
                f = fixture()
                with sessions.begin() as db:
                    command = db.get(ManagedCommand, f.cid)
                    run = db.get(ExecutionRun, f.run_id)
                    if failure == 'missing_approval':
                        db.delete(db.scalar(select(ApprovalDecision).where(ApprovalDecision.preparation_id == run.preparation_id)))
                    elif failure == 'secret_revoked':
                        ref = command.envelope['payload']['resources']['target']['execution_writer']['secret_reference_id']
                        db.get(SecretReference, ref).revoked_at = datetime.now(timezone.utc)
                    elif failure == 'route_missing':
                        quality = db.get(QualityRun, run.id)
                        quality.contract = {**quality.contract, 'route_commands': {'valid': 'missing'}}
                    elif failure == 'quality_veto':
                        quality = db.get(QualityRun, run.id)
                        quality.report = {**quality.report, 'publication_veto': True}
                    elif failure == 'old_write_unconfirmed':
                        control = db.get(RunControl, f.cid)
                        control.facts = {**control.facts, 'termination': {'confirmed': False}}
                    else:
                        attempt = db.get(EngineAttempt, f.cid)
                        request = deepcopy(attempt.request)
                        request['real_routes']['valid']['object_identity']['partition_ids'] = ['stable-partition']
                        attempt.request, attempt.request_sha256 = request, digest(request)
                try:
                    f.service.prepare(f.cid, failure)
                    raise AssertionError('unsafe publication passed: ' + failure)
                except (ValueError, HTTPException):
                    pass
                assert f.adapter.swaps == 0
            passed.append('missing_approval_revoked_secret_missing_route_quality_veto_termination_and_identity_fail_closed')
    return passed


if __name__ == '__main__':
    with database() as (sessions, signer):
        results = verify(sessions, signer)
    print(json.dumps({'level': 'postgresql_signed_p1_publication_fixture', 'real_l2_qualified': False,
        'positive_authorized_publish_tested': True, 'passed': results}, ensure_ascii=False))
