"""Real PostgreSQL/signature/journal cleanup fixtures; external adapters mocked, NOT L2."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from sqlalchemy import func, literal, select, text
from verify_real_runtime_fixtures import database, seed, Objects, Engine, digest
from verify_quality_fixtures import RowAdapter
from app.command_signing import COMMAND_FORMAT, sign_envelope
from app.connector_policy import fingerprint
from app.execution import ExecutionRun, ManagedCommand, authorization_bindings
from app.execution_runtime import EngineAttempt, TargetOwnership
from app.managed_load import LoadBatch, LoadRun, ManagedLoad
from app.models import Connection, Membership, SecretReference
from app.preparations import ApprovalDecision, Preparation
from app.publication import PublicationIntent, PublishedSnapshot
from app.real_cleanup import (CleanupIntent, DorisCleanupAdapter, advance_real_cleanup, check_cleanup_grants,
                              real_cleanup_binding)
from app.real_control import advance_real_control
from app.real_ingress import IngressService
from app.real_job import HTTP_CONNECTOR
from app.real_resources import RealResourceResolver
from app.real_runtime import _token, submit_real
from app.run_control import CONTROL_FORMAT, ManagedControlEffect, RunControl
from app.supervision_runtime import RuntimeSupervision
from app.versions import PipelineVersion
from app.writer_credentials import execution_writer_binding, writer_resource


class _SqlClock:
    """Freeze one SQL clock while delegating aggregates to real SQLAlchemy."""
    def __init__(self, observed_at):
        self.observed_at = observed_at

    def clock_timestamp(self):
        return literal(self.observed_at)

    def __getattr__(self, name):
        return getattr(func, name)


class TerminalEngine(Engine):
    def observe(self, identifier, name):
        self.gets += 1
        assert identifier == self.original['engine_job_id'] and name == self.original['engine_job_name']
        return {'job_id': identifier, 'job_name': name, 'engine_state': 'FINISHED',
                'metrics': {'SourceReceivedCount': '1'}}


def grant_rows(database, rights='Select_priv,Load_priv,Create_priv,Drop_priv'):
    return [{'Roles': '', 'GlobalPrivs': None,
             'DatabasePrivs': 'internal.' + database + ': ' + rights +
                 '; internal.information_schema: Select_priv; internal.mysql: Select_priv',
             'WorkloadGroupPrivs': 'normal: Usage_priv'}]


class CleanupAdapter:
    def __init__(self, objects, sessions, cid, resource):
        self.objects, self.sessions, self.cid, self.resource = objects, sessions, cid, resource
        self.drops, self.lost, self.death, self.after_drop = [], False, False, None

    def privileges(self):
        return grant_rows(self.resource['database'])

    def inspect(self, route):
        return deepcopy(self.objects.identities.get(route['object_run_id']))

    def drop(self, route, *, before_mutation=None):
        with self.sessions() as db:
            intent = db.get(CleanupIntent, (self.cid,
                'rejected' if route['object_run_id'].endswith('-error') else 'valid'))
            assert intent.phase == 'dispatching', 'reservation must commit before DROP'
        assert self.inspect(route) == route['object_identity']
        if before_mutation is not None:
            before_mutation()
        self.drops.append(route['object_run_id'])
        del self.objects.identities[route['object_run_id']]
        if self.after_drop:
            callback, self.after_drop = self.after_drop, None
            callback()
        if self.death:
            self.death = False
            raise KeyboardInterrupt('simulated worker death')
        if self.lost:
            self.lost = False
            raise OSError('synthetic-only-secret-never-persisted')


def enable_policy(sessions, signer, cid, policy, manifest):
    original_get = policy.get
    def get(key):
        value = original_get(key)
        if key == 'target':
            value['execution_writer']['lifecycle_privileges'] = ['Drop_priv']
        return value
    policy.get = get
    target = policy.get('target')
    with sessions.begin() as db:
        command = db.get(ManagedCommand, cid)
        run = db.get(ExecutionRun, command.run_id)
        prep = db.get(Preparation, run.preparation_id)
        frozen = deepcopy(prep.frozen)
        facts = frozen['execution_facts']
        asset = facts['target']
        connection = db.get(Connection, asset['connection_id'])
        connection.resource_fingerprint = fingerprint(target)
        asset['resource_fingerprint'] = fingerprint(target)
        asset['execution_writer'] = execution_writer_binding(db, target, connection)
        frozen['execution_sha256'] = digest(facts)
        prep.frozen, prep.frozen_sha256 = frozen, digest(frozen)
        version = db.get(PipelineVersion, prep.version_id)
        version.frozen = {'execution_facts': deepcopy(facts), 'normalized_plan': deepcopy(frozen['plan'])}
        version.execution_sha256 = digest(facts)
        for approval in db.scalars(select(ApprovalDecision).where(ApprovalDecision.preparation_id == prep.id)):
            approval.frozen_sha256 = prep.frozen_sha256
        command.envelope = sign_envelope(signer, {'format': COMMAND_FORMAT, 'command_id': cid,
            'run_id': run.id, 'accepted_at': run.accepted_at.isoformat(), **authorization_bindings(prep, run.subject)})
    entry = manifest['resources']['target']
    entry['resource_fingerprint'] = fingerprint(target)
    entry['writer_resource_fingerprint'] = fingerprint(writer_resource(target))


def verify(sessions, signer):
    passed = []
    receipt = json.dumps({'schema': 'managed-network-receipt/v1', 'resource_state': 'kept',
        'verification_phase': 'isolated_network_observed', 'http_connector_sha256': HTTP_CONNECTOR['sha256'],
        'engine': {'ip': '192.0.2.4', 'rest_port': 8080, 'file_encoding': 'UTF-8'},
        'gateway': {'ip': '192.0.2.5', 'ingress_port': 8080}}).encode()
    with TemporaryDirectory() as folder:
        path = Path(folder) / 'receipt.json'
        path.write_bytes(receipt)
        with patch.dict(os.environ, {'REAL_NETWORK_RECEIPT_FILE': str(path)}), \
             patch('app.preparations.current_execution_facts', side_effect=lambda db, policy, version:
                   deepcopy(version.frozen['execution_facts'])), patch('app.real_runtime._external_authorization'):
            def fixture():
                cid, policy, secrets, manifest = seed(sessions, signer, receipt)
                enable_policy(sessions, signer, cid, policy, manifest)
                objects, engine = Objects(sessions), TerminalEngine(sessions, cid)
                assert submit_real(cid, sessions=sessions, policy=policy, secrets=secrets, manifest=manifest,
                    client_factory=lambda url: engine, network_verifier=lambda entry, raw: raw == receipt,
                    objects_factory=objects.factory) == 'submitted'
                resolver = RealResourceResolver(sessions=sessions, policy=policy, secrets=secrets, manifest=manifest)
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
                with patch('app.real_ingress.func', _SqlClock(admission_time)):
                    assert ingress.post(cid, _token(cid), b'{"id":1}')['accepted']
                with patch('app.real_control._now', return_value=max(datetime.now(timezone.utc), sample_time + timedelta(seconds=2))):
                    assert advance_real_control(cid, sessions=sessions, ingress_factory=lambda frozen: ingress,
                        client_factory=lambda url: engine) == 'engine_finished_awaiting_quality'
                with sessions.begin() as db:
                    command = db.get(ManagedCommand, cid)
                    run, control = db.get(ExecutionRun, command.run_id), db.get(RunControl, cid)
                    binding = real_cleanup_binding(db, command, run, control)
                    effect_id = str(uuid4())
                    payload = {'format': CONTROL_FORMAT, 'effect_id': effect_id, 'command_id': cid,
                        'run_id': run.id, 'tenant_id': run.tenant_id, 'project_id': run.project_id,
                        'actor_id': 'operator', 'kind': 'cleanup', 'reason': 'operator_requested',
                        'resources': command.envelope['payload']['resources'], **binding}
                    now = datetime.now(timezone.utc)
                    db.add(ManagedControlEffect(id=effect_id, command_id=cid, kind='cleanup', actor_id='operator',
                        idempotency_key='cleanup-fixture', request_sha256=digest({'actor_id': 'operator',
                            'kind': 'cleanup', 'reason': 'operator_requested'}), envelope=sign_envelope(signer, payload),
                        phase='accepted', created_at=now, updated_at=now))
                    request, run_id = deepcopy(db.get(EngineAttempt, cid).request), run.id
                adapter = CleanupAdapter(objects, sessions, cid, writer_resource(policy.get('target')))
                def cleanup(**overrides):
                    arguments = {'sessions': sessions, 'policy': policy, 'secrets': secrets, 'manifest': manifest,
                        'network_verifier': lambda entry, raw: raw == receipt,
                        'ingress_factory': lambda request: ingress, 'client_factory': lambda url: engine,
                        'adapter_factory': lambda resource, credential: adapter}
                    arguments.update(overrides)
                    return advance_real_cleanup(cid, **arguments)
                return cid, effect_id, request, run_id, adapter, cleanup

            def retained(cid):
                with sessions() as db:
                    assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid)) is not None

            cid, eid, request, run_id, adapter, cleanup = fixture()
            assert cleanup() == 'cleaned' and len(adapter.drops) == 2
            assert cleanup() == 'cleaned' and len(adapter.drops) == 2
            with sessions() as db:
                assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid)) is None
                assert db.get(ManagedControlEffect, eid).phase == 'confirmed'
            passed.append('two_original_shadows_single_drop_each_owner_release_only_after_absence_duplicate_no_io')

            for death in (False, True):
                cid, eid, request, run_id, adapter, cleanup = fixture()
                adapter.lost, adapter.death = not death, death
                if death:
                    try:
                        cleanup()
                        raise AssertionError('expected death')
                    except KeyboardInterrupt:
                        pass
                else:
                    assert cleanup() == 'cleanup_pending'
                retained(cid)
                assert cleanup() == 'cleaned' and len(adapter.drops) == 2
            passed.append('lost_drop_reply_and_worker_death_read_absence_never_repeat_drop')

            cid, eid, request, run_id, adapter, cleanup = fixture()
            adapter.objects.identities[run_id]['partition_ids'] = ['replacement-partition']
            assert cleanup() == 'cleanup_denied' and not adapter.drops
            retained(cid)
            passed.append('replacement_identity_before_any_drop_preserves_both_objects_owner')

            for fault in ('metrics', 'lease', 'role', 'secret', 'journal', 'publication'):
                cid, eid, request, run_id, adapter, cleanup = fixture()
                with sessions.begin() as db:
                    control = db.get(RunControl, cid)
                    if fault == 'metrics':
                        control.facts = {**control.facts, 'final_metrics': {'status': 'pending'}}
                    elif fault == 'lease':
                        control.lease_expires_at = datetime.now(timezone.utc) - timedelta(days=1)
                        control.facts = {**control.facts, 'termination': {'confirmed': False}}
                    elif fault == 'role':
                        db.get(Membership, ('project', 'operator')).roles = []
                    elif fault == 'secret':
                        ref = request['real_routes']['valid']['writer_secret_reference_id']
                        db.get(SecretReference, ref).revoked_at = datetime.now(timezone.utc)
                    elif fault == 'journal':
                        row = db.scalar(select(LoadBatch).where(LoadBatch.command_id == request['real_routes']['valid']['command_id']))
                        row.phase, row.receipt, row.response_sha256 = 'unknown', None, None
                    else:
                        now = datetime.now(timezone.utc)
                        db.add(PublicationIntent(id=str(uuid4()), command_id=cid, run_id=run_id,
                            idempotency_key='publication', target_key='x'*64, inputs={}, input_sha256='x'*64,
                            phase='unknown', created_at=now, updated_at=now))
                assert cleanup() == 'cleanup_denied' and not adapter.drops
                retained(cid)
                if fault == 'role':
                    with sessions.begin() as db:
                        db.get(Membership, ('project', 'operator')).roles = ['operator']
            passed.append('missing_final_metrics_lease_without_termination_role_revocation_secret_unknown_batch_publication_deny')

            cid, eid, request, run_id, adapter, cleanup = fixture()
            assert cleanup(network_verifier=lambda entry, raw: False) == 'cleanup_denied' and not adapter.drops
            retained(cid)
            passed.append('historical_receipt_is_not_live_network_quiescence')

            for stale_stage in ('grants', 'object_inspection', 'cursor_open'):
                cid, eid, request, run_id, adapter, cleanup = fixture()
                actual = DorisCleanupAdapter(adapter.resource, 'unused-fixture-only')
                state = {'fresh': True, 'privilege_reads': 0, 'inside_drop': False, 'drops': 0, 'cursor_opened': False}
                def privileges():
                    state['privilege_reads'] += 1
                    state['inside_drop'] = state['privilege_reads'] >= 2
                    if state['inside_drop'] and stale_stage == 'grants':
                        state['fresh'] = False
                    return adapter.privileges()
                def inspect(route):
                    result = adapter.inspect(route)
                    if state['inside_drop'] and stale_stage == 'object_inspection':
                        state['fresh'] = False
                    return result
                class Cursor:
                    def execute(self, statement):
                        assert statement.startswith('DROP TABLE ')
                        state['drops'] += 1
                @contextmanager
                def opened_cursor():
                    # This seam represents successful connection/SET timeout; the
                    # real adapter must invoke authorization after entering it.
                    state['cursor_opened'] = True
                    if stale_stage == 'cursor_open':
                        state['fresh'] = False
                    yield Cursor()
                with patch.object(actual, 'privileges', side_effect=privileges), \
                     patch.object(actual, 'inspect', side_effect=inspect), \
                     patch.object(actual.objects, '_cursor', side_effect=opened_cursor):
                    assert cleanup(adapter_factory=lambda resource, credential: actual,
                        network_verifier=lambda entry, raw: state['fresh'] and raw == receipt) == 'cleanup_pending'
                assert state['cursor_opened'] and state['drops'] == 0 and not adapter.drops
                retained(cid)
                with sessions() as db:
                    intent = db.get(CleanupIntent, (cid, 'valid'))
                    assert intent is not None and intent.phase == 'dispatching'
                    assert db.get(ManagedControlEffect, eid).phase == 'unknown'
                # Freshness recovery cannot reuse a consumed DROP reservation.
                assert cleanup() == 'cleanup_pending' and not adapter.drops
                retained(cid)
            passed.append('actual_drop_adapter_rechecks_after_grants_identity_and_open_cursor_stale_witness_sends_zero_drop_keeps_reservation')

            # A genuine database result-commit failure occurs AFTER the adapter's
            # single DROP. Removing the fault permits read-only reconciliation.
            cid, eid, request, run_id, adapter, cleanup = fixture()
            suffix = uuid4().hex[:12]
            def fail_commit():
                with sessions.begin() as db:
                    db.execute(text(f"CREATE FUNCTION fail_cleanup_{suffix}() RETURNS trigger LANGUAGE plpgsql AS $$ "
                        "BEGIN IF NEW.phase = 'confirmed' THEN RAISE EXCEPTION 'fixture commit failure' "
                        "USING ERRCODE='40001'; END IF; RETURN NEW; END $$"))
                    db.execute(text(f'CREATE CONSTRAINT TRIGGER cleanup_fault_{suffix} AFTER UPDATE ON real_cleanup_intents '
                        f'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_cleanup_{suffix}()'))
            adapter.after_drop = fail_commit
            assert cleanup() == 'cleanup_pending' and len(adapter.drops) == 1
            retained(cid)
            with sessions.begin() as db:
                db.execute(text(f'DROP TRIGGER cleanup_fault_{suffix} ON real_cleanup_intents'))
                db.execute(text(f'DROP FUNCTION fail_cleanup_{suffix}()'))
            assert cleanup() == 'cleaned' and len(adapter.drops) == 2
            passed.append('real_pg_deferred_40001_result_commit_failure_keeps_reservation_then_reads_absence')

            # Snapshot protection is physical, including renamed old tables.
            cid, eid, request, run_id, adapter, cleanup = fixture()
            identity = deepcopy(request['real_routes']['valid']['object_identity'])
            identity['table'] = 'retained_old_name'
            with sessions.begin() as db:
                now = datetime.now(timezone.utc)
                db.add(PublishedSnapshot(id=str(uuid4()), intent_id=str(uuid4()), run_id='another-run',
                    target_key='z'*64, identities={'old': identity}, confirmed_at=now, retain_until=now+timedelta(days=7)))
            assert cleanup() == 'cleanup_denied' and not adapter.drops
            retained(cid)
            passed.append('retained_snapshot_partition_identity_protected_across_rename')

            for configured, rights in (([], 'Select_priv,Load_priv,Create_priv,Drop_priv'),
                                       (['Drop_priv'], 'Select_priv,Load_priv,Create_priv')):
                try:
                    check_cleanup_grants(grant_rows('approved', rights), 'approved', configured)
                    raise AssertionError('DROP authority must be explicit and observed')
                except ValueError:
                    pass
            check_cleanup_grants(grant_rows('approved', 'Select_priv,Load_priv,Create_priv,Drop_priv,Alter_priv'),
                                 'approved', ['Drop_priv', 'Alter_priv'])
            passed.append('same_database_drop_configuration_and_actual_grant_both_required_alter_independent')
    return passed


if __name__ == '__main__':
    with database() as (sessions, signer):
        checks = verify(sessions, signer)
    print(json.dumps({'level': 'postgresql_real_cleanup_fixture', 'passed': checks,
                      'postgresql_random_schema': True, 'real_l2_qualified': False}))
