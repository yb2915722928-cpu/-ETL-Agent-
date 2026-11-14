"""Random-schema real PostgreSQL submission fault fixtures; seams, NOT real L2.

Run with the same DATABASE_URL/compose environment as real_control fixtures.
No deployment migration, actual Doris/SeaTunnel I/O, publication or cleanup.
"""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import base64
import hashlib
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from app.command_signing import COMMAND_FORMAT, sign_envelope, signing_bytes
from app.connector_policy import fingerprint
from app.db import Base
from app.doris_objects import object_marker, table_schema
from app.evidence_signing import EvidenceSigner
from app.execution import ExecutionRun, ManagedCommand, authorization_bindings
from app.execution_runtime import EngineAttempt, TargetOwnership
from app.managed_load import LoadRun
from app.managed_secrets import SecretProvider
from app.models import Account, Connection, EvidenceEvent, Membership, Project, SecretReference, Tenant
from app.preparations import ApprovalDecision, POLICY_VERSION, Preparation
from app.real_contract import real_execution_contract
from app.real_engine import real_runtime_pin, shadow_table
from app.real_ingress import IngressBinding
from app.real_job import HTTP_CONNECTOR
from app.real_runtime import RealObjectIntent, ingress_service, submit_real
from app.run_control import RunControl
from app.runtime_supervision import RuntimeSupervisionContract
from app.studio import Candidate
from app.studio_plan import VERSIONS
from app.supervision_runtime import RuntimeSupervision, request_stop
from app.versions import PipelineVersion
from app.writer_credentials import execution_writer_binding, writer_resource


def digest(value):
    return hashlib.sha256(signing_bytes(value)).hexdigest()


@contextmanager
def database():
    schema = 'real_runtime_fixture_' + uuid4().hex
    admin = create_engine(os.environ['DATABASE_URL'], hide_parameters=True)
    isolated = create_engine(os.environ['DATABASE_URL'], hide_parameters=True,
        connect_args={'options': '-csearch_path=' + schema})
    try:
        with admin.begin() as db:
            db.execute(text(f'CREATE SCHEMA {schema}'))
        Base.metadata.create_all(isolated)
        sessions = sessionmaker(isolated, expire_on_commit=False)
        with patch.dict(os.environ, {'EVIDENCE_SIGNING_KEY': base64.b64encode(bytes(range(32))).decode(),
                'SECRET_MASTER_KEY': base64.b64encode(bytes(range(32))).decode(),
                'REAL_INGRESS_HMAC_KEY': 'synthetic-random-fixture-hmac-key-32bytes', 'EXECUTION_ADAPTER': 'real'}):
            signer = EvidenceSigner()
            with patch.dict(os.environ, {'COMMAND_PUBLIC_KEY': signer.public['public_key']}):
                with sessions.begin() as db:
                    db.add(Tenant(id='tenant', slug='tenant', name='Fixture'))
                    db.flush()
                    db.add(Project(id='project', tenant_id='tenant', slug='project', name='Fixture'))
                    roles = ('maker', 'operator', 'data_checker', 'security_checker')
                    for role in roles:
                        db.add(Account(id=role, tenant_id='tenant', username=role, display_name=role,
                            **{'pass' + 'word_hash': 'unused'}))
                    db.flush()
                    for role in roles:
                        db.add(Membership(project_id='project', account_id=role, tenant_id='tenant', roles=[role]))
                    db.add(Candidate(id='candidate', tenant_id='tenant', project_id='project', owner_id='maker',
                        name='Fixture', plan={}, assessment={}))
                yield sessions, signer
    finally:
        isolated.dispose()
        # Only the verified UUID-named fixture schema is removed.
        assert schema.startswith('real_runtime_fixture_') and schema.replace('_', '').isalnum()
        with admin.begin() as db:
            db.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
        admin.dispose()


def seed(sessions, signer, receipt, target_database=None):
    command_id, run_id, prep_id, version_id = [str(uuid4()) for _ in range(4)]
    source = {'kind': 'mysql', 'direction': 'source', 'host': '192.0.2.1', 'port': 3306,
        'database': 'source_db', 'tables': ['source_table'], 'username': 'reader'}
    target = {'kind': 'doris', 'direction': 'target', 'host': '192.0.2.2', 'port': 9030,
        'database': target_database or 'target_' + uuid4().hex[:12], 'tables': ['approved_table'], 'username': 'probe',
        'physical_cluster_id': 'fixture-cluster', 'execution_writer': {'username': 'writer'}}
    policy = SimpleNamespace(get=lambda key: deepcopy({'source': source, 'target': target}.get(key)))
    provider = SecretProvider()
    now = datetime.now(timezone.utc)
    supervision = RuntimeSupervisionContract(max_rows=10).model_dump()
    with sessions.begin() as db:
        refs = {}
        for slot in ('source', 'probe', 'writer'):
            reference = SecretReference(id=str(uuid4()), tenant_id='tenant', project_id='project', version=1)
            reference.nonce, reference.ciphertext = provider.seal(reference, 'synthetic-only-' + slot + '-credential')
            db.add(reference)
            refs[slot] = reference
        db.flush()
        target_connection = Connection(id=str(uuid4()), tenant_id='tenant', project_id='project', name='target',
            resource_id='target', kind='doris', direction='target', resource_fingerprint=fingerprint(target),
            secret_reference_id=refs['probe'].id, secret_version=1,
            writer_secret_reference_id=refs['writer'].id, writer_secret_version=1)
        db.add(target_connection)
        db.flush()
        source_asset = {'resource_id': 'source', 'secret_reference_id': refs['source'].id, 'secret_version': 1,
            'resource_fingerprint': fingerprint(source), 'schema': {'name': 'source_table',
                'columns': [{'name': 'id', 'type': 'int', 'nullable': False, 'key': 'PRI'}]}}
        target_asset = {'resource_id': 'target', 'connection_id': target_connection.id,
            'resource_fingerprint': fingerprint(target), 'secret_reference_id': refs['probe'].id, 'secret_version': 1,
            'execution_writer': execution_writer_binding(db, target, target_connection),
            'schema': {'name': 'approved_table', 'columns': [{'name': 'id', 'type': 'int', 'nullable': False}]}}
        plan = {'source': {'table': 'source_table'}, 'target': {'table': 'approved_table'},
            'mappings': [{'source': 'id', 'target': 'id', 'operation': 'direct'}], 'quality': []}
        transport = real_execution_contract(plan, source_asset['schema'], target_asset['schema'])
        facts = {'source': source_asset, 'target': target_asset, 'plan': plan,
            'real_execution_contract': transport, 'runtime_supervision_contract': supervision}
        frozen = {'tenant_id': 'tenant', 'project_id': 'project', 'maker_id': 'maker', 'version_id': version_id,
            'tool': 'pipeline.trial', 'environment': 'local', 'risk_level': 'P2', 'created_at': now.isoformat(),
            'expires_at': (now + timedelta(hours=24)).isoformat(), 'execution_sha256': digest(facts),
            'bundle_sha256': 'b' * 64, 'artifact_hashes': {}, 'execution_facts': facts, 'plan': plan,
            'authorized_operation': {'data_plane_mutation': True}, 'budget': {'max_rows': 10},
            'runtime_supervision_contract': supervision, 'policy_version': POLICY_VERSION, 'compiler_versions': VERSIONS}
        db.add(PipelineVersion(id=version_id, tenant_id='tenant', project_id='project', owner_id='maker',
            candidate_id='candidate', candidate_revision=1, name='Fixture', execution_sha256=digest(facts),
            bundle_sha256='b' * 64, frozen={'execution_facts': deepcopy(facts), 'normalized_plan': deepcopy(plan)},
            artifacts={}, artifact_hashes={}))
        db.flush()
        prep = Preparation(id=prep_id, tenant_id='tenant', project_id='project', maker_id='maker',
            version_id=version_id, tool='pipeline.trial', environment='local', risk_level='P2',
            frozen=frozen, frozen_sha256=digest(frozen), created_at=now, expires_at=now + timedelta(hours=24))
        db.add(prep)
        db.flush()
        for slot, role in [('data', 'data_checker'), ('security', 'security_checker')]:
            db.add(ApprovalDecision(id=str(uuid4()), tenant_id='tenant', project_id='project', preparation_id=prep_id,
                slot=slot, checker_id=role, checker_name=role, decision='approve', decided_at=now,
                frozen_sha256=prep.frozen_sha256))
        run = ExecutionRun(id=run_id, tenant_id='tenant', project_id='project', preparation_id=prep_id,
            subject='operator', status='queued', accepted_at=now)
        db.add(run)
        db.flush()
        payload = {'format': COMMAND_FORMAT, 'command_id': command_id, 'run_id': run_id,
            'accepted_at': now.isoformat(), **authorization_bindings(prep, 'operator')}
        db.add(ManagedCommand(id=command_id, tenant_id='tenant', project_id='project', run_id=run_id,
            envelope=sign_envelope(signer, payload), created_at=now))
        db.flush()
        db.add(RuntimeSupervision(command_id=command_id, contract=supervision, updated_at=now))
        db.add(RunControl(command_id=command_id, facts={}))
    pin = real_runtime_pin()
    manifest = {'format': 'etl-real-network-manifest-v1', 'runtime_pin': pin,
        'runtime_overview': {'projectVersion': pin['engine']['version'], 'gitCommitAbbrev': pin['engine']['git_commit_abbrev']},
        'connector_digests': {item['artifact']: item['sha1'] for item in pin['connectors']},
        'driver_digest': pin['driver']['sha1'], 'http_connector_sha256': HTTP_CONNECTOR['sha256'],
        'resources': {'target': {'resource_fingerprint': fingerprint(target),
            'writer_resource_fingerprint': fingerprint(writer_resource(target)),
            'physical_cluster_id': target['physical_cluster_id'], 'engine_url': 'http://192.0.2.4:8080',
            'ingress_url': 'http://192.0.2.5:8080', 'backend_route': {'host': '192.0.2.3', 'port': 8040, 'backend_id': 'be-1'},
            'network_receipt_sha256': hashlib.sha256(receipt).hexdigest()}}}
    return command_id, policy, provider, manifest


class WorkerDeath(BaseException):
    pass


class Objects:
    def __init__(self, sessions):
        self.sessions, self.identities, self.creates, self.inspects = sessions, {}, 0, 0
        self.lost_reply, self.die, self.absent = False, False, False

    def factory(self, resource, credential):
        assert credential == 'synthetic-only-writer-credential'
        self.resource = resource
        return self

    def create_shadow(self, target, run_id, command_id, columns, *, before_mutation=None):
        with self.sessions() as db:
            route = 'rejected' if run_id.endswith('-error') else 'valid'
            assert db.get(RealObjectIntent, (command_id, route)).phase == 'creating'
            assert db.get(EngineAttempt, command_id) is None
        if before_mutation is not None:
            before_mutation()
        self.creates += 1
        schema = table_schema(columns)
        identity = {'physical_cluster_id': self.resource['physical_cluster_id'], 'database': self.resource['database'],
            'table': shadow_table(target, run_id), 'marker': object_marker(command_id),
            'partition_ids': [str(self.creates + 100)], 'schema': schema, 'schema_sha256': fingerprint(schema)}
        if not self.absent:
            self.identities[run_id] = identity
        if self.die:
            self.die = False
            raise WorkerDeath()
        if self.lost_reply:
            self.lost_reply = False
            raise OSError('fixture object response lost')
        return deepcopy(identity)

    def inspect_shadow(self, target, run_id, command_id):
        self.inspects += 1
        if run_id not in self.identities:
            raise OSError('fixture object absent')
        return deepcopy(self.identities[run_id])


class Engine:
    def __init__(self, sessions, command_id):
        self.sessions, self.command_id = sessions, command_id
        self.posts, self.gets, self.lost_reply, self.die = 0, 0, False, False
        self.original, self.state = None, 'RUNNING'

    def submit(self, job, identifier):
        with self.sessions() as db:
            attempt = db.get(EngineAttempt, self.command_id)
            assert attempt.phase == 'submitting' and attempt.engine_job_id == identifier
            assert attempt.request_sha256 == digest(attempt.request)
            assert db.get(IngressBinding, self.command_id) is not None
            assert len(list(db.scalars(select(LoadRun).where(LoadRun.binding['run_id'].as_string() == attempt.request['run_id'])))) == 2
            self.original = deepcopy(attempt.request)
        assert job['source'][0]['pass' + 'word'] == 'synthetic-only-source-credential'
        assert len(job['sink'][0]['headers']['x-etl-ingress-token']) == 64
        self.posts += 1
        if self.die:
            self.die = False
            raise WorkerDeath()
        if self.lost_reply:
            self.lost_reply = False
            raise OSError('fixture submit reply lost with synthetic-only-source-credential')
        return identifier

    def observe(self, identifier, name):
        self.gets += 1
        assert self.original and self.original['engine_job_id'] == identifier and self.original['engine_job_name'] == name
        return {'job_id': identifier, 'job_name': name, 'engine_state': self.state}


def verify(sessions, signer):
    passed = []
    receipt = json.dumps({'schema': 'managed-network-receipt/v1', 'resource_state': 'kept',
        'verification_phase': 'isolated_network_observed', 'engine': {'ip': '192.0.2.4', 'rest_port': 8080, 'file_encoding': 'UTF-8'},
        'gateway': {'ip': '192.0.2.5', 'ingress_port': 8080},
        'http_connector_sha256': HTTP_CONNECTOR['sha256'],
        'fixture': 'injected transport seams, not actual qualification'}, sort_keys=True, separators=(',', ':')).encode()
    with TemporaryDirectory(prefix='real-runtime-fixture-') as directory:
        receipt_path = Path(directory) / 'receipt.json'
        receipt_path.write_bytes(receipt)
        with patch.dict(os.environ, {'REAL_NETWORK_RECEIPT_FILE': str(receipt_path), 'REAL_DEPLOYMENT_MANIFEST_FILE': ''}), \
             patch('app.preparations.current_execution_facts', side_effect=lambda db, policy, version: deepcopy(version.frozen['execution_facts'])), \
             patch('app.real_runtime._external_authorization'):
            def fixture(target_database=None):
                cid, policy, provider, manifest = seed(sessions, signer, receipt, target_database)
                objects, engine = Objects(sessions), Engine(sessions, cid)
                def advance(**overrides):
                    arguments = {'sessions': sessions, 'policy': policy, 'secrets': provider, 'manifest': manifest,
                        'client_factory': lambda url: engine, 'network_verifier': lambda entry, raw: raw == receipt,
                        'objects_factory': objects.factory}
                    arguments.update(overrides)
                    return submit_real(cid, **arguments)
                return cid, manifest, objects, engine, advance

            cid, manifest, objects, engine, advance = fixture()
            assert advance(manifest=None) == 'supervision_unavailable'
            with sessions() as db:
                assert db.get(EngineAttempt, cid) is None
                assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid)) is None
            assert objects.creates == engine.posts == engine.gets == 0
            passed.append('missing_deployment_has_no_attempt_owner_or_io')

            cid, manifest, objects, engine, advance = fixture()
            assert advance(network_verifier=None) == 'supervision_unavailable'
            assert objects.creates == engine.posts == 0
            passed.append('caller_manifest_and_unknown_network_receipt_do_not_qualify')

            cid, manifest, objects, engine, advance = fixture()
            manifest['resources']['target']['network_receipt_sha256'] = '0' * 64
            assert advance() == 'supervision_unavailable' and objects.creates == engine.posts == 0
            passed.append('receipt_hash_matches_actual_proof_bytes')

            cid, manifest, objects, engine, advance = fixture()
            removed = json.loads(receipt)
            removed['resource_state'] = 'removed'
            removed_bytes = json.dumps(removed, sort_keys=True, separators=(',', ':')).encode()
            receipt_path.write_bytes(removed_bytes)
            manifest['resources']['target']['network_receipt_sha256'] = hashlib.sha256(removed_bytes).hexdigest()
            assert advance(network_verifier=lambda entry, raw: True) == 'supervision_unavailable'
            assert objects.creates == engine.posts == 0
            receipt_path.write_bytes(receipt)
            passed.append('removed_history_cannot_qualify_even_with_matching_bytes_and_injected_success')

            cid, manifest, objects, engine, advance = fixture()
            manifest['resources']['target']['engine_url'] = 'http://192.0.2.99:8080'
            assert advance() == 'supervision_unavailable' and objects.creates == engine.posts == 0
            passed.append('receipt_endpoints_must_match_manifest')

            cid, manifest, objects, engine, advance = fixture()
            with sessions.begin() as db:
                run = db.get(ExecutionRun, db.get(ManagedCommand, cid).run_id)
                request_stop(db, run, db.get(RuntimeSupervision, cid), ['operator_stop'], datetime.now(timezone.utc))
            assert advance() == 'cancelled'
            with sessions() as db:
                assert db.get(EngineAttempt, cid) is None
                assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid)) is None
            assert objects.creates == engine.posts == engine.gets == 0
            passed.append('pre_submission_stop_has_no_owner_or_io')

            cid, manifest, objects, engine, advance = fixture()
            objects.lost_reply = True
            assert advance() == 'submission_unknown' and objects.creates == 1
            assert advance() == 'submitted' and objects.creates == 2 and objects.inspects == 1
            assert engine.posts == 1
            passed.append('lost_create_reply_only_inspects_original_marker_schema_partitions')

            cid, manifest, objects, engine, advance = fixture()
            objects.absent, objects.die = True, True
            try:
                advance()
                raise AssertionError('worker did not die')
            except WorkerDeath:
                pass
            assert advance() == 'submission_unknown' and objects.creates == 1 and objects.inspects == 1
            assert advance() == 'submission_unknown' and objects.creates == 1 and objects.inspects == 2
            with sessions() as db:
                assert db.get(RealObjectIntent, (cid, 'valid')).phase == 'unknown'
                assert db.get(EngineAttempt, cid) is None
            passed.append('create_death_even_absent_never_recreates')

            for death in (False, True):
                cid, manifest, objects, engine, advance = fixture()
                engine.lost_reply, engine.die = not death, death
                if death:
                    try:
                        advance()
                        raise AssertionError('worker did not die')
                    except WorkerDeath:
                        pass
                else:
                    assert advance() == 'submission_unknown'
                assert advance() == 'submitted' and advance() == 'submitted'
                assert engine.posts == 1 and engine.gets == 2
                with sessions() as db:
                    attempt = db.get(EngineAttempt, cid)
                    assert attempt.request == engine.original and attempt.request_sha256 == digest(engine.original)
                    rows = [attempt.request, *list(db.scalars(select(EvidenceEvent.facts))),
                            *list(db.scalars(select(IngressBinding.facts))), *list(db.scalars(select(LoadRun.binding)))]
                    persisted = json.dumps(rows)
                    assert 'synthetic-only-source-credential' not in persisted
                    assert 'x-etl-ingress-token' not in persisted and 'plugin_name' not in persisted
                passed.append('submit_' + ('worker_death' if death else 'lost_reply') + '_only_gets_original_no_credentials')

            cid, manifest, objects, engine, advance = fixture()
            with patch('app.real_runtime.IngressService.bind', side_effect=WorkerDeath()):
                try:
                    advance()
                    raise AssertionError('worker did not die')
                except WorkerDeath:
                    pass
            with sessions() as db:
                attempt = db.get(EngineAttempt, cid)
                assert attempt.phase == 'prepared' and 'quality_contract_sha256' in attempt.request
                original, original_sha = deepcopy(attempt.request), attempt.request_sha256
                handles = {name: route['command_id'] for name, route in original['real_routes'].items()}
                assert len(set(handles.values())) == 2 and cid not in handles.values()
            assert advance() == 'submitted' and objects.creates == 2 and engine.posts == 1
            with sessions() as db:
                attempt = db.get(EngineAttempt, cid)
                assert attempt.request == original and attempt.request_sha256 == original_sha
                assert {name: route['command_id'] for name, route in attempt.request['real_routes'].items()} == handles
            passed.append('prepared_binding_death_keeps_route_ids_quality_sha_and_preselected_job')

            for state in ('INITIALIZING', 'SCHEDULED'):
                engine.state = state
                assert advance() == 'submitted'
            engine.state = 'UNKNOWABLE'
            assert advance() == 'submission_unknown'
            engine.state = 'RUNNING'
            assert advance() == 'submitted' and engine.posts == 1
            passed.append('original_id_name_association_accepts_initializing_scheduled_rejects_unknowable')

            target_database = 'serialized_' + uuid4().hex[:12]
            first_id, _, first_objects, first_engine, first = fixture(target_database)
            first_engine.lost_reply = True
            assert first() == 'submission_unknown'
            second_id, _, second_objects, second_engine, second = fixture(target_database)
            assert second() == 'blocked_target'
            with sessions() as db:
                assert db.get(EngineAttempt, second_id) is None
                owners = list(db.scalars(select(TargetOwnership).where(TargetOwnership.command_id == first_id)))
                assert len(owners) == 1
            assert second_objects.creates == second_engine.posts == second_engine.gets == 0
            passed.append('physical_cluster_database_original_target_owner_is_serialized_and_retained')

            cid_done, _, _, done_engine, completed = fixture()
            assert completed() == 'submitted'
            with sessions.begin() as db:
                db.get(ExecutionRun, db.get(ManagedCommand, cid_done).run_id).status = 'supervision_passed'
            with patch('app.real_runtime._authorized', side_effect=AssertionError('completed work reauthorized')), \
                 patch('app.real_runtime._gate', side_effect=AssertionError('completed work needs new witness')):
                assert completed() == 'supervision_passed'
            assert done_engine.posts == 1 and done_engine.gets == 0
            passed.append('completed_delivery_does_not_reopen_when_authority_or_network_is_unavailable')
            from app.run_control import advance_control
            with patch('app.execution_runtime.Session', sessions), \
                 patch('app.real_control.advance_real_control', return_value='supervision_passed') as dispatch:
                assert advance_control(cid_done) == 'supervision_passed'
                assert dispatch.call_args.kwargs['sessions'] is sessions
                assert dispatch.call_args.args == (cid_done,)
            passed.append('production_control_dispatch_uses_original_runtime_session_binding')

            cid_stale, _, stale_objects, stale_engine, stale = fixture()
            checks = []
            def expires_before_submit(entry, raw):
                checks.append(True)
                # Initial gate and both CREATE gates pass. The POST gate expires.
                return len(checks) <= 3
            assert stale(network_verifier=expires_before_submit) == 'submission_unknown'
            assert len(checks) == 4 and stale_objects.creates == 2 and stale_engine.posts == 0
            passed.append('fresh_network_is_rechecked_after_binding_before_the_unique_submit')

            cid_create, _, create_objects, create_engine, expires = fixture()
            checks = []
            def expires_before_create(entry, raw):
                checks.append(True)
                return len(checks) == 1
            assert expires(network_verifier=expires_before_create) == 'submission_unknown'
            assert len(checks) == 2 and create_objects.creates == create_engine.posts == 0
            passed.append('fresh_network_is_rechecked_after_resource_probe_before_create')

            # Original service may observe/close after current authority is gone.
            with sessions.begin() as db:
                command = db.get(ManagedCommand, cid)
                db.get(Account, 'operator').enabled = False
            with patch.dict(os.environ, {'REAL_DEPLOYMENT_MANIFEST_FILE': ''}):
                service = ingress_service(sessions=sessions, policy=seed_policy(), secrets=SecretProvider())
                sample = service.observe(cid)
                assert sample['complete'] is True
                assert service.close(cid)['admission_closed'] is True
            passed.append('observe_and_close_survive_role_revocation_and_missing_deployment_file')
    return passed


def seed_policy():
    # Observe/close must not read live resources; fail if their factory asks.
    def forbidden(key):
        raise AssertionError('observe/close requested live resource authorization')
    return SimpleNamespace(get=forbidden)


if __name__ == '__main__':
    with database() as (sessions, signer):
        checks = verify(sessions, signer)
    print(json.dumps({'passed': checks, 'postgresql_random_schema': True, 'real_l2_qualified': False}, ensure_ascii=False))
