"""Resolver fixtures using signed envelopes and an isolated PostgreSQL schema.

Run with backend on PYTHONPATH and a reachable PostgreSQL DATABASE_URL. Uses real
Ed25519 envelopes, AESGCM references, preparation validation, roles and locks.
Metadata reassessment and Doris reads remain seams: no network or real-L2 claim.
The random schema is removed on success and failure; public is never written.
"""
import base64
from contextlib import contextmanager
import copy
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import json
import os
from types import SimpleNamespace as Row
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import HTTPException

from app import preparations
from app.command_signing import COMMAND_FORMAT, sign_envelope
from app.connector_policy import fingerprint
from app.doris_objects import object_marker, table_schema
from app.engine_adapter import job_name
from app.execution import ExecutionRun, ManagedCommand, authorization_bindings
from app.execution_runtime import EngineAttempt
from app.managed_load import FrozenLoadBinding, LoadRun
from app.managed_secrets import SecretProvider
from app.models import Account, Connection, Membership, SecretReference
from app.preparations import ApprovalDecision, Preparation, POLICY_VERSION
from app.quality_runtime import QualityRun
from app.real_contract import real_execution_contract
from app.real_engine import shadow_table
from app.real_ingress import IngressBindingFacts
from app.real_resources import RealResourceResolver, _sha
from app.studio_plan import VERSIONS
from app.versions import PipelineVersion
from app.writer_credentials import execution_writer_binding, writer_resource


class Memory:
    def __init__(self):
        self.rows, self.events = {}, []

    def add(self, kind, row):
        self.rows.setdefault(kind, []).append(row)
        return row

    def get(self, kind, key, **kwargs):
        return next((x for x in self.rows.get(kind, []) if getattr(x, 'id', getattr(x, 'command_id',
                    getattr(x, 'run_id', None))) == key), None)

    def refresh(self, row):
        self.events.append(('refresh', type(row).__name__))

    def scalars(self, query):
        kind = query.column_descriptions[0]['entity']
        def matches(row):
            return all(getattr(row, term.left.name) == term.right.value for term in query._where_criteria)
        return [row for row in self.rows.get(kind, []) if matches(row)]

    def scalar(self, query):
        return next(iter(self.scalars(query)), None)

    @contextmanager
    def begin(self):
        self.events.append(('begin',))
        yield self
        self.events.append(('end',))


def rejected(call, code=None):
    try:
        call()
    except (ValueError, HTTPException) as error:
        if code:
            assert str(error) == code, (code, str(error))
        return
    raise AssertionError('unsafe resolver fixture accepted')


def run():
    private = Ed25519PrivateKey.generate()
    public = base64.b64encode(private.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw)).decode()
    import hashlib
    signer = Row(key=private, public={'public_key': public,
                 'key_id': hashlib.sha256(base64.b64decode(public)).hexdigest()})
    environment = {'EXECUTION_ADAPTER': 'real', 'COMMAND_PUBLIC_KEY': public,
                   'SECRET_MASTER_KEY': base64.b64encode(bytes(range(32))).decode()}
    with patch.dict(os.environ, environment):
        db = Memory()
        provider = SecretProvider()
        resource = {'kind': 'doris', 'direction': 'target', 'host': '192.0.2.2', 'port': 9030,
            'database': 'approved_db', 'tables': ['approved_table'], 'username': 'probe',
            'physical_cluster_id': 'fixture-cluster', 'execution_writer': {'username': 'writer'}}
        policy = Row(get=lambda key: copy.deepcopy(resource) if key == 'target' else None)
        for reference_id in ('probe-ref', 'writer-ref', 'source-ref'):
            ref = db.add(SecretReference, Row(id=reference_id, tenant_id='tenant', project_id='project',
                version=1, revoked_at=None))
            ref.nonce, ref.ciphertext = provider.seal(ref, 'synthetic-only-' + reference_id)
        connection = db.add(Connection, Row(id='target-connection', tenant_id='tenant', project_id='project',
            resource_id='target', resource_fingerprint=fingerprint(resource), kind='doris', direction='target',
            secret_reference_id='probe-ref', secret_version=1, writer_secret_reference_id='writer-ref',
            writer_secret_version=1))
        target = {'resource_id': 'target', 'connection_id': connection.id,
            'resource_fingerprint': fingerprint(resource), 'secret_reference_id': 'probe-ref', 'secret_version': 1,
            'execution_writer': execution_writer_binding(db, resource, connection),
            'schema': {'name': 'approved_table', 'columns': [{'name': 'id', 'type': 'int(11)', 'nullable': False}]}}
        source = {'resource_id': 'source', 'secret_reference_id': 'source-ref', 'secret_version': 1,
            'schema': {'columns': [{'name': 'id', 'type': 'bigint', 'nullable': False, 'key': 'PRI'}]}}
        plan = {'source': {'table': 'source_table'}, 'target': {'table': 'approved_table'},
                'mappings': [{'source': 'id', 'target': 'id'}], 'quality': []}
        transport = real_execution_contract(plan, source['schema'], target['schema'])
        facts = {'source': source, 'target': target, 'plan': plan, 'real_execution_contract': transport}
        now = datetime.now(timezone.utc)
        prep = db.add(Preparation, Row(id='prep', tenant_id='tenant', project_id='project', maker_id='maker',
            version_id='version', tool='pipeline.trial', environment='local', risk_level='P2',
            created_at=now, expires_at=now + timedelta(hours=24)))
        version = db.add(PipelineVersion, Row(id='version', tenant_id='tenant', project_id='project',
            execution_sha256=_sha(facts), bundle_sha256='b' * 64, artifact_hashes={}, artifacts={},
            frozen={'execution_facts': copy.deepcopy(facts), 'normalized_plan': copy.deepcopy(plan)}))
        prep.frozen = {key: getattr(prep, key) for key in ('tenant_id', 'project_id', 'maker_id',
            'version_id', 'tool', 'environment', 'risk_level')}
        prep.frozen.update(created_at=prep.created_at.isoformat(), expires_at=prep.expires_at.isoformat(),
            execution_sha256=version.execution_sha256, bundle_sha256=version.bundle_sha256,
            artifact_hashes={}, execution_facts=copy.deepcopy(facts), plan=plan,
            authorized_operation={'data_plane_mutation': True}, budget={'max_rows': 2},
            runtime_supervision_contract={'version': 'fixture-v1', 'max_rows': 2},
            policy_version=POLICY_VERSION, compiler_versions=VERSIONS)
        prep.frozen_sha256 = _sha(prep.frozen)
        for who, role in [('operator', 'operator'), ('maker', 'maker'), ('data', 'data_checker'),
                          ('security', 'security_checker')]:
            db.add(Account, Row(id=who, tenant_id='tenant', enabled=True))
            db.add(Membership, Row(id=who, account_id=who, tenant_id='tenant', project_id='project', roles=[role]))
        for slot in ('data', 'security'):
            db.add(ApprovalDecision, Row(id=slot, tenant_id='tenant', project_id='project', preparation_id=prep.id,
                slot=slot, checker_id=slot, decision='approve', frozen_sha256=prep.frozen_sha256))
        runrow = db.add(ExecutionRun, Row(id='root-run', tenant_id='tenant', project_id='project',
            preparation_id=prep.id, subject='operator', accepted_at=now))
        payload = {'format': COMMAND_FORMAT, 'command_id': 'root-command', 'run_id': runrow.id,
            'accepted_at': now.isoformat(), **authorization_bindings(prep, runrow.subject)}
        command = db.add(ManagedCommand, Row(id='root-command', tenant_id='tenant', project_id='project',
            run_id=runrow.id, envelope=sign_envelope(signer, payload)))
        backend = {'host': '192.0.2.3', 'port': 8040, 'backend_id': 'be-1'}
        entry = {'resource_fingerprint': fingerprint(resource), 'writer_resource_fingerprint': fingerprint(writer_resource(resource)),
            'physical_cluster_id': resource['physical_cluster_id'], 'backend_route': backend,
            'engine_url': 'http://192.0.2.4:8080', 'ingress_url': 'http://192.0.2.5:8080',
            'network_receipt_sha256': 'a' * 64}
        manifest = {'format': 'etl-real-network-manifest-v1', 'resources': {'target': entry}}
        routes = {}
        for name in ('valid', 'rejected'):
            objectrun = runrow.id if name == 'valid' else runrow.id + '-error'
            schema = table_schema(transport[name + '_schema'])
            if name == 'valid':
                schema[0]['type'] = 'int'  # Doris may omit integer display width.
            identity = {'physical_cluster_id': resource['physical_cluster_id'], 'database': resource['database'],
                'table': shadow_table('approved_table', objectrun), 'marker': object_marker(command.id),
                'partition_ids': ['123'], 'schema': schema, 'schema_sha256': fingerprint(schema)}
            routes[name] = {'command_id': 'route-' + name, 'object_identity': identity, 'target_table': 'approved_table',
                'object_run_id': objectrun, 'object_command_id': command.id, 'writer_secret_reference_id': 'writer-ref',
                'writer_secret_version': 1, 'resource_fingerprint': fingerprint(resource),
                'physical_cluster_id': resource['physical_cluster_id'], 'backend_route': copy.deepcopy(backend)}
        contract = {'plan': plan, 'primary_key_fields': ['id'],
            'route_commands': {name: route['command_id'] for name, route in routes.items()},
            'stage_version': 'quality-v1', 'cleaning': [], 'filters': []}
        quality = db.add(QualityRun, Row(run_id=runrow.id, contract=contract, contract_sha256=_sha(contract)))
        request = {key: payload[key] for key in ('tool', 'environment', 'bundle_sha256', 'artifact_hashes',
                    'budget', 'authorized_operation', 'runtime_supervision_contract', 'real_execution_contract')}
        request.update(command_id=command.id, run_id=runrow.id, tenant_id='tenant', project_id='project',
            adapter='real', correlation_id='correlation', engine_job_id='1001', engine_job_name=job_name(command.id),
            engine_url=entry['engine_url'], ingress_url=entry['ingress_url'], real_manifest_sha256=_sha(manifest),
            real_routes_version='etl-real-routes-v1', real_routes=routes)
        attempt = db.add(EngineAttempt, Row(command_id=command.id, correlation_id='correlation',
            engine_job_id='1001', request=request, request_sha256=_sha(request)))
        bindings = {}
        for name, route in routes.items():
            identity = route['object_identity']
            binding = FrozenLoadBinding(route['command_id'], runrow.id, '1001', _sha(command.envelope),
                _sha(payload['runtime_supervision_contract']), resource['physical_cluster_id'], resource['database'],
                identity['table'], fingerprint(identity), 'writer-ref', 1, ('be-1',), quality.contract_sha256)
            db.add(LoadRun, Row(command_id=binding.command_id, binding=json.loads(json.dumps(asdict(binding))),
                binding_sha256=_sha(json.loads(json.dumps(asdict(binding))))))
            bindings[name] = binding
        ingress = IngressBindingFacts(command.id, runrow.id, '1001', contract['route_commands'],
                                      quality.contract_sha256, ('id',))
        inspected = []
        live = {name: copy.deepcopy(route['object_identity']) for name, route in routes.items()}
        class Objects:
            def __init__(self, writer, secret):
                assert writer['username'] == 'writer' and secret == 'synthetic-only-writer-ref'
            def inspect_shadow(self, target_table, object_run, object_command):
                inspected.append((target_table, object_run, object_command))
                return copy.deepcopy(live['valid' if object_run == runrow.id else 'rejected'])
        resolver = RealResourceResolver(sessions=db, policy=policy, secrets=provider, manifest=lambda: manifest)
        with patch('app.real_resources.lock_tenant', side_effect=lambda db, tenant: db.events.append(('tenant', tenant))), \
             patch.object(preparations, 'current_execution_facts', side_effect=lambda *_: copy.deepcopy(facts)), \
             patch('app.real_resources.DorisObjects', Objects):
            resolver.authorize(ingress)
            resolved = resolver.resolve(bindings['valid'])
            assert resolved[0]['username'] == 'writer' and resolved[2] == live['valid'] and resolved[3] == backend
            resolved = resolver.resolve(bindings['rejected'])
            assert resolved[2] == live['rejected'] and resolved[0]['username'] == 'writer'
            assert inspected == [('approved_table', 'root-run', 'root-command'),
                                 ('approved_table', 'root-run-error', 'root-command')]
            rejected(lambda: resolver.authorize(replace(ingress, engine_job_id='1002')), 'real_ingress_binding_mismatch')
            rejected(lambda: resolver.authorize({**asdict(ingress), 'passed': True}), 'real_ingress_shape_invalid')
            # Rehash caller-mutated requests to exercise binding checks, not just integrity checks.
            def changed_request(key, value):
                original, digest = attempt.request, attempt.request_sha256
                attempt.request = {**original, key: value}
                attempt.request_sha256 = _sha(attempt.request)
                try: rejected(lambda: resolver.authorize(ingress))
                finally: attempt.request, attempt.request_sha256 = original, digest
            for key, value in [('real_routes', None), ('engine_job_name', 'other'), ('engine_job_id', '1002'),
                               ('engine_url', 'http://192.0.2.8'), ('real_manifest_sha256', '0' * 64),
                               ('runtime_supervision_contract', {}), ('tenant_id', 'other')]:
                changed_request(key, value)
            changed_request('real_execution_contract', {})
            for name in routes:
                for datatype in (('bigint', 'int unsigned', 'varchar(64)', 'text') if name == 'valid'
                                 else ('bigint', 'int unsigned', 'varchar(63)', 'text')):
                    altered = copy.deepcopy(routes)
                    schema = altered[name]['object_identity']['schema']
                    schema[0]['type'] = datatype
                    altered[name]['object_identity']['schema_sha256'] = fingerprint(schema)
                    changed_request('real_routes', altered)
                for field, value in [('object_run_id', runrow.id + '-caller'),
                                     ('object_command_id', 'caller'), ('target_table', 'other')]:
                    altered = copy.deepcopy(routes)
                    altered[name][field] = value
                    changed_request('real_routes', altered)
            for name in routes:
                for field, value in [('marker', 'other'), ('partition_ids', ['456']), ('schema', [])]:
                    previous = live[name][field]
                    live[name][field] = value
                    rejected(lambda: resolver.resolve(bindings[name]), 'real_object_identity_changed')
                    live[name][field] = previous
            for role in ('operator', 'maker', 'data', 'security'):
                member = db.get(Membership, role)
                previous = member.roles
                member.roles = []
                rejected(lambda: resolver.authorize(ingress))
                member.roles = previous
            for reference_id in ('writer-ref', 'probe-ref', 'source-ref'):
                ref = db.get(SecretReference, reference_id)
                ref.revoked_at = now
                rejected(lambda: resolver.authorize(ingress))
                ref.revoked_at = None
            with patch.object(connection, 'writer_secret_version', 2):
                rejected(lambda: resolver.resolve(bindings['valid']))
            with patch.object(quality, 'contract', {**contract, 'cleaning': [{'field': 'id', 'kind': 'trim'}]}):
                rejected(lambda: resolver.authorize(ingress), 'real_quality_contract_mismatch')
            with patch.object(prep, 'frozen_sha256', '0' * 64):
                rejected(lambda: resolver.authorize(ingress), 'real_signed_binding_mismatch')
            with patch.object(command, 'envelope', {**command.envelope, 'signature': 'broken'}):
                rejected(lambda: resolver.authorize(ingress))
            with patch.object(db.get(SecretReference, 'writer-ref'), 'project_id', 'other'):
                rejected(lambda: resolver.resolve(bindings['valid']))
            for key, value in [('writer_version', 2), ('object_id', '0' * 64), ('backend_ids', ('other',))]:
                rejected(lambda: resolver.resolve(replace(bindings['valid'], **{key: value})),
                         'real_load_binding_mismatch')
            # Live metadata mismatch reaches actual validate_preparation.
            previous = facts['target']['resource_fingerprint']
            facts['target']['resource_fingerprint'] = '0' * 64
            rejected(lambda: resolver.authorize(ingress), 'real_preparation_invalid')
            facts['target']['resource_fingerprint'] = previous
            assert db.events[0:2] == [('begin',), ('tenant', 'tenant')]
        print('real resource resolver offline seams passed; network_verified=false; real_l2_qualified=false')
        return {'rows': copy.deepcopy(db.rows), 'environment': environment, 'manifest': manifest,
                'facts': copy.deepcopy(facts), 'bindings': bindings, 'ingress': ingress,
                'live': live, 'signer': signer, 'resource': resource, 'provider': provider}


def postgres(fixture):
    """Real persistence/locks/role and frozen-reference validation; no external I/O."""
    from uuid import uuid4
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import Project, Tenant
    from app.studio import Candidate

    schema = 'real_resources_fixture_' + uuid4().hex
    admin = create_engine(os.environ['DATABASE_URL'], hide_parameters=True)
    isolated = create_engine(os.environ['DATABASE_URL'], hide_parameters=True,
                             connect_args={'options': '-csearch_path=' + schema})
    sessions = sessionmaker(isolated, expire_on_commit=False)
    now = datetime.now(timezone.utc)
    try:
        with admin.begin() as db:
            db.execute(text(f'CREATE SCHEMA {schema}'))
        with isolated.begin() as db:
            assert db.scalar(text('SELECT current_schema()')) == schema
            Base.metadata.create_all(db)
        with sessions.begin() as db:
            db.add(Tenant(id='tenant', slug='fixture', name='Fixture'))
            db.flush()
            db.add(Project(id='project', tenant_id='tenant', slug='fixture', name='Fixture'))
            db.flush()
            rows = fixture['rows']
            # Insert the dependency graph using actual models and PostgreSQL FKs.
            by_table = {kind.__tablename__: kind for kind in rows}
            by_table[Candidate.__tablename__] = Candidate
            for table in Base.metadata.sorted_tables:
                kind = by_table.get(table.name)
                if kind is None:
                    continue
                if kind is Candidate:
                    db.add(Candidate(id='candidate', tenant_id='tenant', project_id='project',
                        owner_id='maker', name='Fixture', plan=fixture['facts']['plan'], assessment={}))
                for row in rows.get(kind, []):
                    values = {key: value for key, value in vars(row).items() if key in table.columns}
                    if kind is Account:
                        values.update(username=row.id, display_name=row.id, **{'pass' + 'word_hash': 'unused'})
                    elif kind is Connection:
                        values.update(name='Fixture target')
                    elif kind is PipelineVersion:
                        values.update(owner_id='maker', candidate_id='candidate', candidate_revision=1, name='Fixture')
                    elif kind is ApprovalDecision:
                        values.update(checker_name=row.checker_id, decided_at=now)
                    elif kind is ManagedCommand:
                        values.update(created_at=now)
                    elif kind is EngineAttempt:
                        values.update(phase='running', created_at=now, updated_at=now)
                    elif kind is LoadRun:
                        values.update(counter_epoch='fixture-epoch', created_at=now, last_observed_at=now)
                    db.add(kind(**values))
                db.flush()

        live, inspected = copy.deepcopy(fixture['live']), []
        class Objects:
            def __init__(self, resource, secret):
                assert resource['username'] == 'writer' and secret == 'synthetic-only-writer-ref'
            def inspect_shadow(self, target, object_run, command):
                inspected.append((target, object_run, command))
                return copy.deepcopy(live['valid' if object_run == 'root-run' else 'rejected'])
        resolver = RealResourceResolver(sessions=sessions, policy=Row(get=lambda key:
            copy.deepcopy(fixture['resource']) if key == 'target' else None),
            secrets=fixture['provider'], manifest=fixture['manifest'])

        @contextmanager
        def changed(kind, key, **values):
            with sessions.begin() as db:
                row = db.get(kind, key)
                previous = {field: copy.deepcopy(getattr(row, field)) for field in values}
                for field, value in values.items():
                    setattr(row, field, value)
            try:
                yield
            finally:
                with sessions.begin() as db:
                    row = db.get(kind, key)
                    for field, value in previous.items():
                        setattr(row, field, value)

        ingress, bindings = fixture['ingress'], fixture['bindings']
        checks = []
        with patch.dict(os.environ, fixture['environment']), \
             patch.object(preparations, 'current_execution_facts', side_effect=lambda *_: copy.deepcopy(fixture['facts'])), \
             patch('app.real_resources.DorisObjects', Objects):
            # New resolver transaction for every callback; no fake Session or locks.
            resolver.authorize(ingress)
            for name in bindings:
                assert resolver.resolve(bindings[name])[2] == live[name]
            assert inspected == [('approved_table', 'root-run', 'root-command'),
                                 ('approved_table', 'root-run-error', 'root-command')]
            checks.append('both signed routes resolve through persisted bindings')
            for who in ('operator', 'maker', 'data', 'security'):
                with changed(Membership, ('project', who), roles=[]):
                    rejected(lambda: resolver.authorize(ingress))
                    for binding in bindings.values():
                        rejected(lambda: resolver.resolve(binding))
            checks.append('current operator/maker/checker roles required on every callback')
            for key in ('probe-ref', 'writer-ref', 'source-ref'):
                with changed(SecretReference, key, revoked_at=now):
                    rejected(lambda: resolver.authorize(ingress))
                    for binding in bindings.values():
                        rejected(lambda: resolver.resolve(binding))
            checks.append('revoked source/probe/writer references deny both routes')
            with changed(Connection, 'target-connection', writer_secret_reference_id='source-ref'):
                for binding in bindings.values():
                    rejected(lambda: resolver.resolve(binding))
            checks.append('current writer-slot rotation cannot authorize an old frozen binding')
            with sessions() as db:
                version_frozen = copy.deepcopy(db.get(PipelineVersion, 'version').frozen)
                prep_frozen = copy.deepcopy(db.get(Preparation, 'prep').frozen)
                envelope = copy.deepcopy(db.get(ManagedCommand, 'root-command').envelope)
                request = copy.deepcopy(db.get(EngineAttempt, 'root-command').request)
                quality = copy.deepcopy(db.get(QualityRun, 'root-run').contract)
            for field in ('execution_facts', 'normalized_plan'):
                with changed(PipelineVersion, 'version', frozen={**version_frozen, field: {}}):
                    rejected(lambda: resolver.authorize(ingress), 'real_version_frozen_mismatch')
            checks.append('current version facts and plan must equal original preparation freeze')
            for field in ('version', 'valid_schema', 'rejected_schema', 'cleaning', 'filters'):
                altered = copy.deepcopy(envelope['payload'])
                altered['real_execution_contract'][field] = 'caller-passed'
                signed = sign_envelope(fixture['signer'], altered)
                with changed(ManagedCommand, 'root-command', envelope=signed):
                    rejected(lambda: resolver.authorize(ingress), 'real_signed_binding_mismatch')
            checks.append('even newly signed caller contracts cannot replace the approved freeze')
            for field, value in [('version', 'caller-v1'), ('rejected_schema', []), ('valid_schema', []),
                                 ('valid_object_run_suffix', '-caller'), ('rejected_object_run_suffix', '-caller')]:
                frozen = copy.deepcopy(prep_frozen)
                frozen['execution_facts']['real_execution_contract'][field] = value
                frozen['execution_sha256'] = _sha(frozen['execution_facts'])
                altered = {**envelope['payload'], 'frozen_sha256': _sha(frozen),
                    'execution_sha256': frozen['execution_sha256'],
                    'real_execution_contract': frozen['execution_facts']['real_execution_contract']}
                with changed(Preparation, 'prep', frozen=frozen, frozen_sha256=_sha(frozen)), \
                     changed(ManagedCommand, 'root-command', envelope=sign_envelope(fixture['signer'], altered)):
                    rejected(lambda: resolver.authorize(ingress), 'real_execution_contract_mismatch')
                    for binding in bindings.values():
                        rejected(lambda: resolver.resolve(binding), 'real_execution_contract_mismatch')
            checks.append('rehashing the freeze and resigning a forged transport cannot bypass contract rederivation')
            for field, value in [('stage_version', 'quality-v2'), ('cleaning', [{'field': 'id', 'kind': 'trim'}]),
                                 ('filters', [{'field': 'id', 'equals': 1}])]:
                altered = {**quality, field: value}
                with changed(QualityRun, 'root-run', contract=altered, contract_sha256=_sha(altered)):
                    rejected(lambda: resolver.authorize(ingress), 'real_quality_contract_mismatch')
            checks.append('quality-v1 with empty cleaning and filters is required after digest recomputation')
            for name in bindings:
                for field, value in [('object_run_id', 'root-run-caller'), ('object_command_id', 'caller'),
                                     ('target_table', 'other-table'), ('writer_secret_reference_id', 'source-ref')]:
                    altered = copy.deepcopy(request)
                    altered['real_routes'][name][field] = value
                    with changed(EngineAttempt, 'root-command', request=altered, request_sha256=_sha(altered)):
                        rejected(lambda: resolver.resolve(bindings[name]), 'real_route_binding_mismatch')
                for field, value in [('type', 'text'), ('name', 'caller_column'), ('nullable', True)]:
                    altered = copy.deepcopy(request)
                    identity = altered['real_routes'][name]['object_identity']
                    identity['schema'][0][field] = value
                    identity['schema_sha256'] = fingerprint(identity['schema'])
                    with changed(EngineAttempt, 'root-command', request=altered, request_sha256=_sha(altered)):
                        rejected(lambda: resolver.resolve(bindings[name]), 'real_object_schema_mismatch')
                for field, value in [('marker', 'caller'), ('partition_ids', ['other']), ('schema', [])]:
                    previous = live[name][field]
                    live[name][field] = value
                    rejected(lambda: resolver.resolve(bindings[name]), 'real_object_identity_changed')
                    live[name][field] = previous
            checks.append('both routes bind signed schema, original command marker, exact suffix, target and writer')
            checks.append('live object marker, partitions and raw schema still match frozen identity exactly')
            for field, value in [('real_manifest_sha256', '0' * 64), ('real_routes_version', 'old'),
                                 ('real_execution_contract', {}), ('real_routes', None)]:
                altered = {**request, field: value}
                with changed(EngineAttempt, 'root-command', request=altered, request_sha256=_sha(altered)):
                    rejected(lambda: resolver.authorize(ingress))
            checks.append('missing qualification manifest/routes/transport contract remains closed')
            resolver.authorize(ingress)
            for binding in bindings.values():
                resolver.resolve(binding)
        print(json.dumps({'postgres_schema': schema, 'passed': checks,
                          'network_verified': False, 'real_l2_qualified': False}, ensure_ascii=False))
    finally:
        isolated.dispose()
        with admin.begin() as db:
            db.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
            assert db.scalar(text('SELECT to_regnamespace(:schema)'), {'schema': schema}) is None
        admin.dispose()
    print('real resource resolver PostgreSQL random schema removed')


if __name__ == '__main__':
    postgres(run())
