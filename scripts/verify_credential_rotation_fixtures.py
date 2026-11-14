"""Credential rotation contract fixtures. Optional --postgres uses only a random schema.

Never reads runtime secret files or contacts source/target engines. Candidate probes
are replaced at the managed I/O boundary; PostgreSQL state, routes, signatures,
CAS, evidence, revocation and rollback use production code. No real L2 claim.
"""
import base64
import copy
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException, Response
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql

from app import credential_rotation as rotation
from app.command_signing import CAPABILITY_FORMAT, sign_envelope
from app.db import Base
from app.evidence_signing import EvidenceSigner
from app.execution import (CommitInput, ExecutionRun, IdempotencyRecord, ManagedCommand,
                           Outbox, ReplayGuard, register_execution_routes)
from app.execution_runtime import execution_view  # Register worker metadata before create_all.
from app import run_control  # Register control/supervision metadata used by the real run_view.
from app.managed_secrets import SecretProvider
from app.models import (Account, Connection, EvidenceEvent, IdentityEvent, Membership,
                        Project, SchemaMigration, SecretReference, Tenant)
from app.preparations import (ApprovalDecision, DecisionInput, Preparation,
                              register_preparation_routes)

SENTINEL = 'rotation-fixture-secret-do-not-expose'
RESOURCE = {'kind': 'doris', 'direction': 'target', 'host': 'fixture.invalid', 'port': 9030,
    'database': 'approved_db', 'tables': ['approved_table'], 'username': 'probe_user',
    'physical_cluster_id': 'fixture-cluster', 'execution_writer': {'username': 'writer_user'}}
POLICY = SimpleNamespace(get=lambda key: copy.deepcopy(RESOURCE) if key == 'target' else None)


def rejected(call):
    try:
        result = call()
    except (HTTPException, ValueError):
        return
    assert getattr(result, 'status_code', 200) >= 400, 'unsafe fixture accepted'


class Routes:
    def __init__(self):
        self.handlers = {}

    def __getattr__(self, method):
        def register(path, **kwargs):
            def capture(handler):
                self.handlers[(method, path)] = handler
                return handler
            return capture
        return register


def providers():
    # Test-owned keys only; never print keys or credentials.
    encoded = base64.b64encode(bytes(range(32))).decode()
    with patch.dict(os.environ, {'SECRET_MASTER_KEY': encoded, 'EVIDENCE_SIGNING_KEY': encoded}):
        return SecretProvider(), EvidenceSigner()


def offline():
    provider, _ = providers()
    candidate = rotation.CredentialRotationCandidate(preparation_id='prepare', tenant_id='tenant',
        project_id='project', secret_reference_id='new-reference', version=2)
    candidate.nonce, candidate.ciphertext = provider.seal(rotation.candidate_reference(candidate), SENTINEL)
    digest = rotation.candidate_digest(candidate)
    assert provider.open(rotation.candidate_reference(candidate)) == SENTINEL
    assert SENTINEL.encode() not in candidate.ciphertext
    candidate.ciphertext += b'changed'
    assert rotation.candidate_digest(candidate) != digest
    body = rotation.RotationInput(slot='execution_writer', credential=SENTINEL)
    assert SENTINEL not in repr(body)
    rejected(lambda: rotation.RotationInput(slot='execution_writer', credential=SENTINEL, username='expanded'))
    rejected(lambda: rotation.RotationInput(slot='unknown', credential=SENTINEL))
    ddl = str(CreateTable(Preparation.__table__).compile(dialect=postgresql.dialect()))
    assert 'preparation_version_shape' in ddl and "version_id IS NULL" in ddl
    assert 'revoked_at' in SecretReference.__table__.columns
    print(json.dumps({'passed': ['candidate encrypted with scoped reference AAD; ciphertext digest tamper detected',
        'secret input redaction; no arbitrary slot or resource expansion',
        'rotation-only nullable pipeline version schema; historical revocation retained'], 'real_l2_claim': False}))


def postgres():
    from app import migrations
    from sqlalchemy.exc import SQLAlchemyError
    name = 'rotation_fixture_' + uuid.uuid4().hex
    admin = create_engine(os.environ['DATABASE_URL'], hide_parameters=True)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA {name}'))
    isolated = create_engine(os.environ['DATABASE_URL'], hide_parameters=True,
        connect_args={'options': '-csearch_path=' + name})
    provider, signer = providers()
    routes = Routes()
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        connector_policy=POLICY, secret_provider=provider, evidence_signer=signer)))
    actors = {key: SimpleNamespace(id=key, tenant_id='tenant', display_name=key)
              for key in ('operator', 'data', 'security', 'maker')}

    def scope(db, actor, tenant_id, project_id):
        member = db.get(Membership, (project_id, actor.id))
        if tenant_id != actor.tenant_id or member is None:
            raise HTTPException(403, 'fixture scope denied')
        return member

    register_preparation_routes(routes, object, object, scope)
    register_execution_routes(routes, object, object, scope)
    rotation.register_credential_rotation_routes(routes, object, object, scope)
    prefix = '/api/tenants/{tenant_id}/projects/{project_id}'
    prepare = routes.handlers[('post', prefix + '/connections/{connection_id}/credential-rotations')]
    decide = routes.handlers[('post', prefix + '/preparations/{preparation_id}/decisions')]
    issue = routes.handlers[('post', prefix + '/preparations/{preparation_id}/capabilities')]
    commit = routes.handlers[('post', prefix + '/preparations/{preparation_id}/commit')]

    def prepare_one(slot='execution_writer', who='operator'):
        with Session(isolated) as db, db.begin():
            return prepare('tenant', 'project', 'target-ref', rotation.RotationInput(slot=slot, credential=SENTINEL),
                           request, db, actors[who])

    def approval(key, slot, who):
        with Session(isolated) as db, db.begin():
            return decide('tenant', 'project', key, DecisionInput(slot=slot, decision='approve'),
                          request, db, actors[who])

    def capability(key):
        with Session(isolated) as db, db.begin():
            result = issue('tenant', 'project', key, request, db, actors['operator'])
            if isinstance(result, dict):
                assert result['capability']['payload']['expires_at'] - result['capability']['payload']['issued_at'] == 300
            return result

    def apply(key, envelope, idem):
        with Session(isolated) as db, db.begin():
            return commit('tenant', 'project', key, CommitInput(capability=envelope, idempotency_key=idem),
                          request, Response(), db, actors['operator'])

    def counts():
        with Session(isolated) as db:
            return [db.scalar(select(func.count()).select_from(model)) for model in
                    (SecretReference, ExecutionRun, ManagedCommand, ReplayGuard, IdempotencyRecord, Outbox,
                     IdentityEvent, EvidenceEvent)]

    try:
        with isolated.begin() as connection:
            Base.metadata.create_all(connection)
            # Simulate pre-V20 schema, then prove additive migration is idempotent.
            connection.execute(text('DROP TABLE credential_rotation_candidates'))
            connection.execute(text('ALTER TABLE secret_references DROP COLUMN revoked_at'))
            connection.execute(text('ALTER TABLE preparations DROP CONSTRAINT preparation_version_shape, '
                                    'ALTER COLUMN version_id SET NOT NULL'))
        with patch.object(migrations, 'engine', isolated):
            migrations.initialize_database()
            migrations.initialize_database()
        with Session(isolated) as db, db.begin():
            assert db.get(SchemaMigration, 'v20-credential-rotation')
            db.add(Tenant(id='tenant', slug='tenant', name='Fixture'))
            db.flush()
            db.add(Project(id='project', tenant_id='tenant', slug='project', name='Fixture'))
            for key in actors:
                db.add(Account(id=key, tenant_id='tenant', username=key, display_name=key,
                               **{'pass' + 'word_hash': 'unused'}))
            db.flush()
            for key, roles in [('operator', ['operator', 'data_checker', 'security_checker']),
                               ('data', ['data_checker', 'security_checker']),
                               ('security', ['security_checker']), ('maker', ['maker'])]:
                db.add(Membership(tenant_id='tenant', project_id='project', account_id=key, roles=roles))
            for key in ('probe', 'writer'):
                reference = SecretReference(id=key, tenant_id='tenant', project_id='project', version=1)
                reference.nonce, reference.ciphertext = provider.seal(reference, 'old-fixture-value')
                db.add(reference)
            db.flush()
            db.add(Connection(id='target-ref', tenant_id='tenant', project_id='project', name='Fixture',
                resource_id='target', kind='doris', direction='target', resource_fingerprint=rotation.fingerprint(RESOURCE),
                secret_reference_id='probe', secret_version=1, writer_secret_reference_id='writer', writer_secret_version=1))
        # Only external I/O is mocked; authorization, signature and transaction remain real.
        with patch.object(rotation, 'inspect_execution_writer', return_value={}), \
                patch.object(rotation, 'inspect_resource', return_value={}):
            rejected(lambda: prepare_one(who='maker'))
            prepared = prepare_one()
            key = prepared['id']
            assert prepared['version_id'] is None and prepared['validation']['state'] == 'pending'
            assert prepared['validation']['current_eligibility']['operator']
            assert SENTINEL not in json.dumps(prepared, default=str)
            with Session(isolated) as db:
                row = db.get(Preparation, key)
                drifted = SimpleNamespace(get=lambda resource_id: {**RESOURCE, 'host': 'changed.invalid'})
                assert not rotation.validate_rotation_preparation(db, drifted, row, require_approved=False)['valid']
                assert not rotation.validate_rotation_preparation(db, POLICY, row, require_approved=False,
                    now=row.expires_at + timedelta(seconds=1))['valid']
                member = db.get(Membership, ('project', 'operator'))
                member.roles = []
                db.flush()
                assert not rotation.validate_rotation_preparation(db, POLICY, row, require_approved=False)['valid']
                db.rollback()
            rejected(lambda: approval(key, 'data', 'operator'))
            rejected(lambda: capability(key))
            assert approval(key, 'data', 'data')['validation']['state'] == 'pending'
            rejected(lambda: approval(key, 'security', 'data'))
            assert approval(key, 'security', 'security')['validation']['state'] == 'approved'
            issued = capability(key)['capability']
            altered = copy.deepcopy(issued)
            altered['payload']['resources']['target']['slot'] = 'probe_readonly'
            rejected(lambda: apply(key, altered, 'tamper'))
            expired = copy.deepcopy(issued['payload'])
            expired['issued_at'], expired['expires_at'] = 1, 301
            rejected(lambda: apply(key, sign_envelope(signer, expired), 'expired'))
            stale = prepare_one()  # Frozen old writer must fail after first rotation.
            before = counts()
            # Force PostgreSQL failure after CAS, run/command and replay/outbox flush.
            with isolated.begin() as connection:
                connection.execute(text("CREATE FUNCTION fail_rotation_evidence() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    "BEGIN IF NEW.action='credential_rotation.completed' THEN RAISE EXCEPTION 'fixture injected failure'; "
                    "END IF; RETURN NEW; END $$"))
                connection.execute(text('CREATE TRIGGER rotation_failure BEFORE INSERT ON identity_events '
                    'FOR EACH ROW EXECUTE FUNCTION fail_rotation_evidence()'))
            try:
                apply(key, issued, 'atomic-retry')
            except SQLAlchemyError:
                pass
            else:
                raise AssertionError('injected PostgreSQL failure did not abort')
            assert counts() == before, 'partial database/evidence success escaped rollback'
            with Session(isolated) as db:
                assert db.get(Connection, 'target-ref').writer_secret_reference_id == 'writer'
                assert db.get(SecretReference, 'writer').revoked_at is None
            with isolated.begin() as connection:
                connection.execute(text('DROP TRIGGER rotation_failure ON identity_events'))
            result = apply(key, issued, 'atomic-retry')
            assert result['status'] == 'succeeded' and result['outbox']['status'] == 'applied_inline'
            assert apply(key, issued, 'atomic-retry')['id'] == result['id']
            rejected(lambda: apply(key, issued, 'replay'))
            with Session(isolated) as db:
                connection = db.get(Connection, 'target-ref')
                assert connection.writer_secret_version == 2 and connection.writer_secret_reference_id != 'writer'
                assert connection.secret_reference_id == 'probe' and connection.secret_version == 1
                assert db.get(SecretReference, 'writer').revoked_at is not None
                assert db.get(SecretReference, 'probe').revoked_at is None
                assert provider.open(db.get(SecretReference, connection.writer_secret_reference_id)) == SENTINEL
                assert rotation.validate_rotation_preparation(db, POLICY, db.get(Preparation, stale['id']))['state'] == 'invalidated'
                from app.run_control import check_bound_secrets
                old = SimpleNamespace(tenant_id='tenant', project_id='project', envelope={'payload': {'resources': {
                    'source': {}, 'target': {'execution_writer': {'secret_reference_id': 'writer', 'secret_version': 1}}}}})
                with patch('app.run_control.SecretProvider', return_value=provider):
                    rejected(lambda: check_bound_secrets(db, old))
            probe_prepare = prepare_one('probe_readonly')
            probe_id = probe_prepare['id']
            approval(probe_id, 'data', 'data')
            approval(probe_id, 'security', 'security')
            probe_result = apply(probe_id, capability(probe_id)['capability'], 'rotate-probe')
            assert probe_result['status'] == 'succeeded'
            with Session(isolated) as db:
                connection = db.get(Connection, 'target-ref')
                assert connection.secret_version == 2 and connection.secret_reference_id != 'probe'
                assert connection.writer_secret_version == 2
                assert db.get(SecretReference, 'probe').revoked_at is not None
                assert SENTINEL not in json.dumps([e.facts for e in db.scalars(select(EvidenceEvent))])
        print(json.dumps({'passed': ['V20 additive migration twice on pre-V20 schema',
            'Operator without Maker prepares; non-Operator denied; self/double-slot denied',
            'two named approvals and 300s signed capability; tamper/expiry denied',
            'PostgreSQL injected failure rolls back secret/CAS/revocation/run/command/replay/idempotency/outbox/evidence',
            'same request retries once; replay denied; writer and probe slots rotate independently',
            'new reference and incrementing version; old reference revoked; old Prepare and bound secret denied',
            'views and persisted evidence never contain sentinel'], 'real_l2_claim': False}))
    finally:
        isolated.dispose()
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA {name} CASCADE'))
        admin.dispose()


if __name__ == '__main__':
    offline()
    if '--postgres' in sys.argv:
        postgres()
