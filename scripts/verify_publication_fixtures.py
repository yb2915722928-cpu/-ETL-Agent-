"""Real-PG publication recovery fixture; mock object observer, NOT real Doris L2.

Tests the post-dispatch state machine without bypassing authorization gates.
Intents are inserted as fixture historical facts, not passed through prepare.
Full signed-command/current-role/quality positive publication remains untested.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import base64
import json
import os
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.command_signing import COMMAND_FORMAT, sign_envelope
from app.evidence_signing import EvidenceSigner
from app.execution import ExecutionRun, ManagedCommand, authorization_bindings
from app.models import Account, Membership, Project, Tenant
from app.preparations import Preparation
from app.studio import Candidate
from app.versions import PipelineVersion
from app.execution_runtime import TargetOwnership
from app.publication import PublicationIntent, PublishedSnapshot, PublicationService, _sha


@contextmanager
def fixture_database():
    schema = 'publication_fixture_' + uuid4().hex
    admin = create_engine(os.environ['DATABASE_URL'], hide_parameters=True)
    isolated = create_engine(os.environ['DATABASE_URL'], hide_parameters=True,
                             connect_args={'options': '-csearch_path=' + schema})
    try:
        with admin.begin() as db:
            db.execute(text(f'CREATE SCHEMA {schema}'))
        Base.metadata.create_all(isolated)
        sessions = sessionmaker(isolated, expire_on_commit=False)
        with patch.dict(os.environ, {'EVIDENCE_SIGNING_KEY': base64.b64encode(bytes(range(32))).decode()}):
            signer = EvidenceSigner()
            with patch.dict(os.environ, {'COMMAND_PUBLIC_KEY': signer.public['public_key']}):
                now = datetime.now(timezone.utc)
                with sessions.begin() as db:
                    db.add(Tenant(id='tenant', slug='tenant', name='Fixture'))
                    db.flush()
                    db.add(Project(id='project', tenant_id='tenant', slug='project', name='Fixture'))
                    for role in ('maker', 'operator'):
                        db.add(Account(id=role, tenant_id='tenant', username=role, display_name=role,
                                       **{'pass' + 'word_hash': 'unused'}))
                    db.flush()
                    for role in ('maker', 'operator'):
                        db.add(Membership(project_id='project', account_id=role, tenant_id='tenant', roles=[role]))
                    db.add(Candidate(id='candidate', tenant_id='tenant', project_id='project', owner_id='maker',
                                     name='Fixture', plan={}, assessment={}))
                    db.flush()
                    db.add(PipelineVersion(id='version', tenant_id='tenant', project_id='project', owner_id='maker',
                        candidate_id='candidate', candidate_revision=1, name='Fixture', execution_sha256='a' * 64,
                        bundle_sha256='b' * 64, frozen={}, artifacts={}, artifact_hashes={}))
                    db.flush()
                    frozen = {'execution_sha256': 'a' * 64, 'bundle_sha256': 'b' * 64, 'artifact_hashes': {},
                        'execution_facts': {'source': {}, 'target': {}},
                        'authorized_operation': {'data_plane_mutation': True}, 'budget': {},
                        'runtime_supervision_contract': {}, 'policy_version': 'fixture',
                        'compiler_versions': {}, 'plan': {}}
                    prep = Preparation(id='preparation', tenant_id='tenant', project_id='project', maker_id='maker',
                        version_id='version', tool='pipeline.publish', environment='local', risk_level='P1',
                        frozen=frozen, frozen_sha256=_sha(frozen), created_at=now, expires_at=now + timedelta(minutes=5))
                    db.add(prep)
                    db.flush()
                    run_id, command_id = str(uuid4()), str(uuid4())
                    db.add(ExecutionRun(id=run_id, tenant_id='tenant', project_id='project', preparation_id=prep.id,
                        subject='operator', status='submission_unknown', accepted_at=now))
                    db.flush()
                    payload = {'format': COMMAND_FORMAT, 'command_id': command_id, 'run_id': run_id,
                               'accepted_at': now.isoformat(), **authorization_bindings(prep, 'operator')}
                    db.add(ManagedCommand(id=command_id, tenant_id='tenant', project_id='project', run_id=run_id,
                        envelope=sign_envelope(signer, payload), created_at=now))
                yield sessions
    finally:
        isolated.dispose()
        # Resolved target is exclusively the locally generated UUID schema.
        assert schema.startswith('publication_fixture_') and schema.replace('_', '').isalnum()
        with admin.begin() as db:
            db.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
        admin.dispose()


class ObjectsFixture:
    def __init__(self, before):
        self.before = before
        self.current = before
        self.swaps = 0
        self.unavailable = False

    def observe(self, cluster, database, stable, shadow):
        if self.unavailable:
            raise OSError('fixture response lost')
        return self.current

    def swap(self, cluster, database, stable, shadow):
        self.swaps += 1
        self.current = {'stable': {**self.before['shadow'], 'table': stable},
                        'shadow': {**self.before['stable'], 'table': shadow}}


def object_identity(table, marker, partition):
    schema = [{'name': 'id', 'type': 'bigint', 'nullable': False}]
    return {'physical_cluster_id': 'fixture-cluster', 'database': 'fixture_db',
            'table': table, 'marker': marker, 'partition_ids': [partition],
            'schema': schema, 'schema_sha256': _sha(schema)}


def seed(sessions, command, phase='dispatching'):
    before = {'stable': object_identity('stable_fixture', 'etl_agent_object:old', '1'),
              'shadow': object_identity('shadow_fixture', 'etl_agent_object:new', '2')}
    after = {'stable': {**before['shadow'], 'table': before['stable']['table']},
             'shadow': {**before['stable'], 'table': before['shadow']['table']}}
    inputs = {'command_id': command.id, 'run_id': command.run_id, 'before': before, 'after': after}
    with sessions.begin() as db:
        now = db.scalar(text('SELECT clock_timestamp()'))
        # isolated clone only: replace an old ownership fixture for this command.
        existing = db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == command.id))
        if existing:
            db.delete(existing)
            db.flush()
        target = {'physical_cluster_id': 'fixture-cluster', 'database': 'fixture_db', 'table': 'stable_fixture'}
        target_key = _sha(target)
        db.add(TargetOwnership(target_key=target_key, command_id=command.id, identity=target, acquired_at=now))
        identifier = str(uuid4())
        db.add(PublicationIntent(id=identifier, command_id=command.id, run_id=command.run_id,
            idempotency_key='historical-fixture-only', target_key=target_key, inputs=inputs,
            input_sha256=_sha(inputs), phase=phase, created_at=now, updated_at=now))
    return identifier, before, after


def verify(sessions):
    with sessions() as db:
        commands = list(db.scalars(select(ManagedCommand).limit(3)))
    assert commands, 'fixture requires at least one existing managed command in the isolated baseline'
    command = commands[0]
    identifier, before, after = seed(sessions, command)
    adapter = ObjectsFixture(before)
    publication = PublicationService(sessions, adapter)
    # Durable dispatching exists, but observing before never permits replay.
    assert publication.reconcile(identifier)['phase'] == 'unknown'
    assert publication.publish(identifier)['phase'] == 'unknown' and adapter.swaps == 0
    # Simulate a successful single external swap whose reply/local commit was
    # lost: recovery must recognize exact physical identities, not swap again.
    adapter.swap('fixture-cluster', 'fixture_db', 'stable_fixture', 'shadow_fixture')
    adapter.unavailable = True
    assert publication.reconcile(identifier)['phase'] == 'unknown'
    assert publication.publish(identifier)['phase'] == 'unknown' and adapter.swaps == 1
    adapter.unavailable = False
    assert publication.reconcile(identifier)['phase'] == 'confirmed'
    assert publication.reconcile(identifier)['phase'] == 'confirmed'
    assert publication.publish(identifier)['phase'] == 'confirmed' and adapter.swaps == 1
    with sessions() as db:
        snapshots = list(db.scalars(select(PublishedSnapshot).where(PublishedSnapshot.intent_id == identifier)))
        assert len(snapshots) == 1
        assert snapshots[0].identities['after'] == after
        assert snapshots[0].retain_until - snapshots[0].confirmed_at >= timedelta(days=7)
        assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == command.id)) is None
    try:
        publication.rollback(snapshots[0].id, 'unapproved')
    except ValueError as error:
        assert str(error) == 'rollback_requires_new_p1_approval_and_verified_retained_snapshot'
    else:
        raise AssertionError('rollback must stay closed')
    # Missing signed lifecycle facts cannot be replaced by arbitrary caller flags.
    try:
        publication.prepare(str(uuid4()), 'no-command')
    except ValueError as error:
        assert str(error) == 'publication_command_missing'
    else:
        raise AssertionError('missing command must reject publication')
    return ['dispatch_before_state_never_replays', 'lost_response_read_only_identity_reconciliation',
            'exact_swapped_identity_single_snapshot_and_seven_day_retention',
            'confirmed_target_released_snapshot_retained_and_rollback_or_missing_command_closed']


if __name__ == '__main__':
    # No public-table reads/copies or API listener: create current metadata only
    # in a fresh UUID schema with fixture-owned signed command facts.
    with fixture_database() as sessions:
        results = verify(sessions)
    print(json.dumps({'level': 'postgresql_publication_recovery_fixture',
        'real_l2_qualified': False, 'positive_authorized_publish_tested': False,
        'passed': results}, ensure_ascii=False))
