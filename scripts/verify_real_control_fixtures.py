"""Real PostgreSQL/random-schema control fixtures; adapter seams, NOT real L2.

No deployment migration, SeaTunnel submit, real stop, or ownership release.
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
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.command_signing import COMMAND_FORMAT, sign_envelope, signing_bytes
from app.evidence_signing import EvidenceSigner
from app.execution import ExecutionRun, ManagedCommand, authorization_bindings
from app.execution_runtime import EngineAttempt, TargetOwnership
from app.models import Account, Membership, Project, SecretReference, Tenant
from app.preparations import Preparation
from app.real_control import advance_real_control
from app.run_control import CONTROL_FORMAT, ManagedControlEffect, RunControl, control_row
from app.runtime_supervision import RuntimeSupervisionContract
from app.studio import Candidate
from app.supervision_runtime import RuntimeSupervision, request_stop
from app.versions import PipelineVersion
from app.engine_adapter import job_name


def digest(value):
    return hashlib.sha256(signing_bytes(value)).hexdigest()


@contextmanager
def database():
    schema = 'real_control_fixture_' + uuid4().hex
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
                yield sessions, signer
    finally:
        isolated.dispose()
        # Verified resolved SQL target is only this locally generated UUID schema.
        assert schema.startswith('real_control_fixture_') and schema.replace('_', '').isalnum()
        with admin.begin() as db:
            db.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
        admin.dispose()


def seed(sessions, signer, *, source=None):
    run_id, command_id, prep_id = [str(uuid4()) for _ in range(3)]
    now = datetime.now(timezone.utc)
    contract = RuntimeSupervisionContract().model_dump()
    frozen = {'execution_sha256': 'a' * 64, 'bundle_sha256': 'b' * 64, 'artifact_hashes': {},
        'execution_facts': {'source': source or {}, 'target': {}},
        'authorized_operation': {'data_plane_mutation': True}, 'budget': {},
        'runtime_supervision_contract': contract, 'policy_version': 'fixture',
        'compiler_versions': {}, 'plan': {}}
    with sessions.begin() as db:
        prep = Preparation(id=prep_id, tenant_id='tenant', project_id='project', maker_id='maker',
            version_id='version', tool='pipeline.trial', environment='local', risk_level='P2',
            frozen=frozen, frozen_sha256=digest(frozen), created_at=now, expires_at=now + timedelta(minutes=5))
        db.add(prep)
        db.flush()
        db.add(ExecutionRun(id=run_id, tenant_id='tenant', project_id='project', preparation_id=prep_id,
            subject='operator', status='submitted', engine_job_id='123', accepted_at=now))
        db.flush()
        payload = {'format': COMMAND_FORMAT, 'command_id': command_id, 'run_id': run_id,
            'accepted_at': now.isoformat(), **authorization_bindings(prep, 'operator')}
        db.add(ManagedCommand(id=command_id, tenant_id='tenant', project_id='project', run_id=run_id,
            envelope=sign_envelope(signer, payload), created_at=now))
        db.flush()
        request = {'command_id': command_id, 'run_id': run_id, 'adapter': 'real',
            'engine_url': 'http://original-engine.invalid', 'engine_job_name': job_name(command_id)}
        db.add(EngineAttempt(command_id=command_id, correlation_id=str(uuid4()), request=request,
            request_sha256=digest(request), phase='submitted', engine_job_id='123', created_at=now, updated_at=now))
        db.add(TargetOwnership(target_key=digest(command_id), identity={'original': command_id},
            command_id=command_id, acquired_at=now))
        db.add(RuntimeSupervision(command_id=command_id, contract=contract, updated_at=now))
    return command_id


class Ingress:
    def __init__(self, sessions):
        self.sessions, self.closed, self.calls = sessions, False, []
        self.count = 0

    def observe(self, handle):
        self.calls.append('observe')
        # A separate transaction can acquire tenant now: factory/barrier must not
        # run while the controller holds tenant FOR UPDATE.
        with self.sessions.begin() as db:
            db.execute(text("SET LOCAL lock_timeout = '250ms'"))
            db.execute(select(Tenant).where(Tenant.id == 'tenant').with_for_update())
        return {'complete': True, 'byte_kind': 'measured', 'sink_bytes': self.count * 115,
            'evaluated': self.count, 'valid': self.count, 'rejected': 0, 'final_output': self.count,
            'admission_closed': self.closed, 'load_quiescent': self.closed, 'failures': [],
            'counter_epoch': 'original-journal', 'observed_at': datetime.now(timezone.utc).isoformat()}

    def close(self, handle, reason, /):
        self.calls.append(('close', reason))
        self.closed = True
        return {'admission_closed': True, 'load_quiescent': False}


class Engine:
    def __init__(self, sessions, command_id, ingress):
        self.sessions, self.command_id, self.ingress = sessions, command_id, ingress
        self.state, self.metrics, self.stops, self.gets = 'PENDING', {'SourceReceivedCount': '0'}, 0, 0
        self.lost_reply, self.reply_override = False, {}

    def observe(self, identifier, name):
        assert identifier == '123' and name == job_name(self.command_id)
        if self.gets % 2:
            assert self.ingress.calls[-1] == 'observe', 'barrier must precede final source GET'
        self.gets += 1
        return {'job_id': identifier, 'job_name': name, 'engine_state': self.state,
            'metrics': self.metrics, **self.reply_override}

    def stop(self, identifier):
        assert identifier == '123' and self.ingress.closed
        with self.sessions() as db:
            control = db.get(RunControl, self.command_id)
            effect = db.get(ManagedControlEffect, control.facts['real_stop']['effect_id'])
            assert effect.phase == 'inflight', 'reservation must already be PG committed'
            assert db.get(RuntimeSupervision, self.command_id).stop_request
        self.stops += 1
        if self.lost_reply:
            raise OSError('fixture reply lost')
        return {'accepted': True}


class CommitFailureSessions:
    """Fail a real PostgreSQL COMMIT using a deferred FK, not a mocked commit."""
    def __init__(self, sessions, transaction):
        self.sessions, self.transaction, self.count = sessions, transaction, 0

    def __call__(self):
        return self.sessions()

    @contextmanager
    def begin(self):
        self.count += 1
        current = self.count
        with self.sessions.begin() as db:
            yield db
            if current == self.transaction:
                db.execute(text('CREATE TEMP TABLE fixture_commit_failure ('
                    'id integer PRIMARY KEY, parent_id integer REFERENCES '
                    'fixture_commit_failure(id) DEFERRABLE INITIALLY DEFERRED) ON COMMIT DROP'))
                db.execute(text('INSERT INTO fixture_commit_failure VALUES (1, 2)'))
                # Both statements succeeded; only the actual COMMIT rejects it.


def accepted_stop(sessions, signer, cid, *, phase='accepted'):
    with sessions.begin() as db:
        command = db.get(ManagedCommand, cid)
        run = db.get(ExecutionRun, command.run_id)
        supervision = db.get(RuntimeSupervision, cid)
        now, identifier, reason = datetime.now(timezone.utc), str(uuid4()), 'fixture_operator_stop'
        request_stop(db, run, supervision, ['operator_stop'], now)
        payload = {'format': CONTROL_FORMAT, 'effect_id': identifier, 'command_id': cid,
            'run_id': run.id, 'tenant_id': run.tenant_id, 'project_id': run.project_id,
            'actor_id': 'operator', 'kind': 'stop', 'reason': reason,
            'resources': command.envelope['payload']['resources']}
        db.add(ManagedControlEffect(id=identifier, command_id=cid, kind='stop', actor_id='operator',
            idempotency_key=identifier, request_sha256=digest({
                'actor_id': 'operator', 'kind': 'stop', 'reason': reason}),
            envelope=sign_envelope(signer, payload), phase=phase, created_at=now, updated_at=now))
    return identifier


def verify(sessions, signer):
    passed = []
    def fixture(*, source=None):
        cid = seed(sessions, signer, source=source)
        ingress = Ingress(sessions)
        engine = Engine(sessions, cid, ingress)
        def factory(facts):
            assert facts['command_id'] == cid and facts['request']['engine_url'] == 'http://original-engine.invalid'
            # Same tenant lock assertion covers factory, not only observe.
            with sessions.begin() as db:
                db.execute(text("SET LOCAL lock_timeout = '250ms'"))
                db.execute(select(Tenant).where(Tenant.id == 'tenant').with_for_update())
            return ingress
        def client(url):
            assert url == 'http://original-engine.invalid'
            return engine
        def advance(*, control_sessions=sessions, due=False):
            if due:
                # Advance only the persisted fixture clock, without sleeping or
                # changing counters/engine state. Every adapter timestamp stays real.
                with sessions.begin() as db:
                    row = db.get(RuntimeSupervision, cid)
                    if row.last_trusted:
                        row.last_trusted = {**row.last_trusted, 'observed_at':
                            (datetime.now(timezone.utc) - timedelta(seconds=2.1)).isoformat()}
            return advance_real_control(cid, ingress_factory=factory, sessions=control_sessions, client_factory=client)
        return cid, ingress, engine, advance

    # A submit echo/lost reply precedes JobMaster visibility. No source/journal
    # sample exists yet; duplicate deliveries cannot extend the fixed deadline.
    cid, ingress, engine, advance = fixture()
    started = datetime.now(timezone.utc)
    with sessions.begin() as db:
        attempt = db.get(EngineAttempt, cid)
        attempt.phase = 'submission_unknown'
        db.get(ExecutionRun, db.get(ManagedCommand, cid).run_id).status = 'submission_unknown'
        control_row(db, cid).facts = {'real_submission': {'reserved_at': started.isoformat()}}
    hidden_gets = []
    def invisible(identifier, name):
        assert identifier == '123' and name == job_name(cid)
        hidden_gets.append(identifier)
        return None
    with patch.object(engine, 'observe', side_effect=invisible):
        assert advance() == 'submission_unknown', 'startup invisibility must remain discovery, not STOP'
        assert not ingress.calls and engine.stops == 0
        with sessions() as db:
            supervision = db.get(RuntimeSupervision, cid)
            assert supervision.last_trusted is None and not supervision.current_metrics
            assert not supervision.stop_request and supervision.publication_veto
            due = db.get(RunControl, cid).wake_at
        assert advance(control_sessions=sessionmaker(sessions.kw['bind'], expire_on_commit=False)) == 'submission_unknown'
        assert len(hidden_gets) == 1, 'startup GET cadence must survive worker/session changes'
        with sessions() as db:
            assert db.get(RunControl, cid).wake_at == due
    with patch('app.real_control._now', return_value=due):
        assert advance() == 'submitted'
    assert engine.stops == 0 and ingress.calls == ['observe']
    with sessions() as db:
        assert db.get(RuntimeSupervision, cid).last_trusted['source_read'] == 0
    passed.append('startup_invisible_then_visible_fixed_clock_no_early_stop_or_fabricated_counters')

    cid, ingress, engine, advance = fixture()
    started = datetime.now(timezone.utc) - timedelta(seconds=6.1)
    with sessions.begin() as db:
        control_row(db, cid).facts = {'real_submission': {'reserved_at': started.isoformat()}}
        db.get(EngineAttempt, cid).created_at = started - timedelta(seconds=1)
    with patch.object(engine, 'observe', return_value=None):
        assert advance() == 'termination_pending'
    assert engine.stops == 0 and ingress.closed
    with sessions() as db:
        assert db.get(RuntimeSupervision, cid).stop_request
    assert advance() == 'stop_requested' and engine.stops == 1
    passed.append('startup_missing_original_stops_at_persisted_six_second_deadline')

    # Duplicate deliveries must not turn the two-second barrier into a polling loop.
    cid, ingress, engine, advance = fixture()
    assert advance() == 'submitted'
    with sessions() as db:
        trusted = deepcopy(db.get(RuntimeSupervision, cid).last_trusted)
        due = datetime.fromisoformat(trusted['observed_at']) + timedelta(seconds=2)
    calls, gets = len(ingress.calls), engine.gets
    assert advance() == 'submitted'
    assert len(ingress.calls) == calls and engine.gets == gets, 'duplicate tick crossed ingress barrier'
    # Reconstruct the caller/session factory to model a different worker: no local clock/cache.
    other_worker = sessionmaker(sessions.kw['bind'], expire_on_commit=False)
    assert advance_real_control(cid, ingress_factory=lambda _: ingress, sessions=other_worker,
        client_factory=lambda _: engine) == 'submitted'
    with sessions() as db:
        assert db.get(RuntimeSupervision, cid).last_trusted == trusted
        assert db.get(RunControl, cid).wake_at == due, 'duplicate tick postponed next due'
    assert len(ingress.calls) == calls and engine.gets == gets
    with patch('app.real_control._now', return_value=due - timedelta(microseconds=1)):
        assert advance() == 'submitted' and len(ingress.calls) == calls
    with patch('app.real_control._now', return_value=due):
        assert advance() == 'submitted'
    assert ingress.calls.count('observe') == 2 and engine.gets == gets + 2
    passed.append('duplicate_tick_new_worker_persisted_clock_original_due_exact_two_second_boundary')

    # A recent trusted observation cannot postpone operator stop or revocation.
    for trigger in ('operator_stop', 'role_revoked'):
        cid, ingress, engine, advance = fixture()
        assert advance() == 'submitted'
        if trigger == 'operator_stop':
            accepted_stop(sessions, signer, cid)
        else:
            with sessions.begin() as db:
                db.get(Membership, ('project', 'operator')).roles = ['auditor']
        ingress.calls.clear()
        try:
            assert advance() == 'stop_requested' and engine.stops == 1
            assert ingress.calls[0] == ('close', 'ingress_closed'), 'stop waited behind barrier'
            assert ingress.closed and ingress.calls.count('observe') == 1
            advance()
            assert engine.stops == 1, 'duplicate stop redelivered POST'
        finally:
            with sessions.begin() as db:
                db.get(Membership, ('project', 'operator')).roles = ['operator']
    passed.append('recent_trusted_clock_never_delays_operator_stop_or_role_revocation_close_before_barrier')

    cid, ingress, engine, advance = fixture()
    assert advance() == 'submitted' and engine.stops == 0
    with sessions() as db:
        assert db.get(RuntimeSupervision, cid).current_metrics['source_read'] == 0
        assert db.get(RuntimeSupervision, cid).stop_request is None
    engine.metrics, engine.lost_reply = {}, True
    assert advance(due=True) == 'stop_delivery_unknown' and engine.stops == 1
    for _ in range(2):
        assert advance() == 'stop_delivery_unknown'
    with sessions() as db:
        supervision = db.get(RuntimeSupervision, cid)
        control = db.get(RunControl, cid)
        assert supervision.publication_veto and supervision.stop_request['delivery_status'] == 'unknown'
        assert control.facts['real_stop']['status'] == 'unknown'
        assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid)).identity == {'original': cid}
        assert control.wake_at and engine.stops == 1
        effect = db.get(ManagedControlEffect, supervision.stop_request['id'])
        assert effect.actor_id == 'operator' and effect.envelope['derivation']['request_id'] == effect.id
    passed.append('actual_pending_zero_then_missing_metrics_durable_close_lost_reply_single_post_owner_retained')

    # Simulated worker death after the reservation commit, before a send/result.
    cid, ingress, engine, advance = fixture()
    with sessions.begin() as db:
        supervision = db.get(RuntimeSupervision, cid)
        run = db.get(ExecutionRun, db.get(ManagedCommand, cid).run_id)
        request_stop(db, run, supervision, ['fixture_unsafe'], datetime.now(timezone.utc))
        from app.real_control import _derive_stop
        effect = _derive_stop(db, db.get(ManagedCommand, cid), run, supervision, datetime.now(timezone.utc))
        effect.phase = 'inflight'
        db.add(RunControl(command_id=cid, facts={'real_stop': {'effect_id': effect.id, 'kind': 'stop', 'status': 'inflight'}}))
    advance(); advance()
    assert engine.stops == 0 and engine.gets == 4
    passed.append('committed_inflight_worker_death_only_gets_never_reposts')

    # A failed reservation COMMIT sends nothing; a failed result COMMIT keeps
    # the earlier inflight reservation and may only read the original job again.
    for transaction in (2, 3):
        cid, ingress, engine, advance = fixture()
        engine.metrics = {}
        fault = CommitFailureSessions(sessions, transaction)
        try:
            advance(control_sessions=fault)
        except IntegrityError as error:
            assert error.orig.sqlstate == '23503'
        else:
            raise AssertionError('deferred PostgreSQL COMMIT must fail')
        assert engine.stops == (0 if transaction == 2 else 1)
        with sessions() as db:
            control = db.get(RunControl, cid)
            assert bool(control.facts.get('real_stop')) == (transaction == 3)
            assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid)) is not None
            if transaction == 3:
                effect = db.get(ManagedControlEffect, control.facts['real_stop']['effect_id'])
                assert effect.phase == 'inflight'
                assert db.get(RuntimeSupervision, cid).stop_request['delivery_status'] == 'inflight'
        advance(); advance()
        assert engine.stops == 1 and engine.gets == 6
    passed.append('actual_pg_reservation_commit_failure_no_post_result_commit_failure_only_get_no_repost')

    cid, ingress, engine, advance = fixture()
    with sessions.begin() as db:
        member = db.get(Membership, ('project', 'operator'))
        member.roles = ['auditor']
    advance()
    assert engine.stops == 1
    with sessions.begin() as db:
        assert db.get(RuntimeSupervision, cid).publication_veto
        db.get(Membership, ('project', 'operator')).roles = ['operator']
    # Mutating a persisted binding after accepting its signed authorization cannot
    # select a replacement URL, issue a new stop, or release its target.
    with sessions.begin() as db:
        attempt = db.get(EngineAttempt, cid)
        attempt.request = {**attempt.request, 'engine_url': 'http://replacement.invalid'}
    assert advance() == 'termination_pending' and engine.stops == 1
    passed.append('revoked_operator_safety_stop_uses_signed_actor_tampered_original_binding_no_io')

    for phase in ('accepted', 'inflight', 'confirmed'):
        cid, ingress, engine, advance = fixture()
        old_id = accepted_stop(sessions, signer, cid, phase=phase)
        with sessions.begin() as db:
            if phase == 'confirmed':
                db.get(ManagedControlEffect, old_id).result = {'accepted': True, 'original_engine_terminal': True}
            db.get(Membership, ('project', 'operator')).roles = ['auditor']
        try:
            advance(); advance()
            assert engine.stops == (1 if phase == 'accepted' else 0) and engine.gets == 4
            with sessions() as db:
                assert db.get(ManagedControlEffect, old_id).phase == 'denied'
                effects = list(db.scalars(select(ManagedControlEffect).where(
                    ManagedControlEffect.command_id == cid)))
                assert any('authorization' in effect.envelope and effect.actor_id == 'operator' for effect in effects)
                assert db.get(RuntimeSupervision, cid).publication_veto
        finally:
            with sessions.begin() as db:
                db.get(Membership, ('project', 'operator')).roles = ['operator']
    passed.append('old_stop_actor_revoked_derives_safety_stop_historical_inflight_never_reposts')

    cid, ingress, engine, advance = fixture()
    with patch.dict(os.environ, {'SECRET_MASTER_KEY': 'fixture-invalid-key'}):
        assert advance() == 'stop_requested' and engine.stops == 1
    with sessions() as db:
        assert 'authorization_revoked_or_secret_unavailable' in db.get(RuntimeSupervision, cid).reasons
    passed.append('unavailable_secret_provider_does_not_prevent_authorized_safety_stop')

    reference = str(uuid4())
    with sessions.begin() as db:
        db.add(SecretReference(id=reference, tenant_id='tenant', project_id='project', version=1,
            nonce=bytes(12), ciphertext=bytes(16), revoked_at=datetime.now(timezone.utc)))
    cid, ingress, engine, advance = fixture(source={'secret_reference_id': reference, 'secret_version': 1})
    assert advance() == 'stop_requested' and engine.stops == 1
    passed.append('revoked_original_secret_does_not_prevent_authorized_safety_stop')

    # Engine terminality can confirm the safety effect before a STOP reservation.
    # An unresolved load journal still vetoes publication and keeps ownership.
    cid, ingress, engine, advance = fixture()
    engine.state, engine.metrics = 'FINISHED', {'SourceReceivedCount': '3'}
    observed = ingress.observe
    def unknown_load(handle):
        return {**observed(handle), 'complete': False, 'load_quiescent': False}
    with sessions.begin() as db:
        db.get(Membership, ('project', 'operator')).roles = ['auditor']
    try:
        with patch.object(ingress, 'observe', side_effect=unknown_load):
            for tick in range(3):
                assert advance() == 'final_metrics_pending'
                with sessions() as db:
                    control, supervision = db.get(RunControl, cid), db.get(RuntimeSupervision, cid)
                    effect = db.scalar(select(ManagedControlEffect).where(ManagedControlEffect.command_id == cid))
                    assert effect.phase == 'confirmed' and effect.result == {'original_engine_terminal': True}
                    assert 'real_stop' not in control.facts and 'inflight' not in control.facts, 'terminal-only confirmation fabricated a STOP send reservation'
                    assert supervision.stop_request['delivery_status'] == 'pending'
                    assert supervision.stop_request['termination_confirmed'] and supervision.publication_veto
                    assert control.facts['final_metrics']['status'] == 'pending'
                    assert not control.facts['external_sink']['quiescent']
                    assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid))
        assert engine.stops == 0 and ingress.closed
    finally:
        with sessions.begin() as db:
            db.get(Membership, ('project', 'operator')).roles = ['operator']
    passed.append('terminal_before_stop_reservation_repeated_tick_no_fake_send_unknown_with_unsettled_journal')

    # A historical derived stop may have been denied after signature corruption.
    # Preserve that evidence and derive a separate safe effect under the original
    # command; reusing its ID must not abort reconciliation or block safety stop.
    for phase in ('accepted', 'denied'):
        cid, ingress, engine, advance = fixture()
        with sessions.begin() as db:
            supervision = db.get(RuntimeSupervision, cid)
            command = db.get(ManagedCommand, cid)
            run = db.get(ExecutionRun, command.run_id)
            request_stop(db, run, supervision, ['fixture_unsafe'], datetime.now(timezone.utc))
            from app.real_control import _derive_stop
            denied = _derive_stop(db, command, run, supervision, datetime.now(timezone.utc))
            denied_id = denied.id
            denied.envelope = {**denied.envelope, 'authorization': {
                **command.envelope, 'signature': base64.b64encode(bytes(64)).decode()}}
            denied.phase = phase
        assert advance() == 'stop_requested' and engine.stops == 1
        with sessions() as db:
            denied = db.get(ManagedControlEffect, denied_id)
            control = db.get(RunControl, cid)
            safe = db.get(ManagedControlEffect, control.facts['real_stop']['effect_id'])
            assert denied.phase == 'denied' and safe.id != denied.id
            assert safe.envelope['derivation']['request_id'] == denied_id
            assert safe.actor_id == 'operator'
        advance()
        assert engine.stops == 1
    passed.append('denied_same_request_id_does_not_block_distinct_derived_safety_stop')

    cid, ingress, engine, advance = fixture()
    engine.state, engine.metrics = 'FINISHED', {'SourceReceivedCount': '0'}
    original_close = ingress.close
    def finish_journal(handle, reason, /):
        # A final receipt/counter becomes visible at seal. Only the post-close
        # barrier and post-barrier GET may supply the final facts.
        ingress.count, engine.metrics = 1, {'SourceReceivedCount': '1'}
        return original_close(handle, reason)
    with patch.object(ingress, 'close', side_effect=finish_journal):
        assert advance() == 'engine_finished_awaiting_quality'
    assert ('close', 'engine_finished') in ingress.calls and engine.stops == 0
    assert ingress.calls.count('observe') == 1, 'FINISHED seal repeated complete barrier'
    with sessions() as db:
        control, supervision = db.get(RunControl, cid), db.get(RuntimeSupervision, cid)
        assert control.facts['termination']['confirmed'] and control.facts['final_metrics']['status'] == 'known'
        assert supervision.publication_veto and supervision.stop_request is None
        assert control.facts['final_metrics']['metrics']['source_read'] == 1
        assert control.facts['final_metrics']['metrics']['sink_bytes'] == 115
        assert control.facts['external_sink']['quiescent']
        assert engine.gets == 2 and ingress.calls[-1] == 'observe'
        assert ingress.calls == [('close', 'engine_finished'), 'observe']
    # The root quality finalizer owns clearance. Healthy later observations must
    # neither clear the initial veto nor re-latch an already finalized clearance.
    with sessions.begin() as db:
        db.get(RuntimeSupervision, cid).publication_veto = False
    assert advance() == 'engine_finished_awaiting_quality'
    with sessions() as db:
        assert not db.get(RuntimeSupervision, cid).publication_veto
    passed.append('finished_positional_close_engine_finished_resamples_preserves_quality_finalizer_veto')

    cid, ingress, engine, advance = fixture()
    engine.state, engine.metrics, ingress.count = 'RUNNING', {'SourceReceivedCount': '1'}, 1
    original_get = engine.observe
    def finish_during_get(identifier, name):
        reply = original_get(identifier, name)
        if engine.gets == 1:
            engine.state = 'FINISHED'
        return reply
    with patch.object(engine, 'observe', side_effect=finish_during_get):
        assert advance() == 'submitted', 'late FINISHED must defer final barrier without safety stop'
    assert ingress.closed and ingress.calls.count('observe') == 1 and engine.stops == 0
    assert advance() == 'engine_finished_awaiting_quality'
    assert ingress.calls.count('observe') == 2 and engine.stops == 0
    passed.append('finished_between_state_get_and_final_get_defers_sealed_barrier_without_double_observe_or_stop')

    for mutation in ('signature', 'attempt_run', 'attempt_name'):
        cid, ingress, engine, advance = fixture()
        with sessions.begin() as db:
            if mutation == 'signature':
                command = db.get(ManagedCommand, cid)
                command.envelope = {**command.envelope, 'signature': base64.b64encode(bytes(64)).decode()}
            else:
                attempt = db.get(EngineAttempt, cid)
                key = 'run_id' if mutation == 'attempt_run' else 'engine_job_name'
                attempt.request = {**attempt.request, key: 'replacement'}
                attempt.request_sha256 = digest(attempt.request)
        assert advance() == 'termination_pending'
        assert engine.stops == engine.gets == 0 and not ingress.calls
        with sessions() as db:
            assert db.get(RuntimeSupervision, cid).publication_veto
            assert db.get(RunControl, cid).facts['real_control']['reason'] == 'original_binding_unverified'
            assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid)) is not None
    passed.append('bad_original_signature_and_rehashed_attempt_binding_mismatch_zero_external_io')

    cid, ingress, engine, advance = fixture()
    with sessions.begin() as db:
        db.get(RuntimeSupervision, cid).publication_veto = False
    observe = ingress.observe
    def changed_during_barrier(handle):
        sample = observe(handle)
        with sessions.begin() as db:
            command = db.get(ManagedCommand, cid)
            command.envelope = {**command.envelope, 'signature': base64.b64encode(bytes(64)).decode()}
        return sample
    with patch.object(ingress, 'observe', side_effect=changed_during_barrier):
        assert advance() == 'termination_pending'
    assert engine.gets == 2 and engine.stops == 0
    with sessions() as db:
        assert db.get(RuntimeSupervision, cid).publication_veto
        assert db.get(RunControl, cid).facts['real_control']['reason'] == 'original_binding_unverified'
    passed.append('binding_changed_during_independent_barrier_relatches_veto_no_post')

    for override in ({'job_id': '456'}, {'job_name': 'replacement'}, {'engine_terminal': True}):
        cid, ingress, engine, advance = fixture()
        engine.reply_override = override
        assert advance() == ('submitted' if 'engine_terminal' in override else 'termination_pending')
        assert engine.stops == 0 and engine.gets == 2
        with sessions() as db:
            assert not db.get(RunControl, cid).facts.get('termination', {}).get('confirmed')
    passed.append('get_uses_original_id_name_and_revalidates_id_name_status_never_terminal_flag')

    cid, ingress, engine, advance = fixture()
    observe = ingress.observe
    def stale(handle):
        sample = observe(handle)
        return {**sample, 'observed_at': (datetime.now(timezone.utc) - timedelta(seconds=6.1)).isoformat()}
    with patch.object(ingress, 'observe', side_effect=stale):
        assert advance() == 'stop_requested' and engine.stops == 1
    with sessions() as db:
        supervision, control = db.get(RuntimeSupervision, cid), db.get(RunControl, cid)
        assert supervision.contract['observation_seconds'] == 2
        assert supervision.contract['stale_after_seconds'] == 6
        assert 'observation_stale' in supervision.reasons and supervision.publication_veto
        assert control.wake_at > control.last_seen_at
    passed.append('two_second_schedule_six_second_staleness_no_pending_startup_grace')

    for state in ('UNRECOGNIZED_TERMINAL', 'CANCELED'):
        cid, ingress, engine, advance = fixture()
        engine.state, engine.metrics = state, {}
        assert advance() == ('termination_pending' if state == 'UNRECOGNIZED_TERMINAL' else 'final_metrics_pending')
        assert engine.stops == 0
        with sessions() as db:
            control = db.get(RunControl, cid)
            assert control.facts.get('termination', {}).get('confirmed', False) == (state == 'CANCELED')
            assert control.facts['final_metrics']['status'] == 'pending'
            assert db.get(RuntimeSupervision, cid).publication_veto
            assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid)) is not None
    passed.append('unknown_terminal_and_canceled_without_source_metrics_remain_pending_owner_retained')

    for state, source_count, sent_count in (('CANCELED', 5, 2), ('FAILED', 0, 0), ('FINISHED', 5, 2)):
        cid, ingress, engine, advance = fixture()
        if state == 'FINISHED':
            accepted_stop(sessions, signer, cid)
        engine.state, engine.metrics, ingress.count = state, {'SourceReceivedCount': str(source_count)}, sent_count
        assert advance() == 'terminated'
        assert engine.stops == 0
        with sessions() as db:
            control, supervision = db.get(RunControl, cid), db.get(RuntimeSupervision, cid)
            final = control.facts['final_metrics']
            assert final['status'] == 'known' and final['usage'] == 'cleanup_only'
            assert final['metrics']['source_read'] == source_count
            assert final['metrics']['final_output'] == sent_count
            assert final['sample']['terminal_purpose'] == 'cleanup_only'
            assert final['sample'] == supervision.last_trusted and final['metrics'] == supervision.current_metrics
            assert supervision.publication_veto and supervision.stop_request
            assert control.facts['termination']['engine_state'] == state
            assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid)) is not None
        assert advance() == 'terminated' and engine.stops == 0
    passed.append('canceled_failed_or_stopped_finished_known_partial_or_empty_counts_cleanup_only_never_publish')

    for fault in ('unknown_bytes', 'epoch_reset', 'counter_reset', 'stale'):
        cid, ingress, engine, advance = fixture()
        ingress.count, engine.state, engine.metrics = 2, 'RUNNING', {'SourceReceivedCount': '3'}
        assert advance() == 'submitted'
        engine.state = 'CANCELED'
        original_observe = ingress.observe
        def broken_terminal(handle):
            sample = original_observe(handle)
            if fault == 'unknown_bytes':
                return {**sample, 'complete': False, 'sink_bytes': None}
            if fault == 'epoch_reset':
                return {**sample, 'counter_epoch': 'replacement'}
            if fault == 'counter_reset':
                return {**sample, 'sink_bytes': 0}
            return {**sample, 'observed_at': (datetime.now(timezone.utc) - timedelta(seconds=7)).isoformat()}
        with patch.object(ingress, 'observe', side_effect=broken_terminal):
            assert advance(due=True) == 'final_metrics_pending'
        with sessions() as db:
            control, supervision = db.get(RunControl, cid), db.get(RuntimeSupervision, cid)
            assert control.facts['final_metrics']['status'] == 'pending'
            assert supervision.current_metrics['sink_bytes'] is None and supervision.publication_veto
            assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid)) is not None
        assert engine.stops == 0
    passed.append('terminal_unknown_bytes_stale_and_epoch_or_counter_reset_never_become_known')

    for phase in ('accepted', 'unknown'):
        cid, ingress, engine, advance = fixture()
        with sessions.begin() as db:
            run = db.get(ExecutionRun, db.get(ManagedCommand, cid).run_id)
            run.status = 'supervision_passed'  # Early completion guard must not swallow cleanup.
            db.add(ManagedControlEffect(id=str(uuid4()), command_id=cid, kind='cleanup', actor_id='operator',
                idempotency_key=str(uuid4()), request_sha256='0' * 64, envelope={}, phase=phase,
                created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc)))
        def cleanup_dispatch(command_id, **dependencies):
            assert command_id == cid and dependencies['sessions'] is sessions
            with sessions.begin() as db:
                db.execute(text("SET LOCAL lock_timeout = '250ms'"))
                db.execute(select(Tenant).where(Tenant.id == 'tenant').with_for_update())
            return 'cleanup_pending'
        with patch('app.real_cleanup.advance_real_cleanup', side_effect=cleanup_dispatch) as dispatched:
            assert advance() == 'cleanup_pending' and dispatched.call_count == 1
        assert engine.stops == engine.gets == 0 and not ingress.calls
        with sessions() as db:
            effect = db.scalar(select(ManagedControlEffect).where(
                ManagedControlEffect.command_id == cid, ManagedControlEffect.kind == 'cleanup'))
            assert effect.phase == phase
    passed.append('accepted_or_unknown_cleanup_dispatched_before_finished_guard_without_tenant_or_control_lock')

    cid, engine_calls = seed(sessions, signer), []
    def forbidden_client(url):
        engine_calls.append(url)
        raise AssertionError('no engine IO')
    for _ in range(2):
        assert advance_real_control(cid, sessions=sessions, client_factory=forbidden_client) == 'termination_pending'
    assert not engine_calls
    with sessions() as db:
        supervision = db.get(RuntimeSupervision, cid)
        assert supervision.stop_request and supervision.publication_veto
        assert supervision.stop_request['delivery_status'] == 'pending'
        assert db.get(RunControl, cid).facts['external_sink']['admission_closed'] is False
        assert db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == cid)) is not None
    passed.append('missing_factory_durable_stop_veto_no_external_io_or_false_quiescence')
    return passed


if __name__ == '__main__':
    with database() as (sessions, signer):
        results = verify(sessions, signer)
    print(json.dumps({'level': 'postgresql_real_control_adapter_fixture', 'real_l2_qualified': False,
        'passed': results}, ensure_ascii=False))
