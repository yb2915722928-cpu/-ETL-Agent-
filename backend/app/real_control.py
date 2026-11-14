"""Bounded real supervision/stop step; caller owns the command advisory lock.

No submission, publication, or ownership release is implemented here. Signed
cleanup effects delegate to the separate durable cleanup module before guards.
Factory and ingress barriers run outside tenant transactions. A committed inflight
stop is an irreversible send reservation, including after a dead worker or failed
result commit; later steps only GET the original SeaTunnel identity.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import os
import socket

from fastapi import HTTPException
from sqlalchemy import func, select

from .command_signing import COMMAND_FORMAT, signing_bytes, verify_envelope
from .db import Session
from .engine_adapter import SeaTunnelClient, associated_job, job_name
from .evidence import lock_tenant
from .execution import ExecutionRun, ManagedCommand, authorization_bindings
from .execution_runtime import EngineAttempt, record
from .models import new_id
from .preparations import get_preparation, require_current_role
from .real_observation import compose_observation, derive_terminal_metrics
from .run_control import (ManagedControlEffect, RunControl, check_running_authorization,
                          control_row, effects_for, final_facts, verify_effect)
from .runtime_supervision import COUNTERS, evaluate_observation
from .supervision_runtime import RuntimeObservation, RuntimeSupervision, request_stop


def _now(db):
    return db.scalar(select(func.clock_timestamp()))


def _rows(db, command_id):
    command = db.get(ManagedCommand, command_id)
    if command is None:
        return None
    lock_tenant(db, command.tenant_id)
    run = db.get(ExecutionRun, command.run_id)
    attempt = db.get(EngineAttempt, command_id)
    supervision = db.get(RuntimeSupervision, command_id)
    control = control_row(db, command_id)
    return command, run, attempt, supervision, control


def _binding(db, command, run, attempt, supervision):
    payload = verify_envelope(command.envelope, os.environ['COMMAND_PUBLIC_KEY'], COMMAND_FORMAT)
    preparation = get_preparation(db, run.tenant_id, run.project_id, run.preparation_id)
    expected = {'command_id': command.id, 'run_id': run.id, 'accepted_at': run.accepted_at.isoformat(),
                **authorization_bindings(preparation, run.subject)}
    if (any(payload.get(key) != value for key, value in expected.items())
            or (command.tenant_id, command.project_id) != (run.tenant_id, run.project_id)
            or supervision is None or payload.get('runtime_supervision_contract') != supervision.contract
            or supervision.contract.get('observation_seconds') != 2
            or supervision.contract.get('stale_after_seconds') != 6):
        raise ValueError('original_command_binding_mismatch')
    if (attempt is None or attempt.request.get('adapter') != 'real'
            or not isinstance(attempt.request.get('engine_url'), str)
            or not attempt.request['engine_url'].startswith(('http://', 'https://'))
            or attempt.request.get('engine_job_name') != job_name(command.id)
            or attempt.request.get('job_name', job_name(command.id)) != job_name(command.id)
            or attempt.request.get('command_id') != command.id
            or attempt.request.get('run_id') != run.id
            or (attempt.request.get('engine_job_id') is not None and
                str(attempt.request['engine_job_id']) != str(attempt.engine_job_id))
            or (run.engine_job_id is not None and run.engine_job_id != attempt.engine_job_id)):
        raise ValueError('original_attempt_binding_mismatch')
    SeaTunnelClient.validate_job_id(attempt.engine_job_id)
    if hashlib.sha256(signing_bytes(attempt.request)).hexdigest() != attempt.request_sha256:
        raise ValueError('original_attempt_digest_mismatch')
    return payload


def _derive_stop(db, command, run, supervision, now):
    identifier = supervision.stop_request['id']
    existing = db.get(ManagedControlEffect, identifier)
    if existing is not None and existing.phase != 'denied':
        return existing
    # A denied effect remains audit evidence. A replacement effect is still
    # derived from the same latched request, never a new stop/send permission.
    effect_id = identifier if existing is None else new_id()
    effect = ManagedControlEffect(id=effect_id, command_id=command.id, kind='stop',
        actor_id=command.envelope['payload']['subject'],
        idempotency_key='automatic-safety-stop' if existing is None else 'automatic-safety-stop:' + effect_id,
        request_sha256=hashlib.sha256(signing_bytes(command.envelope)).hexdigest(),
        envelope={'authorization': command.envelope, 'derivation': {
            'command_id': command.id, 'request_id': identifier, 'kind': 'stop'}},
        phase='accepted', created_at=now, updated_at=now)
    db.add(effect)
    db.flush()
    return effect


def _valid_effect(db, effect, command, run, supervision):
    verify_effect(effect, command, run)
    if effect.command_id != command.id or effect.kind != 'stop':
        raise ValueError('stop_effect_binding_mismatch')
    if 'authorization' in effect.envelope:
        if (effect.actor_id != command.envelope['payload']['subject']
                or effect.envelope.get('derivation') != {'command_id': command.id,
                    'request_id': supervision.stop_request['id'], 'kind': 'stop'}
                or effect.request_sha256 != hashlib.sha256(signing_bytes(command.envelope)).hexdigest()):
            raise ValueError('derived_stop_binding_mismatch')
    else:
        require_current_role(db, run.tenant_id, run.project_id, effect.actor_id, 'operator')
        payload = effect.envelope['payload']
        digest = hashlib.sha256(signing_bytes({'actor_id': effect.actor_id,
            'kind': effect.kind, 'reason': payload['reason']})).hexdigest()
        if effect.request_sha256 != digest:
            raise ValueError('control_request_digest_mismatch')


def _closed_sample(service, command_id, reason):
    try:
        closed = service.close(command_id, reason)
        # Admission close alone is sufficient before STOP, never quiescence proof.
        return closed if isinstance(closed, dict) and closed.get('admission_closed') is True else None
    except Exception:
        return None


def advance_real_control(command_id, *, ingress_factory=None, sessions=Session,
                         client_factory=SeaTunnelClient):
    """Advance under caller's command lock; factory(frozen_facts) -> ingress service.

    Factory receives command_id/run_id/engine_job_id/request/command_payload.
    It must return an internal, original-bound IngressService, never caller flags.
    """
    # Route accepted/recovering cleanup before completed-run guards and outside
    # every tenant/control transaction. The cleanup module commits its own
    # irreversible reservations and reauthorizes the signed original itself.
    with sessions() as routing_db:
        cleanup = routing_db.scalar(select(ManagedControlEffect).where(
            ManagedControlEffect.command_id == command_id, ManagedControlEffect.kind == 'cleanup',
            ManagedControlEffect.phase.in_(('accepted', 'unknown', 'dispatching', 'inflight'))))
        if cleanup is not None:
            original = routing_db.get(ManagedCommand, command_id)
            original_payload = deepcopy(original.envelope['payload']) if original else None
    if cleanup is not None:
        from .real_cleanup import advance_real_cleanup
        def cleanup_ingress(request):
            return ingress_factory({'command_id': command_id, 'run_id': request['run_id'],
                'engine_job_id': request['engine_job_id'], 'request': request,
                'command_payload': original_payload})
        return advance_real_cleanup(command_id, sessions=sessions, client_factory=client_factory,
            ingress_factory=cleanup_ingress if ingress_factory is not None else None)
    with sessions.begin() as db:
        rows = _rows(db, command_id)
        if rows is None:
            return 'missing'
        command, run, attempt, supervision, control = rows
        now = _now(db)
        if run.status == 'cleaned' or run.status == 'supervision_passed' and not supervision.stop_request:
            # Finished counters belong to the completed run. Duplicate deliveries
            # must not accrue duration or reopen its sealed quality report.
            control.wake_at = None
            return run.status
        control.lease_owner = socket.gethostname() + ':' + str(os.getpid())
        control.lease_token, control.last_seen_at = new_id(), now
        control.lease_expires_at = now + timedelta(seconds=15)
        control.wake_at = now + timedelta(seconds=2)
        try:
            payload = _binding(db, command, run, attempt, supervision)
        except (ValueError, HTTPException, KeyError, TypeError):
            if supervision:
                supervision.publication_veto = True
            control.facts = {**control.facts, 'real_control': {'status': 'unknown',
                'reason': 'original_binding_unverified', 'ownership_retained': True}}
            return 'termination_pending'
        frozen = {'command_id': command.id, 'run_id': run.id,
            'engine_job_id': attempt.engine_job_id, 'request': dict(attempt.request),
            'command_payload': payload}
        contract, previous, created = dict(supervision.contract), supervision.last_trusted, attempt.created_at
        reasons = []
        try:
            # Revocation latches safety stop; it cannot prevent stopping the old job.
            check_running_authorization(db, command, run)
        except (HTTPException, ValueError, KeyError, RuntimeError):
            reasons.append('authorization_revoked_or_secret_unavailable')
        already_stopping = bool(supervision.stop_request)
        # A submission reservation is a fixed startup clock, never a heartbeat.
        # It grants neither write admission nor replacement measurements.
        startup_deadline = None
        if previous is None:
            try:
                reserved = datetime.fromisoformat(control.facts['real_submission']['reserved_at'])
                if reserved.tzinfo is not None and created <= reserved <= now:
                    startup_deadline = reserved + timedelta(seconds=contract['stale_after_seconds'])
            except (KeyError, TypeError, ValueError):
                pass
        if startup_deadline and now >= startup_deadline:
            reasons.append('observation_stale')
        if (now - created).total_seconds() > contract['max_seconds']:
            reasons.append('duration_exceeded')
        startup = control.facts.get('real_startup') or {}
        if startup_deadline and not already_stopping and not reasons and now < startup_deadline:
            try:
                due = datetime.fromisoformat(startup['next_observe_at'])
                if now < due <= startup_deadline:
                    control.wake_at = due
                    return run.status
            except (KeyError, TypeError, ValueError):
                pass
        # A delivery/lease heartbeat is not a measurement. Use the committed
        # trusted sample across workers and keep its original due time unchanged.
        terminal_barrier_pending = (control.facts.get('real_control') or {}).get('terminal_barrier_pending')
        if not already_stopping and not reasons and not terminal_barrier_pending and previous:
            try:
                observed_at = datetime.fromisoformat(previous['observed_at'])
                due = observed_at + timedelta(seconds=contract['observation_seconds'])
                if observed_at <= now < due:
                    control.wake_at = due
                    return run.status
            except (KeyError, TypeError, ValueError):
                pass  # An invalid timestamp never buys a supervision grace period.
    service = None
    try:
        service = ingress_factory(deepcopy(frozen)) if ingress_factory is not None else None
    except Exception:
        pass
    # Stop/revocation closes admission before any observation barrier or engine I/O.
    closed = _closed_sample(service, command_id, 'ingress_closed') if service and (already_stopping or reasons) else None
    client = None
    if service is not None:
        try:
            client = client_factory(frozen['request']['engine_url'])
        except Exception:
            pass

    def engine_observation():
        try:
            candidate = client.observe(frozen['engine_job_id'], job_name(command_id)) if client else None
            if isinstance(candidate, dict):
                return associated_job({'jobId': candidate.get('job_id'), 'jobName': candidate.get('job_name'),
                    'jobStatus': candidate.get('engine_state'), 'metrics': candidate.get('metrics')},
                    frozen['engine_job_id'], job_name(command_id))
        except Exception:
            pass
        return None

    # This first GET only selects sealing; its counters are never composed.
    # Seal before the sole full journal barrier, then GET the final source counters.
    proof = engine_observation()
    # Discovery may precede JobMaster visibility or its first source metrics.
    # Do not open the journal's write window to discover that source fact.
    # After the first trusted sample, gaps retain immediate fail-closed handling.
    metrics = proof.get('metrics') if proof else None
    source = metrics.get('SourceReceivedCount') if isinstance(metrics, dict) else None
    source_known = (type(source) is int and source >= 0 or
                    type(source) is str and source.isascii() and source.isdecimal())
    startup_missing = proof is None or (not proof['engine_terminal'] and not source_known)
    if startup_deadline and startup_missing and service and client and not (already_stopping or reasons):
        with sessions.begin() as db:
            command, run, attempt, supervision, control = _rows(db, command_id)
            checked_at = _now(db)
            _binding(db, command, run, attempt, supervision)
            if (checked_at < startup_deadline and supervision.last_trusted is None
                    and not supervision.stop_request and attempt.request == frozen['request']):
                # Recheck authorization after independent discovery I/O.
                try:
                    check_running_authorization(db, command, run)
                except (HTTPException, ValueError, KeyError, RuntimeError):
                    reasons.append('authorization_revoked_or_secret_unavailable')
                else:
                    due = min(checked_at + timedelta(seconds=contract['observation_seconds']), startup_deadline)
                    control.facts = {**control.facts, 'real_startup': {'status': 'awaiting_original_source',
                        'deadline': startup_deadline.isoformat(), 'next_observe_at': due.isoformat()}}
                    control.wake_at = due
                    supervision.publication_veto = True
                    return run.status
    cleanup_only = bool(proof and proof['engine_terminal'] and
                        (already_stopping or reasons or proof['engine_state'] in ('CANCELED', 'FAILED')))
    sealing = bool(proof and proof['engine_terminal'])
    if sealing:
        reason = 'ingress_closed' if cleanup_only else 'engine_finished'
        closed = _closed_sample(service, command_id, reason)
    try:
        ingress = service.observe(command_id) if service is not None and (not sealing or closed) else None
    except Exception:
        ingress = None
    proof = engine_observation()
    engine_observed_at = datetime.now(timezone.utc).isoformat()
    # A terminal transition between the two GETs needs a post-close barrier.
    # Defer that one barrier to the next tick instead of treating pre-close
    # journal facts as final or doing a second complete observation this tick.
    if proof and proof['engine_terminal'] and not sealing and not (already_stopping or reasons):
        reason = 'engine_finished' if proof['engine_state'] == 'FINISHED' else 'ingress_closed'
        closed = _closed_sample(service, command_id, reason)
        if closed and closed.get('admission_closed'):
            with sessions.begin() as db:
                command, run, attempt, supervision, control = _rows(db, command_id)
                try:
                    _binding(db, command, run, attempt, supervision)
                    if attempt.request != frozen['request'] or attempt.engine_job_id != frozen['engine_job_id']:
                        raise ValueError('original_attempt_changed_during_observation')
                except (ValueError, HTTPException, KeyError, TypeError):
                    supervision.publication_veto = True
                    control.facts = {**control.facts, 'real_control': {'status': 'unknown',
                        'reason': 'original_binding_unverified', 'ownership_retained': True}}
                    return 'termination_pending'
                supervision.publication_veto = True
                control.wake_at = _now(db)
                control.facts = {**control.facts, 'real_control': {'status': 'terminal_barrier_pending',
                    'terminal_barrier_pending': True, 'ownership_retained': True},
                    'final_metrics': {'status': 'pending', 'overshoot': None}}
                return run.status
    cleanup_only = bool(proof and proof['engine_terminal'] and
                        (already_stopping or reasons or proof['engine_state'] in ('CANCELED', 'FAILED')))
    with sessions() as clock_db:
        now = _now(clock_db)
    if startup_deadline and now >= startup_deadline:
        reasons.append('observation_stale')
    if cleanup_only:
        sample, decision, composition_reasons = derive_terminal_metrics(contract, proof, ingress, previous,
            engine_job_id=frozen['engine_job_id'], expected_name=job_name(command_id),
            engine_observed_at=engine_observed_at, elapsed_seconds=(now - created).total_seconds(), now=now)
    else:
        sample, composition_reasons = compose_observation(contract, proof, ingress,
            engine_job_id=frozen['engine_job_id'], expected_name=job_name(command_id),
            engine_observed_at=engine_observed_at, elapsed_seconds=(now - created).total_seconds(), now=now)
        decision = evaluate_observation(contract, sample, previous, now)
    reasons = list(dict.fromkeys([*reasons, *composition_reasons, *decision['reasons']]))
    if (now - created).total_seconds() > contract['max_seconds']:
        reasons = list(dict.fromkeys([*reasons, 'duration_exceeded']))
    if (already_stopping or reasons) and closed is None:
        closed = _closed_sample(service, command_id, 'ingress_closed') if service else None
    dispatch = None
    with sessions.begin() as db:
        command, run, attempt, supervision, control = _rows(db, command_id)
        now = _now(db)
        if startup_deadline and now >= startup_deadline:
            reasons = list(dict.fromkeys([*reasons, 'observation_stale']))
        # Revalidate frozen identity after all independent ingress transactions.
        try:
            _binding(db, command, run, attempt, supervision)
            if attempt.request != frozen['request'] or attempt.engine_job_id != frozen['engine_job_id']:
                raise ValueError('original_attempt_changed_during_observation')
        except (ValueError, HTTPException, KeyError, TypeError):
            supervision.publication_veto = True
            control.facts = {**control.facts, 'real_control': {'status': 'unknown',
                'reason': 'original_binding_unverified', 'ownership_retained': True}}
            return 'termination_pending'
        control.wake_at = now + timedelta(seconds=2)
        # Only the quality finalizer may clear publication veto. A healthy final
        # recheck preserves its decision; every unsafe/running step latches veto.
        if reasons or supervision.stop_request or decision['status'] != 'finished':
            supervision.publication_veto = True
        db.add(RuntimeObservation(command_id=command_id, received_at=now,
            source=sample.get('source') if sample else None, sample=sample,
            decision={**decision, 'reasons': reasons}))
        supervision.current_metrics, supervision.updated_at = decision['metrics'], now
        reliable = sample is not None and all(decision['metrics'][key] is not None for key in COUNTERS)
        if reliable:
            supervision.last_trusted = sample
            control.wake_at = datetime.fromisoformat(sample['observed_at']) + timedelta(seconds=contract['observation_seconds'])
        if reasons:
            request_stop(db, run, supervision, reasons, now)
        elif not supervision.stop_request:
            supervision.status, supervision.reasons = decision['status'], []
        terminal = bool(proof and proof['engine_terminal'])
        control.facts = {**control.facts, 'real_control': {'status': 'observed' if proof else 'unknown',
            'ownership_retained': True}, 'external_sink': {
                'admission_closed': bool(ingress and ingress.get('admission_closed')),
                'quiescent': bool(ingress and ingress.get('load_quiescent')),
                'observed_at': ingress.get('observed_at') if ingress else None}}
        if terminal:
            control.facts = {**control.facts, 'termination': {'confirmed': True, 'basis': 'engine_terminal',
                'engine_job_id': attempt.engine_job_id, 'job_name': job_name(command_id),
                'engine_state': proof['engine_state'], 'confirmed_at': now.isoformat()}}
            if supervision.stop_request:
                supervision.stop_request = {**supervision.stop_request, 'termination_confirmed': True}
        if terminal and reliable and sample['state'] == 'finished':
            final = final_facts(contract, sample, decision['metrics'],
                (supervision.stop_request or {}).get('detection_metrics'), now)
            if cleanup_only:
                final = {**final, 'usage': 'cleanup_only', 'engine_state': proof['engine_state']}
                supervision.publication_veto = True
            control.facts = {**control.facts, 'final_metrics': final}
        else:
            control.facts = {**control.facts, 'final_metrics': {'status': 'pending', 'overshoot': None}}
        if supervision.stop_request:
            effects = effects_for(db, command_id)
            # Capture irreversible send history BEFORE denying stale/tampered
            # effects. Revocation cannot erase an earlier reservation, even if a
            # historical worker omitted the separate control ledger.
            reserved = control.facts.get('real_stop') or control.facts.get('inflight')
            historical = next((e for e in effects if e.kind == 'stop' and e.phase in
                ('inflight', 'dispatching', 'unknown', 'confirmed')
                # This exact result records terminal observation only. Any actual
                # reservation, delivery status, or other historical result stays
                # conservative send history, including an acknowledged STOP.
                and not (e.phase == 'confirmed' and e.result == {'original_engine_terminal': True})), None)
            sent = bool(reserved or historical or supervision.stop_request.get('delivery_status') in
                ('inflight', 'unknown', 'acknowledged'))
            if sent and not reserved:
                # Keep recovered history durable after the old effect is denied;
                # otherwise the next tick could forget it and issue a second POST.
                reservation = {'effect_id': historical.id if historical else supervision.stop_request['id'],
                    'kind': 'stop', 'status': 'unknown', 'engine_job_id': attempt.engine_job_id,
                    'job_name': job_name(command_id), 'engine_url': attempt.request['engine_url'],
                    'reconciled_at': now.isoformat()}
                control.facts = {**control.facts, 'real_stop': reservation, 'inflight': reservation}
            valid = []
            for effect in effects:
                if effect.kind != 'stop' or effect.phase == 'denied':
                    continue
                try:
                    _valid_effect(db, effect, command, run, supervision)
                    valid.append(effect)
                except (HTTPException, ValueError, KeyError, TypeError):
                    effect.phase, effect.result, effect.updated_at = 'denied', {'reason': 'control_authorization_invalid'}, now
            if not valid:
                derived = _derive_stop(db, command, run, supervision, now)
                _valid_effect(db, derived, command, run, supervision)
                valid = [derived]
            if terminal:
                for effect in valid:
                    effect.phase, effect.updated_at = 'confirmed', now
                    effect.result = {**effect.result, 'original_engine_terminal': True}
                run.status = 'terminated' if control.facts['final_metrics']['status'] == 'known' else 'final_metrics_pending'
            elif not sent and closed and closed.get('admission_closed') and proof is not None:
                effect = valid[0]
                effect.phase, effect.updated_at = 'inflight', now
                reservation = {'effect_id': effect.id, 'kind': 'stop', 'status': 'inflight',
                    'engine_job_id': attempt.engine_job_id, 'job_name': job_name(command_id),
                    'engine_url': attempt.request['engine_url'], 'started_at': now.isoformat()}
                control.facts = {**control.facts, 'real_stop': reservation, 'inflight': reservation}
                supervision.stop_request = {**supervision.stop_request, 'delivery_status': 'inflight',
                    'dispatched_at': now.isoformat()}
                dispatch = dict(reservation)
                run.status = 'stop_requested'
                record(db, run, 'control.real_stop_prepared', reservation)
            else:
                run.status = 'stop_delivery_unknown' if sent else 'termination_pending'
        elif terminal and decision['status'] == 'finished' and reliable:
            run.status = 'engine_finished_awaiting_quality'
        else:
            run.status = 'submitted'
        result = run.status
    # Transaction context has successfully committed the send reservation before I/O.
    if dispatch is None:
        return result
    try:
        reply = client.stop(dispatch['engine_job_id'])
        acknowledged = isinstance(reply, dict) and reply.get('accepted') is True
    except Exception:
        acknowledged = False
    with sessions.begin() as db:
        command, run, attempt, supervision, control = _rows(db, command_id)
        now = _now(db)
        effect = db.get(ManagedControlEffect, dispatch['effect_id'])
        effect.phase, effect.updated_at = ('confirmed' if acknowledged else 'unknown'), now
        effect.result = {'accepted': acknowledged, 'termination_confirmed': False,
                         'ownership_retained': True}
        reservation = {**control.facts['real_stop'], 'status': 'acknowledged' if acknowledged else 'unknown'}
        control.facts = {**control.facts, 'real_stop': reservation, 'inflight': reservation}
        supervision.stop_request = {**supervision.stop_request,
            'delivery_status': 'acknowledged' if acknowledged else 'unknown'}
        control.wake_at = now + timedelta(seconds=2)
        run.status = 'stop_requested' if acknowledged else 'stop_delivery_unknown'
        record(db, run, 'control.real_stop_result', {'effect_id': effect.id, **effect.result})
        return run.status
