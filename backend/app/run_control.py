"""Durable L1 run control: intent, external effect, terminal proof and cleanup.

Lock order is command advisory -> tenant -> control/effect rows. HTTP accepts
intent under tenant only; it never waits for the command lock. A lease schedules
reconciliation; it cannot fence an already sent HTTP call or release a target.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import os
import socket

from cryptography.exceptions import InvalidTag
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, ForeignKey, JSON, String, UniqueConstraint, func, select
from sqlalchemy.orm import Mapped, mapped_column

from .command_signing import COMMAND_FORMAT, sign_envelope, signing_bytes, verify_envelope
from .db import Base
from .evidence import append_event, lock_tenant
from .execution import ExecutionRun, ManagedCommand, Outbox, get_run
from .managed_secrets import SecretProvider
from .models import Account, SecretReference, new_id
from .preparations import current_role, decisions_for, get_preparation, require_current_role, SLOTS
from .runtime_supervision import COUNTERS, evaluate_observation
from .supervision_runtime import RuntimeObservation, RuntimeSupervision, request_stop

CONTROL_FORMAT = 'etl-managed-control-v1'


class RunControl(Base):
    __tablename__ = 'run_controls'
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_commands.id'), primary_key=True)
    facts: Mapped[dict] = mapped_column(JSON, default=dict)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    wake_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ManagedControlEffect(Base):
    __tablename__ = 'managed_control_effects'
    __table_args__ = (UniqueConstraint('command_id', 'kind', 'idempotency_key'),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_commands.id'), index=True)
    kind: Mapped[str] = mapped_column(String(16))
    actor_id: Mapped[str] = mapped_column(String(36))
    idempotency_key: Mapped[str] = mapped_column(String(128))
    request_sha256: Mapped[str] = mapped_column(String(64))
    envelope: Mapped[dict] = mapped_column(JSON)
    phase: Mapped[str] = mapped_column(String(32), default='accepted')
    result: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ControlInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    idempotency_key: str = Field(min_length=1, max_length=128)
    reason: str = Field(min_length=1, max_length=256)


def control_row(db, command_id):
    row = db.get(RunControl, command_id)
    if row is None:
        row = RunControl(command_id=command_id, facts={})
        db.add(row)
        db.flush()
    return row


def effects_for(db, command_id):
    return list(db.scalars(select(ManagedControlEffect).where(ManagedControlEffect.command_id == command_id)
                           .order_by(ManagedControlEffect.created_at, ManagedControlEffect.id)))


def control_view(db, command_id):
    row = db.get(RunControl, command_id)
    return {**(row.facts if row else {}), 'termination': (row.facts if row else {}).get('termination', {'confirmed': False}),
        'final_metrics': (row.facts if row else {}).get('final_metrics', {'status': 'pending', 'overshoot': None}),
        'lease': {key: getattr(row, key) for key in ('lease_owner', 'lease_token', 'last_seen_at', 'lease_expires_at', 'wake_at')} if row else None,
        'effects': [{key: getattr(effect, key) for key in ('id', 'kind', 'actor_id', 'phase', 'envelope',
                    'request_sha256', 'result', 'created_at', 'updated_at')} for effect in effects_for(db, command_id)]}


def check_bound_secrets(db, command):
    """Open the original references only; never consult a Connection replacement."""
    provider = SecretProvider()
    for asset in command.envelope['payload']['resources'].values():
        reference_id = asset.get('secret_reference_id')
        if reference_id is None:
            continue
        ref = db.get(SecretReference, reference_id, populate_existing=True)
        if ref is None or (ref.tenant_id, ref.project_id, ref.version) != (
                command.tenant_id, command.project_id, asset['secret_version']):
            raise HTTPException(409, '原绑定凭据引用或版本不可用；禁止切换凭据')
        try:
            provider.open(ref)
        except InvalidTag:
            raise HTTPException(409, '原绑定凭据不可解密；禁止切换凭据') from None


def check_running_authorization(db, command, run):
    require_current_role(db, run.tenant_id, run.project_id, run.subject, 'operator')
    preparation = get_preparation(db, run.tenant_id, run.project_id, run.preparation_id)
    if not current_role(db, run.tenant_id, run.project_id, preparation.maker_id, 'maker'):
        raise HTTPException(403, '原申请人的当前 Maker 职责失效')
    for decision in decisions_for(db, preparation):
        require_current_role(db, run.tenant_id, run.project_id, decision.checker_id, SLOTS[decision.slot])
    check_bound_secrets(db, command)


def cleanup_binding(db, command, run, control):
    from .execution_runtime import EngineAttempt, TargetOwnership
    attempt = db.get(EngineAttempt, command.id)
    proof = control.facts.get('termination', {})
    if not proof.get('confirmed') or proof.get('basis') != 'engine_terminal':
        raise HTTPException(409, '尚未确认引擎终止；停止 ACK、失联或 lease 到期不能允许清理')
    if control.facts.get('inflight'):
        raise HTTPException(409, '在途变更结果尚未核对，禁止清理')
    if control.facts.get('final_metrics', {}).get('status') != 'known':
        raise HTTPException(409, '最终指标尚未持久确认，暂不清理')
    ownership = db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == command.id))
    if ownership is None or attempt is None or proof.get('publication') != 'unpublished' or (
            proof.get('unpublished_object') != attempt.request.get('unpublished_object')):
        raise HTTPException(409, '本运行未发布对象身份或目标占有不明确，禁止清理')
    check_bound_secrets(db, command)
    return {'target_key': ownership.target_key, 'target': ownership.identity,
            'unpublished_object': proof['unpublished_object'], 'engine_job_id': attempt.engine_job_id,
            'correlation_id': attempt.correlation_id, 'submission_sha256': attempt.request_sha256}


def wake(db, command, control, now):
    control.wake_at = now
    outbox = db.scalar(select(Outbox).where(Outbox.command_id == command.id))
    outbox.status = 'pending'


def register_control_routes(app, Db, Actor, project_scope):
    @app.post('/api/tenants/{tenant_id}/projects/{project_id}/runs/{run_id}/{kind}', status_code=202)
    def request_control(tenant_id: str, project_id: str, run_id: str, kind: str,
                        body: ControlInput, request: Request, db: Db, actor: Actor):
        if kind not in ('stop', 'cleanup'):
            raise HTTPException(404, '未登记运行控制操作')
        lock_tenant(db, actor.tenant_id)
        try:
            project_scope(db, actor, tenant_id, project_id)
            require_current_role(db, tenant_id, project_id, actor.id, 'operator')
            run = get_run(db, tenant_id, project_id, run_id)
            command = db.scalar(select(ManagedCommand).where(ManagedCommand.run_id == run.id))
            control = control_row(db, command.id)
            digest = hashlib.sha256(signing_bytes({'actor_id': actor.id, 'kind': kind, 'reason': body.reason})).hexdigest()
            previous = db.scalar(select(ManagedControlEffect).where(ManagedControlEffect.command_id == command.id,
                ManagedControlEffect.kind == kind, ManagedControlEffect.idempotency_key == body.idempotency_key))
            if previous:
                if previous.request_sha256 != digest:
                    raise HTTPException(409, '同一幂等键绑定了不同控制请求')
                return {'effect_id': previous.id, 'phase': previous.phase}
            now = datetime.now(timezone.utc)
            binding = cleanup_binding(db, command, run, control) if kind == 'cleanup' else {}
            if kind == 'stop' and run.status in ('denied', 'blocked_target', 'metadata_checked', 'cancelled', 'cleaned', 'terminated'):
                raise HTTPException(409, '该运行当前状态不能新建停止请求')
            if kind == 'cleanup' and any(e.kind == 'cleanup' and e.phase not in ('denied', 'confirmed') for e in effects_for(db, command.id)):
                raise HTTPException(409, '已有清理效果正在核对，请查询原请求')
            effect_id = new_id()
            payload = {'format': CONTROL_FORMAT, 'effect_id': effect_id, 'command_id': command.id,
                'run_id': run.id, 'tenant_id': tenant_id, 'project_id': project_id, 'actor_id': actor.id,
                'kind': kind, 'reason': body.reason, 'resources': command.envelope['payload']['resources'],
                'budget': {'http_timeout_seconds': 5, 'scope': 'only_this_run_unpublished_object'}, **binding}
            effect = ManagedControlEffect(id=effect_id, command_id=command.id, kind=kind, actor_id=actor.id,
                idempotency_key=body.idempotency_key, request_sha256=digest,
                envelope=sign_envelope(request.app.state.evidence_signer, payload), created_at=now, updated_at=now)
            db.add(effect)
            if kind == 'stop':
                supervision = db.get(RuntimeSupervision, command.id)
                if supervision is None:
                    raise HTTPException(409, '此运行无数据变更监督契约')
                request_stop(db, run, supervision, ['operator_stop'], now)
            wake(db, command, control, now)
            append_event(db, actor, 'control.' + kind + '_accepted', {'effect_id': effect.id,
                'command_id': command.id, 'reason': body.reason, 'binding': binding}, project_id, run_id=run.id)
            return {'effect_id': effect.id, 'phase': 'accepted'}
        except HTTPException as error:
            append_event(db, actor, 'control.' + kind + '_denied', {'reason': error.detail, 'run_id': run_id})
            return JSONResponse(status_code=error.status_code, content={'detail': error.detail})


def ensure_stop_effect(db, command, run, supervision, now):
    effects = effects_for(db, command.id)
    if any(effect.kind == 'stop' for effect in effects):
        return
    # Automatic safety stop is a derived managed effect under the original signed
    # supervision contract, not a new Worker signing authority or cleanup grant.
    db.add(ManagedControlEffect(id=supervision.stop_request['id'], command_id=command.id, kind='stop',
        actor_id=run.subject, idempotency_key='automatic-safety-stop', request_sha256=hashlib.sha256(
            signing_bytes(command.envelope)).hexdigest(), envelope={'authorization': command.envelope,
            'derivation': {'command_id': command.id, 'request_id': supervision.stop_request['id'], 'kind': 'stop'}},
        created_at=now, updated_at=now))


def verify_effect(effect, command, run):
    if 'authorization' in effect.envelope:
        payload = verify_envelope(effect.envelope['authorization'], os.environ['COMMAND_PUBLIC_KEY'], COMMAND_FORMAT)
        if effect.kind != 'stop' or payload != command.envelope['payload'] or not payload.get('runtime_supervision_contract'):
            raise HTTPException(403, '自动安全停止授权不匹配')
        return
    payload = verify_envelope(effect.envelope, os.environ['COMMAND_PUBLIC_KEY'], CONTROL_FORMAT)
    if any(payload.get(k) != v for k, v in {'effect_id': effect.id, 'command_id': command.id, 'run_id': run.id,
        'tenant_id': run.tenant_id, 'project_id': run.project_id, 'actor_id': effect.actor_id, 'kind': effect.kind,
        'resources': command.envelope['payload']['resources']}.items()):
        raise HTTPException(403, '受管控制效果范围或签名绑定不一致')


def final_facts(contract, sample, metrics, detection, now):
    limits = {'source_read': 'max_rows', 'sink_bytes': 'max_bytes', 'elapsed_seconds': 'max_seconds',
              'amplification': 'max_amplification', 'reject_ratio': 'max_reject_ratio'}
    return {'status': 'known', 'sample': sample, 'received_at': now.isoformat(), 'metrics': metrics,
        'overshoot': {key: max(metrics[key] - contract[limit], 0) if metrics[key] is not None else None for key, limit in limits.items()},
        'post_detection_delta': {key: sample[key] - detection[key] if detection and detection.get(key) is not None else None for key in COUNTERS}}


def advance_control(command_id):
    """One bounded step under command advisory lock; all effects commit before I/O."""
    from . import execution_runtime as runtime
    with runtime.Session.begin() as db:
        command = db.get(ManagedCommand, command_id)
        lock_tenant(db, command.tenant_id)
        run = db.get(ExecutionRun, command.run_id)
        supervision = db.get(RuntimeSupervision, command_id)
        attempt = db.get(runtime.EngineAttempt, command_id)
        control = control_row(db, command_id)
        now = db.scalar(select(func.clock_timestamp()))
        token = new_id()
        if control.lease_expires_at and control.lease_expires_at < now:
            runtime.record(db, run, 'control.lease_reconciled', {'previous_owner': control.lease_owner,
                'inflight': control.facts.get('inflight'), 'ownership_retained': True})
        control.lease_owner, control.lease_token, control.last_seen_at = socket.gethostname() + ':' + str(os.getpid()), token, now
        control.lease_expires_at = now + timedelta(seconds=15)
        control.wake_at = control.lease_expires_at
        if run.status in ('cleaned', 'cancelled'):
            control.wake_at = None
            return run.status
        if not attempt or not attempt.engine_job_id or supervision is None:
            return run.status
        if attempt.request['engine_url'] != os.environ['L1_ENGINE_URL']:
            runtime.record(db, run, 'control.engine_unknown', {'reason': '原引擎配置变化', 'ownership_retained': True})
            return 'termination_pending'
        try:
            check_running_authorization(db, command, run)
        except HTTPException as error:
            if not supervision.stop_request:
                request_stop(db, run, supervision, ['authorization_revoked_or_secret_unavailable'], now)
                runtime.record(db, run, 'control.authorization_veto', {'reason': error.detail, 'ownership_retained': True})
        # Persisted calls left by a dead Worker are discovered, never replayed.
        inflight = control.facts.get('inflight')
        if inflight:
            operation, effect_id = 'reconcile', inflight['effect_id']
            path = '/effects/' + effect_id
            body = None
        elif run.status in ('terminated', 'supervision_passed'):
            # Observation is finished once the run is terminal; only a pending
            # management effect can still need dispatching.
            if not [effect for effect in effects_for(db, command_id) if effect.phase == 'accepted']:
                control.wake_at = None
                return run.status
        else:
            try:
                proof = runtime.engine_request('GET', '/jobs/' + attempt.engine_job_id + '/status')
            except (OSError, ValueError):
                proof = None
            now = datetime.now(timezone.utc)
            terminal = bool(proof and proof.get('id') == attempt.engine_job_id and
                proof.get('correlation_id') == attempt.correlation_id and proof.get('request_sha256') == attempt.request_sha256 and
                proof.get('state') in ('finished', 'stopped', 'failed') and proof.get('inflight_writes') == 0 and
                proof.get('unpublished_object') == attempt.request.get('unpublished_object'))
            if terminal and not control.facts.get('termination', {}).get('confirmed'):
                control.facts = {**control.facts, 'termination': {**proof, 'confirmed': True, 'basis': 'engine_terminal',
                    'confirmed_at': now.isoformat()}}
                if supervision.stop_request:
                    supervision.stop_request = {**supervision.stop_request, 'termination_confirmed': True}
                runtime.record(db, run, 'control.termination_confirmed', control.facts['termination'])
            try:
                sample = runtime.engine_request('GET', '/jobs/' + attempt.engine_job_id + '/metrics')
            except (OSError, ValueError):
                sample = None
            decision = evaluate_observation(supervision.contract, sample, supervision.last_trusted, now)
            reliable = all(decision['metrics'][key] is not None for key in COUNTERS)
            if (now - attempt.created_at).total_seconds() > supervision.contract['max_seconds'] and not terminal:
                decision['reasons'] = list(dict.fromkeys([*decision['reasons'], 'duration_exceeded']))
            db.add(RuntimeObservation(command_id=command_id, received_at=now, source=sample.get('source') if sample else None,
                                      sample=sample, decision=decision))
            supervision.current_metrics, supervision.updated_at = decision['metrics'], now
            if reliable:
                supervision.last_trusted = sample
            if not supervision.stop_request and decision['reasons']:
                request_stop(db, run, supervision, decision['reasons'], now)
            if supervision.stop_request:
                supervision.publication_veto = True
            else:
                supervision.status, supervision.reasons = decision['status'], decision['reasons']
                supervision.publication_veto = not (terminal and decision['status'] == 'finished')
            if control.facts.get('termination', {}).get('confirmed'):
                if reliable and sample['state'] == 'finished':
                    if control.facts.get('final_metrics', {}).get('status') != 'known':
                        control.facts = {**control.facts, 'final_metrics': final_facts(supervision.contract, sample,
                            decision['metrics'], (supervision.stop_request or {}).get('detection_metrics'), now)}
                        runtime.record(db, run, 'control.final_metrics', control.facts['final_metrics'])
                    run.status = 'terminated' if supervision.stop_request else 'supervision_passed'
                else:
                    control.facts = {**control.facts, 'final_metrics': {'status': 'pending', 'overshoot': None,
                        'reason': decision['reasons']}}
                    run.status = 'final_metrics_pending'
            else:
                run.status = 'stop_requested' if supervision.stop_request else 'submitted'
        if supervision.stop_request:
            ensure_stop_effect(db, command, run, supervision, now)
            db.flush()
        pending = [e for e in effects_for(db, command_id) if e.phase == 'accepted']
        # Stop effects sharing the same durable request acknowledge one outcome.
        if supervision.stop_request and supervision.stop_request['delivery_status'] == 'acknowledged':
            for effect in pending:
                if effect.kind == 'stop':
                    effect.phase, effect.result, effect.updated_at = 'confirmed', {'accepted': True, 'request_id': supervision.stop_request['id']}, now
            pending = [e for e in pending if e.kind != 'stop']
        if not inflight and not pending:
            if run.status in ('terminated', 'supervision_passed'):
                control.wake_at = None
            return run.status
        if inflight:
            # A call left behind by a dead Worker keeps its durable intent: the
            # effect is settled only by the read-back below and never replayed.
            effect = db.get(ManagedControlEffect, inflight['effect_id'])
        else:
            effect = pending[0]
            try:
                verify_effect(effect, command, run)
                if effect.kind == 'cleanup':
                    require_current_role(db, run.tenant_id, run.project_id, effect.actor_id, 'operator')
                    binding = cleanup_binding(db, command, run, control)
                    if any(effect.envelope['payload'].get(k) != v for k, v in binding.items()):
                        raise HTTPException(409, '清理对象范围已变化')
                elif not supervision.stop_request:
                    raise HTTPException(409, '停止意图缺失')
            except (HTTPException, ValueError) as error:
                effect.phase, effect.result = 'denied', {'reason': str(error.detail) if isinstance(error, HTTPException) else str(error)}
                runtime.record(db, run, 'control.effect_denied', {'effect_id': effect.id, **effect.result})
                return run.status
            operation, effect_id = effect.kind, effect.id
            request_id = supervision.stop_request['id'] if operation == 'stop' else effect.id
            body = {'request_id': request_id, 'effect_id': effect.id, 'job_id': attempt.engine_job_id,
                'correlation_id': attempt.correlation_id, 'request_sha256': attempt.request_sha256,
                'unpublished_object': attempt.request['unpublished_object']}
            path = '/jobs/' + attempt.engine_job_id + '/' + operation
            effect.phase, effect.updated_at = 'dispatching', now
            control.facts = {**control.facts, 'inflight': {'effect_id': effect.id, 'kind': operation,
                'request': body, 'started_at': now.isoformat(), 'lease_token': token, 'status': 'dispatching'}}
            runtime.record(db, run, 'control.effect_dispatch_prepared', control.facts['inflight'])
    # Intent is durable. Hold the shared tenant lock from reauthorization through
    # the bounded mutation call, matching membership/account mutation ordering.
    with runtime.Session.begin() as db:
        lock_tenant(db, command.tenant_id)
        run, control = db.get(ExecutionRun, command.run_id), db.get(RunControl, command_id)
        if control.lease_token != token:
            return 'busy'
        effect = db.get(ManagedControlEffect, effect_id)
        supervision = db.get(RuntimeSupervision, command_id)
        if operation == 'cleanup':
            try:
                require_current_role(db, run.tenant_id, run.project_id, effect.actor_id, 'operator')
                check_bound_secrets(db, command)
            except HTTPException as error:
                effect.phase, effect.result = 'denied', {'reason': error.detail}
                control.facts = {**control.facts, 'inflight': None}
                runtime.record(db, run, 'control.effect_denied', {'effect_id': effect_id, 'reason': error.detail})
                return run.status
        dispatched_at = datetime.now(timezone.utc).isoformat()
        if operation == 'stop':
            supervision.stop_request = {**supervision.stop_request, 'dispatched_at': dispatched_at}
        try:
            result = runtime.engine_request('GET' if operation == 'reconcile' else 'POST', path, body)
            inflight = control.facts['inflight']
            expected = inflight['request']
            matched = all(result.get(key) == value for key, value in expected.items())
            kind = inflight['kind']
            confirmed = matched and (result.get('accepted') is True if kind == 'stop' else result.get('cleaned') is True)
        except (OSError, ValueError):
            result, confirmed = None, False
            kind = control.facts['inflight']['kind']
        now = datetime.now(timezone.utc)
        if confirmed:
            effect.phase, effect.result, effect.updated_at = 'confirmed', result, now
            control.facts = {**control.facts, 'inflight': None}
            if kind == 'stop':
                supervision.stop_request = {**supervision.stop_request, 'delivery_status': 'acknowledged',
                    'dispatched_at': supervision.stop_request.get('dispatched_at', control.facts.get('dispatch_prepared_at')),
                    'acknowledged_at': now.isoformat()}
                run.status = 'stop_requested'
            else:
                owner = db.scalar(select(runtime.TargetOwnership).where(runtime.TargetOwnership.command_id == command_id).with_for_update())
                binding = effect.envelope['payload']
                if owner and owner.target_key == binding['target_key'] and owner.identity == binding['target']:
                    db.delete(owner)
                    run.status = 'cleaned'
                    control.wake_at = None
        else:
            effect.phase, effect.result, effect.updated_at = 'unknown', {'reason': '外部效果待核对；禁止重送或释放目标'}, now
            control.facts = {**control.facts, 'inflight': {**control.facts['inflight'], 'status': 'unknown'}}
            if kind == 'stop':
                supervision.stop_request = {**supervision.stop_request, 'delivery_status': 'unknown'}
            run.status = 'cleanup_unknown' if kind == 'cleanup' else 'stop_delivery_unknown'
        runtime.record(db, run, 'control.effect_' + effect.phase, {'effect_id': effect_id, 'kind': kind,
            'result': effect.result, 'ownership_retained': run.status != 'cleaned'})
        return run.status
