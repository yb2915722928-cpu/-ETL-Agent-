"""Read-only recovery of the original real-engine attempt. Never a submit path.

HTTP persists intent under the tenant lock, without taking a command lock.
advance_reconciliation acquires command advisory -> tenant -> rows, commits the
GET intent before I/O, and may repeat only that GET after process death. Leases
schedule work; they do not fence writers or authorize cleanup.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import os
from typing import Literal, get_args

from fastapi import BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, ForeignKey, JSON, String, UniqueConstraint, select
from sqlalchemy.orm import Mapped, mapped_column

from .command_signing import COMMAND_FORMAT, sign_envelope, signing_bytes, verify_envelope
from .db import Base, Session
from .engine_adapter import SeaTunnelClient, job_name
from .evidence import append_event, lock_tenant
from .execution import ExecutionRun, ManagedCommand, authorization_bindings, get_run
from .execution_runtime import EngineAttempt, command_lock
from .models import Account, new_id
from .preparations import get_preparation, require_current_role
from .run_control import control_row
from .supervision_runtime import RuntimeSupervision

RECONCILIATION_FORMAT = 'etl-reconciliation-v1'
ReconciliationReason = Literal['submission_unknown', 'worker_lost', 'lease_expired',
                               'stop_unconfirmed', 'operator_requested']


class ReconciliationRecord(Base):
    __tablename__ = 'reconciliation_records'
    __table_args__ = (UniqueConstraint('command_id', 'idempotency_key'),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_commands.id'), index=True)
    actor_id: Mapped[str] = mapped_column(String(36))
    idempotency_key: Mapped[str] = mapped_column(String(128))
    request_sha256: Mapped[str] = mapped_column(String(64))
    envelope: Mapped[dict] = mapped_column(JSON)
    phase: Mapped[str] = mapped_column(String(32), default='accepted')
    result: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ReconcileInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    idempotency_key: str = Field(min_length=1, max_length=128)
    reason: ReconciliationReason


def digest(value):
    return hashlib.sha256(signing_bytes(value)).hexdigest()


def attempt_binding(attempt):
    if attempt is None:
        return None
    return {'engine_job_id': attempt.engine_job_id, 'request': attempt.request,
            'request_sha256': attempt.request_sha256, 'correlation_id': attempt.correlation_id}


def record_view(row):
    return {'record_id': row.id, 'command_id': row.command_id, 'actor_id': row.actor_id,
            'phase': row.phase, 'reason': row.envelope['payload']['reason'],
            'basis': row.result.get('basis', 'intent_persisted'), 'result': row.result,
            'evidence': row.envelope, 'created_at': row.created_at, 'updated_at': row.updated_at}


def reconciliation_view(db, command_id):
    return [record_view(row) for row in db.scalars(select(ReconciliationRecord).where(
        ReconciliationRecord.command_id == command_id).order_by(ReconciliationRecord.created_at,
                                                               ReconciliationRecord.id))]


def verify_command(db, command, run):
    payload = verify_envelope(command.envelope, os.environ['COMMAND_PUBLIC_KEY'], COMMAND_FORMAT)
    preparation = get_preparation(db, run.tenant_id, run.project_id, run.preparation_id)
    expected = {'command_id': command.id, 'run_id': run.id, 'accepted_at': run.accepted_at.isoformat(),
                **authorization_bindings(preparation, run.subject)}
    if (command.tenant_id, command.project_id) != (run.tenant_id, run.project_id) or any(
            payload.get(key) != value for key, value in expected.items()):
        raise ValueError('command_binding_mismatch')
    return payload


def verify_attempt(attempt, command, run, payload):
    if attempt is None:
        return
    request = attempt.request
    expected = {'command_id': command.id, 'run_id': run.id, 'tenant_id': run.tenant_id,
                'project_id': run.project_id, 'correlation_id': attempt.correlation_id,
                **{key: payload.get(key) for key in ('tool', 'environment', 'budget',
                    'authorized_operation', 'runtime_supervision_contract', 'bundle_sha256', 'artifact_hashes')}}
    if digest(request) != attempt.request_sha256 or any(request.get(k) != v for k, v in expected.items()):
        raise ValueError('attempt_binding_mismatch')
    if request.get('adapter') != 'real' or not isinstance(request.get('engine_url'), str) or not request['engine_url']:
        raise ValueError('original_real_engine_unavailable')


def retain_veto(db, command_id):
    supervision = db.get(RuntimeSupervision, command_id)
    if supervision:
        supervision.publication_veto = True
    # Do not manufacture termination, close load admission, clear stop requests,
    # or touch TargetOwnership. Existing control facts remain authoritative.


def register_reconciliation_routes(app, Db, Actor, project_scope):
    @app.post('/api/tenants/{tenant_id}/projects/{project_id}/runs/{run_id}/reconcile', status_code=202)
    def reconcile(tenant_id: str, project_id: str, run_id: str, body: ReconcileInput,
                  request: Request, background_tasks: BackgroundTasks, db: Db, actor: Actor):
        lock_tenant(db, actor.tenant_id)
        try:
            project_scope(db, actor, tenant_id, project_id)
            require_current_role(db, tenant_id, project_id, actor.id, 'operator')
            run = get_run(db, tenant_id, project_id, run_id)
            command = db.scalar(select(ManagedCommand).where(ManagedCommand.run_id == run.id))
            if command is None:
                raise HTTPException(409, '原受管命令尚不可核验')
            request_digest = digest({'actor_id': actor.id, 'reason': body.reason})
            previous = db.scalar(select(ReconciliationRecord).where(
                ReconciliationRecord.command_id == command.id,
                ReconciliationRecord.idempotency_key == body.idempotency_key))
            if previous:
                if previous.request_sha256 != request_digest:
                    raise HTTPException(409, '同一幂等键绑定了不同核对请求')
                if previous.phase in ('accepted', 'observing'):
                    background_tasks.add_task(advance_reconciliation, command.id)
                return record_view(previous)
            payload = verify_command(db, command, run)
            attempt = db.get(EngineAttempt, command.id)
            verify_attempt(attempt, command, run, payload)
            now = datetime.now(timezone.utc)
            record_id = new_id()
            envelope = sign_envelope(request.app.state.evidence_signer, {
                'format': RECONCILIATION_FORMAT, 'record_id': record_id, 'command_id': command.id,
                'run_id': run.id, 'tenant_id': tenant_id, 'project_id': project_id,
                'actor_id': actor.id, 'reason': body.reason, 'idempotency_key': body.idempotency_key,
                'request_sha256': request_digest, 'command_sha256': digest(command.envelope),
                'attempt': attempt_binding(attempt), 'read_only': True})
            row = ReconciliationRecord(id=record_id, command_id=command.id, actor_id=actor.id,
                idempotency_key=body.idempotency_key, request_sha256=request_digest, envelope=envelope,
                phase='accepted', result={}, created_at=now, updated_at=now)
            db.add(row)
            retain_veto(db, command.id)
            control_row(db, command.id).reconciliation_wake_at = now
            append_event(db, actor, 'reconciliation.accepted', {'record_id': record_id,
                'command_id': command.id, 'reason': body.reason, 'intent': envelope}, project_id, run_id=run.id)
            background_tasks.add_task(advance_reconciliation, command.id)
            return record_view(row)
        except (HTTPException, ValueError) as error:
            detail = error.detail if isinstance(error, HTTPException) else '签名命令或原作业绑定核验失败'
            append_event(db, actor, 'reconciliation.denied', {'run_id': run_id, 'reason': detail})
            return JSONResponse(status_code=error.status_code if isinstance(error, HTTPException) else 409,
                                content={'detail': detail})


def advance_reconciliation(command_id):
    """One pending intent, bounded original GET only. Safe to call after restart."""
    with command_lock(command_id) as acquired:
        if not acquired:
            return 'busy'
        with Session.begin() as db:
            command = db.get(ManagedCommand, command_id)
            if command is None:
                return 'missing'
            lock_tenant(db, command.tenant_id)
            row = db.scalar(select(ReconciliationRecord).where(ReconciliationRecord.command_id == command_id,
                ReconciliationRecord.phase.in_(('accepted', 'observing'))).order_by(
                    ReconciliationRecord.created_at, ReconciliationRecord.id).with_for_update())
            if row is None:
                return 'idle'
            run, attempt = db.get(ExecutionRun, command.run_id), db.get(EngineAttempt, command_id)
            retain_veto(db, command_id)
            now = datetime.now(timezone.utc)
            control_row(db, command_id).reconciliation_wake_at = now + timedelta(seconds=15)
            try:
                require_current_role(db, run.tenant_id, run.project_id, row.actor_id, 'operator')
                payload = verify_command(db, command, run)
                verify_attempt(attempt, command, run, payload)
                signed = verify_envelope(row.envelope, os.environ['COMMAND_PUBLIC_KEY'], RECONCILIATION_FORMAT)
                expected = {'record_id': row.id, 'command_id': command.id, 'run_id': run.id,
                    'tenant_id': run.tenant_id, 'project_id': run.project_id, 'actor_id': row.actor_id,
                    'idempotency_key': row.idempotency_key, 'request_sha256': row.request_sha256,
                    'command_sha256': digest(command.envelope), 'attempt': attempt_binding(attempt), 'read_only': True}
                if signed.get('reason') not in get_args(ReconciliationReason):
                    raise ValueError('reconciliation_reason_invalid')
                if digest({'actor_id': row.actor_id, 'reason': signed.get('reason')}) != row.request_sha256:
                    raise ValueError('reconciliation_request_mismatch')
                if any(signed.get(k) != v for k, v in expected.items()):
                    raise ValueError('reconciliation_binding_mismatch')
                if attempt is None or not attempt.engine_job_id:
                    return finish(db, row, run, 'unknown', 'original_job_id_missing')
                identifier = SeaTunnelClient.validate_job_id(attempt.engine_job_id)
            except (HTTPException, ValueError):
                return finish(db, row, run, 'unknown', 'authorization_or_binding_unavailable')
            row.phase, row.updated_at = 'observing', now
            record_id, url = row.id, attempt.request['engine_url']
            append_event(db, db.get(Account, row.actor_id), 'reconciliation.get_intent', {
                'record_id': row.id, 'engine_job_id': identifier, 'read_only': True}, run.project_id, run_id=run.id)
        # The preceding transaction is committed before this external read. A
        # process dying here leaves observing, recoverable by the identical GET.
        try:
            proof = SeaTunnelClient(url).observe(identifier, job_name(command_id))
        except (OSError, ValueError, TypeError):
            proof = None
        with Session.begin() as db:
            lock_tenant(db, command.tenant_id)
            row, run = db.get(ReconciliationRecord, record_id), db.get(ExecutionRun, command.run_id)
            if proof is None:
                return finish(db, row, run, 'unknown', 'original_job_unconfirmed')
            # Store a whitelist only; engine metrics/messages may contain secrets.
            facts = {key: proof[key] for key in ('job_id', 'job_name', 'engine_state', 'engine_terminal')}
            return finish(db, row, run, 'observed', 'original_id_name_status', facts)


def finish(db, row, run, phase, basis, proof=None):
    row.phase, row.updated_at = phase, datetime.now(timezone.utc)
    row.result = {'basis': basis, **(proof or {}), 'sink_quiescent': None,
        'publication_veto': True, 'cleanup_authorized': False, 'ownership_released': False,
        'real_l2_claim': False}
    retain_veto(db, row.command_id)
    append_event(db, db.get(Account, row.actor_id), 'reconciliation.' + phase, {
        'record_id': row.id, 'result': row.result}, run.project_id, run_id=run.id)
    return phase
