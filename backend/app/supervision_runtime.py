"""Durable observations and stop intent. Termination confirmation/cleanup belongs to V11."""
from datetime import datetime, timezone
from sqlalchemy import Boolean, DateTime, ForeignKey, JSON, String, select
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base
from .models import new_id
from .runtime_supervision import evaluate_observation


class RuntimeSupervision(Base):
    __tablename__ = 'runtime_supervisions'
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_commands.id'), primary_key=True)
    contract: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(32), default='awaiting_metrics')
    publication_veto: Mapped[bool] = mapped_column(Boolean, default=True)
    reasons: Mapped[list] = mapped_column(JSON, default=list)
    current_metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    last_trusted: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    stop_request: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class RuntimeObservation(Base):
    __tablename__ = 'runtime_observations'
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_commands.id'), index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sample: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    decision: Mapped[dict] = mapped_column(JSON)


def supervision_view(db, command_id):
    row = db.get(RuntimeSupervision, command_id)
    if row is None:
        return None
    observation = db.scalar(select(RuntimeObservation).where(RuntimeObservation.command_id == command_id)
                            .order_by(RuntimeObservation.received_at.desc()).limit(1))
    return {**{key: getattr(row, key) for key in ('contract', 'status', 'publication_veto', 'reasons',
             'current_metrics', 'stop_request', 'updated_at')},
            'latest_observation': {'id': observation.id, 'received_at': observation.received_at,
                'source': observation.source, 'sample': observation.sample, 'decision': observation.decision} if observation else None,
            'termination': '尚未确认；停止响应仅表示请求已接收，目标占有持续保留'}


def request_stop(db, run, row, reasons, now):
    """Public seam for V11 revocation and termination handling; caller owns tenant/command locks."""
    from .execution_runtime import record
    row.publication_veto = True
    row.reasons = list(dict.fromkeys([*row.reasons, *reasons]))
    row.status = 'stop_requested'
    run.status = 'stop_requested'
    if row.stop_request is None:
        row.stop_request = {'id': new_id(), 'detected_at': now.isoformat(), 'requested_at': now.isoformat(),
                            'reasons': row.reasons, 'delivery_status': 'pending', 'termination_confirmed': False}
        record(db, run, 'supervision.stop_requested', {'command_id': row.command_id, **row.stop_request,
            'publication_veto': True, 'ownership_retained': True})


def supervise_locked(command_id):
    """Called under command_lock. Stop intent commits before bounded external I/O."""
    from . import execution_runtime as runtime
    from .execution import ExecutionRun, ManagedCommand
    from .evidence import lock_tenant
    with runtime.Session.begin() as db:
        command = db.get(ManagedCommand, command_id)
        lock_tenant(db, command.tenant_id)
        run = db.get(ExecutionRun, command.run_id)
        row = db.get(RuntimeSupervision, command_id)
        attempt = db.get(runtime.EngineAttempt, command_id)
        if row is None:
            # Older accepted commands did not authorize this metric contract.
            run.status = 'supervision_unavailable'
            runtime.record(db, run, 'supervision.unavailable', {'reason': '冻结监督契约缺失，禁止发布', 'ownership_retained': True})
            return run.status
        if row.status == 'finished' or (row.stop_request and row.stop_request['delivery_status'] == 'acknowledged'):
            return run.status
        now = datetime.now(timezone.utc)
        if not row.stop_request:
            try:
                if attempt.request['engine_url'] != runtime.os.environ['L1_ENGINE_URL']:
                    raise ValueError('original engine changed')
                sample = runtime.engine_request('GET', '/jobs/' + attempt.engine_job_id + '/metrics')
            except (OSError, ValueError):
                sample = None
            now = datetime.now(timezone.utc)
            decision = evaluate_observation(row.contract, sample, row.last_trusted, now)
            # Wall clock budget cannot be bypassed by a frozen engine elapsed counter.
            if (now - attempt.created_at).total_seconds() > row.contract['max_seconds']:
                decision['reasons'] = list(dict.fromkeys([*decision['reasons'], 'duration_exceeded']))
                decision['status'] = 'unsafe'
            db.add(RuntimeObservation(command_id=command_id, received_at=now,
                source=sample.get('source') if sample else None, sample=sample, decision=decision))
            row.current_metrics, row.updated_at = decision['metrics'], now
            if decision['status'] != 'unsafe':
                row.last_trusted = sample
            row.status, row.reasons = decision['status'], decision['reasons']
            # Awaiting/running observations never grant permission to publish.
            row.publication_veto = decision['status'] != 'finished'
            runtime.record(db, run, 'supervision.observed', {'command_id': command_id, **decision,
                'source': sample.get('source') if sample else None, 'publication_veto': row.publication_veto})
            if decision['reasons']:
                request_stop(db, run, row, decision['reasons'], now)
            elif decision['status'] == 'finished':
                run.status = 'supervision_passed'
                return run.status
            else:
                return 'submitted'
        stop = row.stop_request
        engine_job_id, engine_url = attempt.engine_job_id, attempt.request['engine_url']
    try:
        if engine_url != runtime.os.environ['L1_ENGINE_URL']:
            raise ValueError('original engine changed')
        response = runtime.engine_request('POST', '/jobs/' + engine_job_id + '/stop', {'request_id': stop['id']})
        acknowledged = response.get('request_id') == stop['id'] and response.get('accepted') is True
        delivery = 'acknowledged' if acknowledged else 'unknown'
    except (OSError, ValueError):
        delivery = 'unknown'
    with runtime.Session.begin() as db:
        lock_tenant(db, command.tenant_id)
        row, run = db.get(RuntimeSupervision, command_id), db.get(ExecutionRun, command.run_id)
        row.stop_request = {**row.stop_request, 'delivery_status': delivery,
                            'last_delivery_at': datetime.now(timezone.utc).isoformat()}
        runtime.record(db, run, 'supervision.stop_delivery', {'command_id': command_id, **row.stop_request,
            'publication_veto': True, 'ownership_retained': True})
        return 'stop_requested' if delivery == 'acknowledged' else 'stop_delivery_unknown'
