"""L1 command execution. PostgreSQL owns deduplication and physical isolation.

A committed submitting phase is irreversible: every later delivery discovers the
original correlation only. Unknown outcomes never release target ownership.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
import urllib.request

from fastapi import HTTPException
from sqlalchemy import DateTime, ForeignKey, JSON, String, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Mapped, mapped_column

from .command_signing import COMMAND_FORMAT, signing_bytes, verify_envelope
from .connector_policy import ResourcePolicy
from .db import Base, Session, engine
from .evidence import append_event, lock_tenant
from .execution import ExecutionRun, ManagedCommand, authorization_bindings
from .managed_secrets import SecretProvider
from .models import Account, EvidenceEvent, new_id
from .preparations import check_execution_resources, get_preparation, require_current_role, validate_preparation
from .versions import get_version
from .supervision_runtime import RuntimeSupervision, supervision_view


class EngineAttempt(Base):
    __tablename__ = 'engine_attempts'
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_commands.id'), primary_key=True)
    correlation_id: Mapped[str] = mapped_column(String(36), unique=True)
    request_sha256: Mapped[str] = mapped_column(String(64))
    request: Mapped[dict] = mapped_column(JSON)
    phase: Mapped[str] = mapped_column(String(32))
    reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
    engine_job_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TargetOwnership(Base):
    __tablename__ = 'target_ownership'
    target_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    identity: Mapped[dict] = mapped_column(JSON)
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_commands.id'), unique=True)
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


@contextmanager
def command_lock(command_id):
    # Session lock survives transaction boundaries; death closes the connection.
    with engine.connect() as connection:
        key = int.from_bytes(hashlib.sha256(command_id.encode()).digest()[:8], 'big', signed=True)
        acquired = connection.scalar(text('SELECT pg_try_advisory_lock(:key)'), {'key': key})
        connection.commit()
        try:
            yield acquired
        finally:
            if acquired:
                connection.execute(text('SELECT pg_advisory_unlock(:key)'), {'key': key})
                connection.commit()


def record(db, run, action, facts):
    append_event(db, db.get(Account, run.subject), action, facts, run.project_id,
                 preparation_id=run.preparation_id, run_id=run.id)


def execution_view(db, command_id):
    attempt = db.get(EngineAttempt, command_id)
    ownership = db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == command_id))
    command = db.get(ManagedCommand, command_id)
    from .run_control import control_view
    from .reconciliation import reconciliation_view
    from .quality_runtime import QualityRun
    from .real_publication import publication_view
    quality = db.get(QualityRun, command.run_id) if command else None
    events = list(db.scalars(select(EvidenceEvent).where(EvidenceEvent.run_id == command.run_id,
        EvidenceEvent.scope == command.project_id).order_by(EvidenceEvent.sequence.desc()).limit(20)))
    adapter = attempt.request.get('adapter') if attempt else os.environ.get('EXECUTION_ADAPTER', 'disabled')
    return {'level': {'l1': 'L1', 'real': 'real'}.get(adapter, 'disabled'),
            'attempt': {key: getattr(attempt, key) for key in ('correlation_id', 'request_sha256', 'request',
                'phase', 'reason', 'engine_job_id', 'created_at', 'updated_at')} if attempt else None,
            'ownership': {'target_key': ownership.target_key, 'identity': ownership.identity,
                          'acquired_at': ownership.acquired_at, 'retained': True} if ownership else None,
            'control_facts': [{'action': event.action, 'occurred_at': event.occurred_at,
                               'facts': event.facts} for event in reversed(events)],
            'supervision': supervision_view(db, command_id),
            'control': control_view(db, command_id),
            'reconciliation': reconciliation_view(db, command_id),
            'publication_state': publication_view(db, command_id),
            'quality': {'contract_sha256': quality.contract_sha256, 'source_facts': quality.source_facts,
                        'failures': quality.failures, 'finalized': quality.finalized, 'report': quality.report,
                        'real_l2_qualified': False} if quality else None,
            'publication': ('L1 仅确认模拟未发布对象清理；不执行真实 ETL、发布或回退'
                if os.environ.get('EXECUTION_ADAPTER') == 'l1'
                else '真实受管搬运尚未通过 L2 验收；发布与回退闭锁')}


def engine_url():
    """本次运行绑定的引擎地址；适配器切换后不得向其他引擎发现。"""
    if os.environ.get('EXECUTION_ADAPTER', 'disabled') == 'real':
        return os.environ['REAL_ENGINE_URL']
    return os.environ['L1_ENGINE_URL']


def engine_request(method, path, body=None):
    if os.environ.get('EXECUTION_ADAPTER', 'disabled') == 'real':
        raise ValueError('真实受管执行尚未完成字节监督、写入凭据与清理验收；禁止数据面调用')
    request = urllib.request.Request(os.environ['L1_ENGINE_URL'] + path,
        data=signing_bytes(body) if body is not None else None, method=method,
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def _check_submission_authorization(db, policy, secrets, run, row=None):
    from .run_control import check_bound_secrets
    command = db.scalar(select(ManagedCommand).where(ManagedCommand.run_id == run.id))
    check_bound_secrets(db, command)
    require_current_role(db, run.tenant_id, run.project_id, run.subject, 'operator')
    row = row or get_preparation(db, run.tenant_id, run.project_id, run.preparation_id, lock=True)
    validation = validate_preparation(db, policy, row, check_expiry=False)
    if not validation['valid']:
        raise HTTPException(409, '；'.join(validation['reasons']))
    version = get_version(db, run.tenant_id, run.project_id, row.version_id)
    actor = db.get(Account, run.subject)
    check_execution_resources(db, policy, secrets, version, actor, 'operator', preparation_id=row.id)
    if row.frozen['execution_facts']['target'].get('execution_writer'):
        from .writer_credentials import check_execution_writer
        check_execution_writer(db, policy, secrets, version, actor, 'operator', preparation_id=row.id)


def drive(command_id):
    from .run_control import advance_control
    state = _submit(command_id)
    if state not in ('submitted', 'submission_unknown', 'stop_requested', 'stop_delivery_unknown', 'termination_pending',
                     'final_metrics_pending', 'engine_finished_awaiting_quality', 'terminated', 'supervision_passed', 'cleanup_unknown'):
        return state
    with command_lock(command_id) as acquired:
        if not acquired:
            return 'busy'
        state = advance_control(command_id)
        if state == 'engine_finished_awaiting_quality':
            from .real_finalization import finalize_real
            state = finalize_real(command_id, sessions=Session)
        if state in ('terminated', 'final_metrics_pending'):
            from .real_finalization import finalize_real_quality_diagnostics
            finalize_real_quality_diagnostics(command_id, sessions=Session)
        return state


def _submit(command_id):
    if os.environ.get('EXECUTION_ADAPTER', 'disabled') not in ('l1', 'real'):
        raise RuntimeError('执行适配器未启用；普通部署不得提交引擎')
    if os.environ.get('EXECUTION_ADAPTER') == 'real':
        # Protocol selection does not grant authority. The real module rechecks
        # the signed command and takes its own command lock before any mutation.
        with Session() as routing_db:
            original = routing_db.get(ManagedCommand, command_id)
            run = routing_db.get(ExecutionRun, original.run_id) if original else None
            preparation = get_preparation(routing_db, run.tenant_id, run.project_id, run.preparation_id) if run else None
            real_mutation = preparation is not None and preparation.tool in ('pipeline.trial', 'pipeline.publish')
        if real_mutation:
            from .real_runtime import submit_real
            return submit_real(command_id, sessions=Session)
    policy, secrets = ResourcePolicy(), SecretProvider()
    with command_lock(command_id) as acquired:
        if not acquired:
            return 'busy'
        with Session.begin() as db:
            command = db.get(ManagedCommand, command_id)
            if command is None:
                return 'missing'
            lock_tenant(db, command.tenant_id)
            run = db.get(ExecutionRun, command.run_id)
            row = get_preparation(db, run.tenant_id, run.project_id, run.preparation_id, lock=True)
            attempt = db.get(EngineAttempt, command_id)
            supervision = db.get(RuntimeSupervision, command_id)
            if attempt is None and supervision and supervision.stop_request:
                from .run_control import control_row, effects_for
                control = control_row(db, command_id)
                control.facts = {**control.facts, 'termination': {'confirmed': True, 'basis': 'cancelled_before_submission',
                    'confirmed_at': datetime.now(timezone.utc).isoformat(), 'terminated_at': None}}
                control.wake_at = None
                for effect in effects_for(db, command_id):
                    effect.phase, effect.result = 'confirmed', {'cancelled_before_submission': True}
                run.status = 'cancelled'
                record(db, run, 'control.cancelled_before_submission', {'command_id': command_id})
                return run.status
            if run.status in ('denied', 'metadata_checked', 'blocked_target', 'cancelled', 'cleaned', 'supervision_unavailable'):
                # Terminal without cleanup: the publisher must stop waking this command.
                from .run_control import control_row
                control_row(db, command_id).wake_at = None
                return run.status
            if attempt and attempt.engine_job_id:
                return run.status
            try:
                payload = verify_envelope(command.envelope, os.environ['COMMAND_PUBLIC_KEY'], COMMAND_FORMAT)
                expected = {'command_id': command.id, 'run_id': run.id, 'accepted_at': run.accepted_at.isoformat(),
                            **authorization_bindings(row, run.subject)}
                if any(payload.get(key) != value for key, value in expected.items()) or (
                        command.tenant_id, command.project_id) != (run.tenant_id, run.project_id):
                    raise HTTPException(403, '签名命令与运行、主体、范围或冻结制品绑定不一致')
                if row.environment != 'local' or row.tool not in ('pipeline.publish', 'pipeline.trial', 'metadata.read'):
                    raise HTTPException(403, '执行工具或环境未登记')
                if attempt is None:
                    if payload['authorized_operation']['data_plane_mutation'] and not payload.get('runtime_supervision_contract'):
                        raise HTTPException(409, '冻结监督契约缺失；请重新冻结版本并准备运行')
                    _check_submission_authorization(db, policy, secrets, run, row)
                    if not payload['authorized_operation']['data_plane_mutation']:
                        run.status = 'metadata_checked'
                        record(db, run, 'execution.metadata_checked', {'command_id': command_id, 'data_plane_mutation': False})
                        return run.status
                    if os.environ.get('EXECUTION_ADAPTER') == 'real':
                        from .real_engine import transfer_gate
                        _, conditions = transfer_gate(True, False, False, False)
                        run.status = 'supervision_unavailable'
                        from .run_control import control_row
                        control_row(db, command_id).wake_at = None
                        record(db, run, 'execution.real_gate_denied', {'command_id': command_id,
                            'conditions': conditions, 'data_plane_mutation': False})
                        return run.status
                    target = policy.get(payload['resources']['target']['resource_id'])
                    identity = {'physical_cluster_id': target['physical_cluster_id'],
                                'database': target['database'], 'table': row.frozen['plan']['target']['table']}
                    target_key = hashlib.sha256(signing_bytes(identity)).hexdigest()
                    now = datetime.now(timezone.utc)
                    db.execute(insert(TargetOwnership).values(target_key=target_key, identity=identity,
                        command_id=command_id, acquired_at=now).on_conflict_do_nothing(index_elements=['target_key']))
                    owner = db.get(TargetOwnership, target_key)
                    if owner.command_id != command_id:
                        run.status = 'blocked_target'
                        record(db, run, 'execution.target_blocked', {'command_id': command_id, 'target_key': target_key})
                        return run.status
                    request = {'command_id': command_id, 'run_id': run.id, 'correlation_id': new_id(),
                        'tenant_id': run.tenant_id, 'project_id': run.project_id, 'tool': row.tool,
                        'environment': row.environment, 'adapter': os.environ.get('EXECUTION_ADAPTER', 'disabled'),
                        'engine_url': engine_url(),
                        'target': identity, 'unpublished_object': 'l1-unpublished:' + command_id,
                        'budget': payload['budget'], 'authorized_operation': payload['authorized_operation'],
                        'runtime_supervision_contract': payload['runtime_supervision_contract'],
                        'bundle_sha256': payload['bundle_sha256'], 'artifact_hashes': payload['artifact_hashes']}
                    digest = hashlib.sha256(signing_bytes(request)).hexdigest()
                    attempt = EngineAttempt(command_id=command_id, correlation_id=request['correlation_id'],
                        request_sha256=digest, request=request, phase='submitting', created_at=now, updated_at=now)
                    db.add(attempt)
                    run.status = 'submitting'
                    record(db, run, 'execution.submission_prepared', {'command_id': command_id,
                        'correlation_id': attempt.correlation_id, 'request_sha256': digest, 'target_key': target_key})
                    submit = True
                else:
                    if attempt.request['engine_url'] != engine_url():
                        raise HTTPException(409, '原始引擎配置变化；保持未知与隔离，不向其他引擎发现')
                    run.status = attempt.phase = 'submission_unknown'
                    attempt.reason = '提交进程中断或响应未知，仅发现原关联作业'
                    attempt.updated_at = datetime.now(timezone.utc)
                    record(db, run, 'execution.submission_unknown', {'command_id': command_id,
                        'correlation_id': attempt.correlation_id, 'reason': attempt.reason})
                    submit = False
            except (HTTPException, ValueError) as error:
                reason = str(error.detail) if isinstance(error, HTTPException) else str(error)
                if attempt:
                    run.status = attempt.phase = 'submission_unknown'
                    attempt.reason = reason
                else:
                    run.status = 'denied'
                record(db, run, 'execution.denied', {'command_id': command_id, 'reason': reason})
                return run.status
            request, digest = attempt.request, attempt.request_sha256
        # All correlation, ownership and evidence facts are durable before I/O.
        # Recheck under the shared tenant lock and hold it through bounded submit:
        # a completed account/member revocation cannot slip before the call.
        with Session.begin() as db:
            lock_tenant(db, command.tenant_id)
            run, attempt = db.get(ExecutionRun, command.run_id), db.get(EngineAttempt, command_id)
            if submit:
                policy = ResourcePolicy()
                try:
                    _check_submission_authorization(db, policy, secrets, run)
                except HTTPException as error:
                    run.status = attempt.phase = 'denied'
                    attempt.reason = error.detail
                    record(db, run, 'execution.denied', {'command_id': command_id, 'reason': error.detail})
                    return run.status
            try:
                result = engine_request('POST', '/jobs', {'request': request, 'request_sha256': digest}) if submit else engine_request(
                    'GET', '/jobs?correlation_id=' + request['correlation_id'])
                jobs = [result] if submit else result['jobs']
                job = jobs[0] if len(jobs) == 1 and jobs[0]['request_sha256'] == digest else None
                reason = None if job else '未能唯一发现原始作业；禁止重新提交、发布和清理'
            except (OSError, ValueError) as error:
                job, reason = None, '引擎响应未知：' + type(error).__name__
            run.status = attempt.phase = 'submitted' if job else 'submission_unknown'
            run.engine_job_id = attempt.engine_job_id = job['id'] if job else None
            attempt.reason, attempt.updated_at = reason, datetime.now(timezone.utc)
            record(db, run, 'execution.submitted' if job else 'execution.submission_unknown',
                   {'command_id': command_id, 'correlation_id': attempt.correlation_id,
                    'engine_job_id': attempt.engine_job_id, 'reason': reason, 'ownership_retained': True})
            return run.status
