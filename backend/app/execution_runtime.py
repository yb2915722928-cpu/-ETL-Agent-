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
from .supervision_runtime import supervise_locked, supervision_view


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
    events = list(db.scalars(select(EvidenceEvent).where(EvidenceEvent.run_id == command.run_id,
        EvidenceEvent.scope == command.project_id).order_by(EvidenceEvent.sequence.desc()).limit(20)))
    return {'level': 'L1' if os.environ.get('EXECUTION_ADAPTER', 'disabled') == 'l1' else 'disabled',
            'attempt': {key: getattr(attempt, key) for key in ('correlation_id', 'request_sha256', 'request',
                'phase', 'reason', 'engine_job_id', 'created_at', 'updated_at')} if attempt else None,
            'ownership': {'target_key': ownership.target_key, 'identity': ownership.identity,
                          'acquired_at': ownership.acquired_at, 'retained': True} if ownership else None,
            'control_facts': [{'action': event.action, 'occurred_at': event.occurred_at,
                               'facts': event.facts} for event in reversed(events)],
            'supervision': supervision_view(db, command_id),
            'publication': '未实现：L1 不执行数据搬运、发布或清理'}


def engine_request(method, path, body=None):
    request = urllib.request.Request(os.environ['L1_ENGINE_URL'] + path,
        data=signing_bytes(body) if body is not None else None, method=method,
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def _check_submission_authorization(db, policy, secrets, run, row=None):
    require_current_role(db, run.tenant_id, run.project_id, run.subject, 'operator')
    row = row or get_preparation(db, run.tenant_id, run.project_id, run.preparation_id, lock=True)
    validation = validate_preparation(db, policy, row, check_expiry=False)
    if not validation['valid']:
        raise HTTPException(409, '；'.join(validation['reasons']))
    check_execution_resources(db, policy, secrets,
        get_version(db, run.tenant_id, run.project_id, row.version_id),
        db.get(Account, run.subject), 'operator', preparation_id=row.id)


def drive(command_id):
    state = _submit(command_id)
    if state not in ('submitted', 'stop_requested'):
        return state
    with command_lock(command_id) as acquired:
        return supervise_locked(command_id) if acquired else 'busy'


def _submit(command_id):
    if os.environ.get('EXECUTION_ADAPTER', 'disabled') != 'l1':
        raise RuntimeError('执行适配器未启用；普通部署不得提交引擎')
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
            if run.status in ('submitted', 'denied', 'metadata_checked', 'blocked_target', 'stop_requested',
                              'supervision_passed', 'supervision_unavailable'):
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
                        'environment': row.environment, 'adapter': 'l1', 'engine_url': os.environ['L1_ENGINE_URL'],
                        'target': identity, 'budget': payload['budget'], 'authorized_operation': payload['authorized_operation'],
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
                    if attempt.request['engine_url'] != os.environ['L1_ENGINE_URL']:
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
