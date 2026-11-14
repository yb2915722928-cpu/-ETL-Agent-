"""Atomic Commit acceptance. Acceptance is a queued intent, never engine submission."""
from datetime import datetime, timezone

from fastapi import HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, ForeignKeyConstraint, Integer, JSON, String, UniqueConstraint, select
from sqlalchemy.orm import Mapped, mapped_column

from .command_signing import CAPABILITY_FORMAT, COMMAND_FORMAT, sign_envelope, signing_bytes, verify_envelope
from .db import Base
from .evidence import append_event, lock_tenant
from .models import new_id
from .preparations import check_execution_resources, get_preparation, require_current_role, validate_preparation
from .versions import get_version
import hashlib


class ExecutionRun(Base):
    __tablename__ = 'execution_runs'
    __table_args__ = (
        UniqueConstraint('id', 'tenant_id', 'project_id'),
        UniqueConstraint('preparation_id'),
        ForeignKeyConstraint(['preparation_id', 'tenant_id', 'project_id'],
                             ['preparations.id', 'preparations.tenant_id', 'preparations.project_id']),
        ForeignKeyConstraint(['subject', 'tenant_id'], ['accounts.id', 'accounts.tenant_id']),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    preparation_id: Mapped[str] = mapped_column(String(36))
    subject: Mapped[str] = mapped_column(String(36))
    status: Mapped[str] = mapped_column(String(32), default='queued')
    engine_job_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    accepted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ManagedCommand(Base):
    __tablename__ = 'managed_commands'
    __table_args__ = (
        UniqueConstraint('id', 'tenant_id', 'project_id'),
        UniqueConstraint('run_id'),
        ForeignKeyConstraint(['run_id', 'tenant_id', 'project_id'],
                             ['execution_runs.id', 'execution_runs.tenant_id', 'execution_runs.project_id']),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    run_id: Mapped[str] = mapped_column(String(36))
    envelope: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class Outbox(Base):
    __tablename__ = 'execution_outbox'
    __table_args__ = (UniqueConstraint('command_id'),
        ForeignKeyConstraint(['command_id', 'tenant_id', 'project_id'],
                             ['managed_commands.id', 'managed_commands.tenant_id', 'managed_commands.project_id']),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    command_id: Mapped[str] = mapped_column(String(36))
    status: Mapped[str] = mapped_column(String(32), default='pending')
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ReplayGuard(Base):
    __tablename__ = 'capability_consumptions'
    __table_args__ = (UniqueConstraint('run_id'),
        ForeignKeyConstraint(['run_id', 'tenant_id', 'project_id'],
                             ['execution_runs.id', 'execution_runs.tenant_id', 'execution_runs.project_id']),)
    jti: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    run_id: Mapped[str] = mapped_column(String(36))
    consumed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class IdempotencyRecord(Base):
    __tablename__ = 'commit_idempotency'
    __table_args__ = (
        UniqueConstraint('tenant_id', 'project_id', 'key'),
        UniqueConstraint('run_id'),
        ForeignKeyConstraint(['run_id', 'tenant_id', 'project_id'],
                             ['execution_runs.id', 'execution_runs.tenant_id', 'execution_runs.project_id']),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    key: Mapped[str] = mapped_column(String(128))
    request_sha256: Mapped[str] = mapped_column(String(64))
    run_id: Mapped[str] = mapped_column(String(36))


class SignedEnvelope(BaseModel):
    model_config = ConfigDict(extra='forbid')
    payload: dict
    signature: str = Field(min_length=1, max_length=128)


class CommitInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    capability: SignedEnvelope
    idempotency_key: str = Field(min_length=1, max_length=128)


def authorization_bindings(row, subject):
    frozen = row.frozen
    return {'tenant_id': row.tenant_id, 'project_id': row.project_id, 'subject': subject,
            'preparation_id': row.id, 'frozen_sha256': row.frozen_sha256, 'version_id': row.version_id,
            'tool': row.tool, 'environment': row.environment, 'risk_level': row.risk_level,
            'execution_sha256': frozen['execution_sha256'], 'bundle_sha256': frozen['bundle_sha256'],
            'artifact_hashes': frozen['artifact_hashes'], 'resources': {
                side: frozen['execution_facts'][side] for side in ('source', 'target')},
            'authorized_operation': frozen['authorized_operation'], 'budget': frozen['budget'],
            'runtime_supervision_contract': frozen.get('runtime_supervision_contract'),
            'policy_version': frozen['policy_version'], 'compiler_versions': frozen['compiler_versions']}


def get_run(db, tenant_id, project_id, run_id):
    row = db.scalar(select(ExecutionRun).where(ExecutionRun.id == run_id,
                    ExecutionRun.tenant_id == tenant_id, ExecutionRun.project_id == project_id))
    if row is None:
        raise HTTPException(404, '当前项目不存在该运行')
    return row


def run_view(db, row):
    from .execution_runtime import execution_view
    command = db.scalar(select(ManagedCommand).where(ManagedCommand.run_id == row.id))
    outbox = db.scalar(select(Outbox).where(Outbox.command_id == command.id))
    return {'id': row.id, 'tenant_id': row.tenant_id, 'project_id': row.project_id,
            'preparation_id': row.preparation_id, 'subject': row.subject, 'status': row.status,
            'engine_job_id': row.engine_job_id, 'accepted_at': row.accepted_at,
            'managed_command': command.envelope,
            'execution': execution_view(db, command.id),
            'outbox': {key: getattr(outbox, key) for key in ('id', 'command_id', 'status', 'attempts', 'created_at')}}


def register_execution_routes(app, Db, Actor, project_scope):
    scope = '/api/tenants/{tenant_id}/projects/{project_id}'
    preparation_path = scope + '/preparations/{preparation_id}'

    def authorize(db, actor, tenant_id, project_id, operator=False):
        try:
            project_scope(db, actor, tenant_id, project_id)
            if operator:
                require_current_role(db, tenant_id, project_id, actor.id, 'operator')
        except HTTPException as error:
            append_event(db, actor, 'execution.scope_denied', {'reason': error.detail, 'operator': operator})
            return JSONResponse(status_code=error.status_code, content={'detail': error.detail})

    def ready(db, request, row, actor):
        validation = validate_preparation(db, request.app.state.connector_policy, row)
        if not validation['valid']:
            raise HTTPException(409, '；'.join(validation['reasons']))
        check_execution_resources(db, request.app.state.connector_policy, request.app.state.secret_provider,
                                  get_version(db, row.tenant_id, row.project_id, row.version_id),
                                  actor, 'operator', preparation_id=row.id)
        if datetime.now(timezone.utc) >= row.expires_at:
            raise HTTPException(409, '受管检查结束时准备单已过期，请重新准备')

    def denied(db, actor, row, action, error):
        append_event(db, actor, action, {'reason': error.detail}, row.project_id,
                     preparation_id=row.id, version_id=row.version_id)
        return JSONResponse(status_code=error.status_code, content={'detail': error.detail})

    @app.post(preparation_path + '/capabilities', status_code=201)
    def issue(tenant_id: str, project_id: str, preparation_id: str, request: Request, db: Db, actor: Actor):
        lock_tenant(db, actor.tenant_id)
        rejection = authorize(db, actor, tenant_id, project_id, True)
        if rejection:
            return rejection
        row = get_preparation(db, tenant_id, project_id, preparation_id, lock=True)
        try:
            ready(db, request, row, actor)
            if db.scalar(select(ExecutionRun.id).where(ExecutionRun.preparation_id == row.id)):
                raise HTTPException(409, '此准备单已接受运行，不可再次签发')
        except HTTPException as error:
            return denied(db, actor, row, 'capability.denied', error)
        issued = int(datetime.now(timezone.utc).timestamp())
        envelope = sign_envelope(request.app.state.evidence_signer, {
            'format': CAPABILITY_FORMAT, 'jti': new_id(), 'issued_at': issued, 'expires_at': issued + 300,
            **authorization_bindings(row, actor.id)})
        append_event(db, actor, 'capability.issued', {'jti': envelope['payload']['jti'],
                     'expires_at': issued + 300, 'frozen_sha256': row.frozen_sha256}, project_id,
                     version_id=row.version_id, preparation_id=row.id)
        return {'capability': envelope, 'expires_at': issued + 300}

    @app.post(preparation_path + '/commit', status_code=201)
    def commit(tenant_id: str, project_id: str, preparation_id: str, body: CommitInput,
               request: Request, response: Response, db: Db, actor: Actor):
        lock_tenant(db, actor.tenant_id)
        rejection = authorize(db, actor, tenant_id, project_id, True)
        if rejection:
            return rejection
        envelope = body.capability.model_dump()
        digest = hashlib.sha256(signing_bytes({'preparation_id': preparation_id, 'subject': actor.id,
                                               'capability': envelope})).hexdigest()
        previous = db.scalar(select(IdempotencyRecord).where(IdempotencyRecord.tenant_id == tenant_id,
                    IdempotencyRecord.project_id == project_id, IdempotencyRecord.key == body.idempotency_key))
        if previous:
            if previous.request_sha256 != digest:
                append_event(db, actor, 'commit.denied', {'reason': 'idempotency_conflict'}, project_id)
                return JSONResponse(status_code=409, content={'detail': '同一幂等键绑定了不同请求'})
            response.status_code = 200
            return run_view(db, get_run(db, tenant_id, project_id, previous.run_id))
        row = get_preparation(db, tenant_id, project_id, preparation_id, lock=True)
        try:
            try:
                payload = verify_envelope(envelope, request.app.state.evidence_signer.public['public_key'], CAPABILITY_FORMAT)
            except ValueError as error:
                raise HTTPException(403, str(error)) from None
            expected = authorization_bindings(row, actor.id)
            if any(payload.get(key) != value for key, value in expected.items()):
                raise HTTPException(403, 'Capability 主体、工具、环境、资源或制品绑定不匹配')
            now = datetime.now(timezone.utc)
            if not (isinstance(payload.get('issued_at'), int) and isinstance(payload.get('expires_at'), int)
                    and payload['expires_at'] - payload['issued_at'] == 300
                    and payload['issued_at'] <= now.timestamp() < payload['expires_at']):
                raise HTTPException(409, 'Capability 已过期或签发时效不合法')
            if db.get(ReplayGuard, payload['jti']):
                raise HTTPException(409, 'Capability 已消费，不可重放')
            if db.scalar(select(ExecutionRun.id).where(ExecutionRun.preparation_id == row.id)):
                raise HTTPException(409, '此准备单已接受唯一运行，请查询运行中心')
            ready(db, request, row, actor)
            # Managed external I/O may cross either authorization deadline.
            # Accept against the completion instant, never the preflight start.
            now = datetime.now(timezone.utc)
            if now.timestamp() >= payload['expires_at'] or now >= row.expires_at:
                raise HTTPException(409, '受管检查结束时授权或准备单已过期，请重新授权')
        except HTTPException as error:
            return denied(db, actor, row, 'commit.denied', error)
        run_id, command_id = new_id(), new_id()
        run = ExecutionRun(id=run_id, tenant_id=tenant_id, project_id=project_id,
                           preparation_id=row.id, subject=actor.id, accepted_at=now)
        db.add(run)
        db.flush()
        command = ManagedCommand(id=command_id, tenant_id=tenant_id, project_id=project_id, run_id=run_id,
            envelope=sign_envelope(request.app.state.evidence_signer, {'format': COMMAND_FORMAT,
                'command_id': command_id, 'run_id': run_id, 'accepted_at': now.isoformat(), **expected}), created_at=now)
        db.add(command)
        db.flush()
        db.add_all([ReplayGuard(jti=payload['jti'], tenant_id=tenant_id, project_id=project_id, run_id=run_id, consumed_at=now),
            IdempotencyRecord(tenant_id=tenant_id, project_id=project_id, key=body.idempotency_key,
                              request_sha256=digest, run_id=run_id),
            Outbox(id=new_id(), tenant_id=tenant_id, project_id=project_id, command_id=command_id, created_at=now)])
        from .supervision_runtime import RuntimeSupervision
        if expected['runtime_supervision_contract'] is not None:
            db.add(RuntimeSupervision(command_id=command_id, contract=expected['runtime_supervision_contract'], updated_at=now))
        from .run_control import RunControl
        db.add(RunControl(command_id=command_id, facts={}, wake_at=now))
        db.flush()
        append_event(db, actor, 'commit.accepted', {'command_id': command_id, 'jti': payload['jti'],
                     'status': 'queued', 'frozen_sha256': row.frozen_sha256}, project_id,
                     version_id=row.version_id, preparation_id=row.id, run_id=run_id,
                     artifact_hashes=row.frozen['artifact_hashes'])
        return run_view(db, run)

    @app.get(scope + '/runs')
    def runs(tenant_id: str, project_id: str, db: Db, actor: Actor):
        rejection = authorize(db, actor, tenant_id, project_id)
        if rejection:
            return rejection
        return [run_view(db, row) for row in db.scalars(select(ExecutionRun).where(
            ExecutionRun.tenant_id == tenant_id, ExecutionRun.project_id == project_id)
            .order_by(ExecutionRun.accepted_at.desc()))]

    @app.get(scope + '/runs/{run_id}')
    def run(tenant_id: str, project_id: str, run_id: str, db: Db, actor: Actor):
        rejection = authorize(db, actor, tenant_id, project_id)
        if rejection:
            return rejection
        return run_view(db, get_run(db, tenant_id, project_id, run_id))
