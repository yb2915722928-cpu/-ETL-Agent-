"""P1 credential rotation. Callers own tenant-first transaction and project scope.

Only this managed exit materializes a new reference and swaps the selected slot.
Candidate bytes stay encrypted outside Preparation, capabilities and evidence.
"""
from datetime import datetime, timedelta, timezone
from typing import Literal

from cryptography.exceptions import InvalidTag
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy import ForeignKeyConstraint, Integer, LargeBinary, String, select, update
from sqlalchemy.orm import Mapped, mapped_column

from .connector_policy import fingerprint
from .connector_probe import ProbeFailure, inspect_resource
from .db import Base
from .evidence import append_event, lock_tenant
from .models import Connection, SecretReference, new_id
from .preparations import (POLICY_VERSION, SLOTS, Preparation, current_role,
                           decisions_for, preparation_view, require_current_role)
from .studio_plan import VERSIONS, canonical_json
from .versions import sha256
from .writer_credentials import inspect_execution_writer, writer_resource

TOOL = 'credential.rotate'
ROTATION_POLICY = 'credential-rotation-v1'
SLOT_COLUMNS = {'probe_readonly': ('secret_reference_id', 'secret_version'),
                'execution_writer': ('writer_secret_reference_id', 'writer_secret_version')}


class CredentialRotationCandidate(Base):
    __tablename__ = 'credential_rotation_candidates'
    __table_args__ = (ForeignKeyConstraint(['preparation_id', 'tenant_id', 'project_id'],
        ['preparations.id', 'preparations.tenant_id', 'preparations.project_id']),)
    preparation_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    secret_reference_id: Mapped[str] = mapped_column(String(36), unique=True)
    version: Mapped[int] = mapped_column(Integer)
    nonce: Mapped[bytes] = mapped_column(LargeBinary)
    ciphertext: Mapped[bytes] = mapped_column(LargeBinary)


class RotationInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    slot: Literal['probe_readonly', 'execution_writer']
    credential: SecretStr = Field(min_length=1, max_length=512)


def candidate_reference(candidate):
    return SecretReference(id=candidate.secret_reference_id, tenant_id=candidate.tenant_id,
        project_id=candidate.project_id, version=candidate.version,
        nonce=candidate.nonce, ciphertext=candidate.ciphertext)


def candidate_digest(candidate):
    return fingerprint({'reference_id': candidate.secret_reference_id, 'version': candidate.version,
        'tenant_id': candidate.tenant_id, 'project_id': candidate.project_id,
        'nonce': candidate.nonce.hex(), 'ciphertext': candidate.ciphertext.hex()})


def current_rotation_binding(db, policy, tenant_id, project_id, connection_id, slot):
    if slot not in SLOT_COLUMNS:
        raise HTTPException(409, '凭据职责槽未登记')
    connection = db.scalar(select(Connection).where(Connection.id == connection_id,
        Connection.tenant_id == tenant_id, Connection.project_id == project_id)
        .execution_options(populate_existing=True))
    if connection is None:
        raise HTTPException(404, '当前项目不存在该连接')
    resource = policy.get(connection.resource_id)
    if (not resource or connection.kind == 'csv' or fingerprint(resource) != connection.resource_fingerprint
            or resource.get('kind') != connection.kind or resource.get('direction') != connection.direction):
        raise HTTPException(409, '连接资源身份或部署白名单已漂移')
    selected_resource = writer_resource(resource) if slot == 'execution_writer' else resource
    id_column, version_column = SLOT_COLUMNS[slot]
    reference_id, version = getattr(connection, id_column), getattr(connection, version_column)
    reference = db.get(SecretReference, reference_id, populate_existing=True) if reference_id else None
    if (reference is None or reference.revoked_at is not None or
            (reference.tenant_id, reference.project_id, reference.version) != (tenant_id, project_id, version)):
        raise HTTPException(409, '所选凭据槽引用或版本无效')
    return connection, selected_resource, {'connection_id': connection.id, 'resource_id': connection.resource_id,
        'resource_fingerprint': connection.resource_fingerprint, 'slot': slot,
        'slot_resource_fingerprint': fingerprint(selected_resource), 'username': selected_resource['username'],
        'secret_reference_id': reference_id, 'secret_version': version}


def validate_rotation_preparation(db, policy, row, require_approved=True, check_expiry=True, now=None):
    now = now or datetime.now(timezone.utc)
    reasons, frozen = [], row.frozen
    eligible = {'operator': current_role(db, row.tenant_id, row.project_id, row.maker_id, 'operator')}
    if not eligible['operator']:
        reasons.append('申请人当前账号或 Operator 资格失效')
    if check_expiry and now >= row.expires_at:
        reasons.append('准备单已超过24小时有效期，请重新准备与审批')
    if sha256(canonical_json(frozen)) != row.frozen_sha256:
        reasons.append('准备单冻结指纹不一致')
    if (row.tool != TOOL or row.risk_level != 'P1' or row.version_id is not None
            or frozen.get('rotation_policy') != ROTATION_POLICY or frozen.get('policy_version') != POLICY_VERSION
            or frozen.get('compiler_versions') != VERSIONS):
        reasons.append('轮换工具、风险、版本或适用策略不一致')
    if any(frozen.get(key) != getattr(row, key) for key in
           ('tenant_id', 'project_id', 'maker_id', 'version_id', 'tool', 'environment', 'risk_level')):
        reasons.append('准备单绑定主体、工具或环境不一致')
    if (frozen.get('created_at') != row.created_at.isoformat()
            or frozen.get('expires_at') != row.expires_at.isoformat()):
        reasons.append('准备单时间绑定不一致')
    candidate = db.get(CredentialRotationCandidate, row.id, populate_existing=True)
    rotation = frozen.get('rotation', {})
    if (candidate is None or candidate_digest(candidate) != rotation.get('candidate_sha256')
            or candidate.secret_reference_id != rotation.get('new_secret_reference_id')
            or candidate.version != rotation.get('new_secret_version')
            or (candidate.tenant_id, candidate.project_id) != (row.tenant_id, row.project_id)):
        reasons.append('轮换候选密文或引用绑定不一致')
    try:
        _, _, binding = current_rotation_binding(db, policy, row.tenant_id, row.project_id,
            rotation.get('connection_id'), rotation.get('slot'))
        if binding != rotation.get('previous_binding'):
            reasons.append('所选凭据版本、身份或资源已漂移，请重新准备与审批')
        if (rotation.get('new_secret_version') != binding['secret_version'] + 1
                or rotation.get('new_secret_reference_id') == binding['secret_reference_id']):
            reasons.append('新凭据必须使用全新引用及递增版本')
    except HTTPException as error:
        reasons.append(error.detail)
    decisions = decisions_for(db, row)
    for decision in decisions:
        eligible[decision.slot] = decision.slot in SLOTS and current_role(
            db, row.tenant_id, row.project_id, decision.checker_id, SLOTS[decision.slot])
        if not eligible[decision.slot]:
            reasons.append('审批人的当前账号或具名职责已失效')
        if decision.checker_id == row.maker_id or decision.frozen_sha256 != row.frozen_sha256:
            reasons.append('审批独立性或冻结指纹不一致')
    if len({d.checker_id for d in decisions}) != len(decisions):
        reasons.append('同一账号不可承担两个审批槽')
    if reasons:
        state = 'expired' if check_expiry and now >= row.expires_at else 'invalidated'
    elif any(d.decision == 'reject' for d in decisions):
        state = 'rejected'
        reasons.append('已有具名拒绝决定，须重新准备')
    elif {d.slot for d in decisions if d.decision == 'approve'} == set(SLOTS):
        state = 'approved'
    else:
        state = 'pending'
        if require_approved:
            reasons.append('尚缺独立数据与安全双审批')
    return {'valid': not reasons, 'reasons': reasons, 'state': state, 'current_eligibility': eligible}


def check_rotation_resources(db, policy, secret_provider, row, actor, role):
    """Managed candidate read/grant check; never writes external data or exposes samples."""
    lock_tenant(db, row.tenant_id)
    if actor.tenant_id != row.tenant_id:
        raise HTTPException(403, '无权检查其他租户的凭据')
    require_current_role(db, row.tenant_id, row.project_id, actor.id, role)
    validation = validate_rotation_preparation(db, policy, row, require_approved=False)
    if not validation['valid']:
        raise HTTPException(409, '；'.join(validation['reasons']))
    rotation = row.frozen['rotation']
    _, resource, binding = current_rotation_binding(db, policy, row.tenant_id, row.project_id,
        rotation['connection_id'], rotation['slot'])
    candidate = db.get(CredentialRotationCandidate, row.id)
    try:
        credential = secret_provider.open(candidate_reference(candidate))
        if rotation['slot'] == 'execution_writer':
            inspect_execution_writer(resource, credential)
        else:
            inspect_resource(resource, credential)
    except (InvalidTag, ProbeFailure):
        raise HTTPException(409, '候选凭据受管验证失败') from None
    append_event(db, actor, 'credential_rotation.resources_checked', {'tool': 'credential.authorization.read',
        'risk_level': 'P3', 'slot': rotation['slot'], 'binding': binding,
        'candidate_sha256': rotation['candidate_sha256']}, row.project_id, preparation_id=row.id)
    return binding


def create_rotation_preparation(db, policy, secret_provider, actor, tenant_id, project_id, connection_id, body):
    lock_tenant(db, tenant_id)
    if actor.tenant_id != tenant_id:
        raise HTTPException(403, '无权轮换其他租户的凭据')
    require_current_role(db, tenant_id, project_id, actor.id, 'operator')
    _, _, binding = current_rotation_binding(db, policy, tenant_id, project_id, connection_id, body.slot)
    created = datetime.now(timezone.utc)
    expires = created + timedelta(hours=24)
    row_id = new_id()
    candidate = CredentialRotationCandidate(preparation_id=row_id, tenant_id=tenant_id, project_id=project_id,
        secret_reference_id=new_id(), version=binding['secret_version'] + 1)
    candidate.nonce, candidate.ciphertext = secret_provider.seal(candidate_reference(candidate), body.credential.get_secret_value())
    rotation = {'connection_id': connection_id, 'slot': body.slot, 'previous_binding': binding,
        'new_secret_reference_id': candidate.secret_reference_id, 'new_secret_version': candidate.version,
        'candidate_sha256': candidate_digest(candidate)}
    # Common command envelope shape keeps the existing signed capability/commit exit reusable.
    frozen = {'tenant_id': tenant_id, 'project_id': project_id, 'maker_id': actor.id, 'version_id': None,
        'tool': TOOL, 'environment': 'local', 'risk_level': 'P1', 'rotation_policy': ROTATION_POLICY,
        'risk_reason': '登记凭据轮换须独立数据与安全双审批', 'rotation': rotation,
        'policy_version': POLICY_VERSION, 'compiler_versions': VERSIONS, 'approval_slots': list(SLOTS),
        'execution_sha256': fingerprint(rotation), 'bundle_sha256': fingerprint({}), 'artifact_hashes': {},
        'execution_facts': {'source': {}, 'target': binding},
        'authorized_operation': {'tool': TOOL, 'data_plane_mutation': False, 'tools': [TOOL]},
        'budget': {'max_connections': 1, 'max_slots': 1}, 'runtime_supervision_contract': None,
        'impact': {'connection_id': connection_id, 'slot': body.slot, 'old_bindings_invalidated': True},
        'recovery': '保留旧版本供已绑定运行核对；不可偷偷改用新凭据，撤销旧版本须安全停机',
        'created_at': created.isoformat(), 'expires_at': expires.isoformat()}
    row = Preparation(id=row_id, tenant_id=tenant_id, project_id=project_id, maker_id=actor.id,
        version_id=None, tool=TOOL, environment='local', risk_level='P1', frozen=frozen,
        frozen_sha256=sha256(canonical_json(frozen)), created_at=created, expires_at=expires)
    db.add(row)
    db.flush()
    db.add(candidate)
    db.flush()
    check_rotation_resources(db, policy, secret_provider, row, actor, 'operator')
    append_event(db, actor, 'credential_rotation.prepared', {'connection_id': connection_id, 'slot': body.slot,
        'frozen_sha256': row.frozen_sha256, 'new_secret_version': candidate.version}, project_id, preparation_id=row.id)
    return row


def commit_rotation(db, policy, secret_provider, signer, row, actor, payload, idempotency_key, request_sha256):
    """After signed capability checks, atomically swap and ledger acceptance.

    Never commits/catches database errors. Caller MUST roll back on any failure.
    This inline command must not be submitted to the engine/outbox publisher.
    """
    from .command_signing import COMMAND_FORMAT, sign_envelope
    from .execution import (ExecutionRun, IdempotencyRecord, ManagedCommand, Outbox,
                            ReplayGuard, authorization_bindings)
    lock_tenant(db, row.tenant_id)
    require_current_role(db, row.tenant_id, row.project_id, actor.id, 'operator')
    validation = validate_rotation_preparation(db, policy, row)
    if not validation['valid']:
        raise HTTPException(409, '；'.join(validation['reasons']))
    check_rotation_resources(db, policy, secret_provider, row, actor, 'operator')
    now = datetime.now(timezone.utc)
    if now >= row.expires_at or now.timestamp() >= payload['expires_at']:
        raise HTTPException(409, '受管检查结束时授权或准备单已过期，请重新授权')
    rotation = row.frozen['rotation']
    candidate = db.get(CredentialRotationCandidate, row.id)
    secret = candidate_reference(candidate)
    db.add(secret)
    db.flush()
    id_column, version_column = SLOT_COLUMNS[rotation['slot']]
    previous = rotation['previous_binding']
    changed = db.execute(update(Connection).where(Connection.id == rotation['connection_id'],
        Connection.tenant_id == row.tenant_id, Connection.project_id == row.project_id,
        Connection.resource_fingerprint == previous['resource_fingerprint'],
        getattr(Connection, id_column) == previous['secret_reference_id'],
        getattr(Connection, version_column) == previous['secret_version'])
        .values(**{id_column: secret.id, version_column: secret.version}).execution_options(synchronize_session=False))
    if changed.rowcount != 1:
        raise HTTPException(409, '所选凭据槽已变化，轮换未应用')
    old_reference = db.get(SecretReference, previous['secret_reference_id'], populate_existing=True)
    old_reference.revoked_at = now
    run_id, command_id = new_id(), new_id()
    run = ExecutionRun(id=run_id, tenant_id=row.tenant_id, project_id=row.project_id,
        preparation_id=row.id, subject=actor.id, status='succeeded', accepted_at=now)
    db.add(run)
    db.flush()
    db.add(ManagedCommand(id=command_id, tenant_id=row.tenant_id, project_id=row.project_id, run_id=run_id,
        envelope=sign_envelope(signer, {'format': COMMAND_FORMAT, 'command_id': command_id,
            'run_id': run_id, 'accepted_at': now.isoformat(), **authorization_bindings(row, actor.id)}), created_at=now))
    db.flush()
    db.add_all([ReplayGuard(jti=payload['jti'], tenant_id=row.tenant_id, project_id=row.project_id,
        run_id=run_id, consumed_at=now), IdempotencyRecord(tenant_id=row.tenant_id, project_id=row.project_id,
        key=idempotency_key, request_sha256=request_sha256, run_id=run_id),
        Outbox(id=new_id(), tenant_id=row.tenant_id, project_id=row.project_id, command_id=command_id,
               status='applied_inline', created_at=now)])
    db.flush()
    append_event(db, actor, 'credential_rotation.completed', {'command_id': command_id,
        'connection_id': rotation['connection_id'], 'slot': rotation['slot'],
        'previous_secret_reference_id': previous['secret_reference_id'], 'previous_secret_version': previous['secret_version'],
        'secret_reference_id': secret.id, 'secret_version': secret.version,
        'frozen_sha256': row.frozen_sha256, 'status': 'applied_inline'}, row.project_id,
        preparation_id=row.id, run_id=run_id)
    return run


def register_credential_rotation_routes(app, Db, Actor, project_scope):
    prefix = '/api/tenants/{tenant_id}/projects/{project_id}'

    @app.post(prefix + '/connections/{connection_id}/credential-rotations', status_code=201)
    def prepare_rotation(tenant_id: str, project_id: str, connection_id: str, body: RotationInput,
                         request: Request, db: Db, actor: Actor):
        lock_tenant(db, actor.tenant_id)
        try:
            project_scope(db, actor, tenant_id, project_id)
            # Savepoint prevents rejected candidate/probe attempts becoming preparations.
            with db.begin_nested():
                row = create_rotation_preparation(db, request.app.state.connector_policy,
                    request.app.state.secret_provider, actor, tenant_id, project_id, connection_id, body)
        except HTTPException as error:
            append_event(db, actor, 'credential_rotation.denied', {'reason': error.detail})
            return JSONResponse(status_code=error.status_code, content={'detail': error.detail})
        return preparation_view(db, request.app.state.connector_policy, row)
