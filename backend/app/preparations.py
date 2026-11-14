"""Frozen authorization facts and independent, named ApprovalDecisions.

Commit/Worker callers authorize their scope, lock_tenant BEFORE get_preparation
(lock=True), call explicit managed check_execution_resources, then pure
validate_preparation in their transaction. No data-plane mutations occur here.
"""
from datetime import datetime, timedelta, timezone
from typing import Literal

from cryptography.exceptions import InvalidTag
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, ForeignKeyConstraint, JSON, String, UniqueConstraint, select
from sqlalchemy.orm import Mapped, mapped_column

from .connector_policy import fingerprint
from .connector_probe import MAX_COLUMNS, MAX_TABLES, QUERY_TIMEOUT_SECONDS, SAMPLE_ROWS, TOTAL_SECONDS, ProbeFailure, inspect_resource
from .db import Base
from .evidence import append_event, lock_tenant
from .models import Account, Membership, MetadataProfile, SecretReference, new_id
from .operation_authorization import operation_authorizer
from .studio_plan import VERSIONS, assess_plan, canonical_json
from .versions import get_version, sha256

POLICY_VERSION = 'preparation-policy-v1'
TRIAL_LIMITS = {'max_rows': 10000, 'max_bytes': 16777216, 'max_seconds': 120}
SLOTS = {'data': 'data_checker', 'security': 'security_checker'}
TOOLS = {'pipeline.publish': ('P1', '正式目标发布意图，须独立数据与安全审批'),
         'pipeline.trial': ('P2', '隔离写入/试运行意图，须独立数据与安全审批'),
         'metadata.read': ('P3', '仅受限只读元数据，使用现有白名单与探查预算')}


class Preparation(Base):
    __tablename__ = 'preparations'
    __table_args__ = (
        UniqueConstraint('id', 'tenant_id', 'project_id'),
        ForeignKeyConstraint(['version_id', 'tenant_id', 'project_id'],
                             ['pipeline_versions.id', 'pipeline_versions.tenant_id', 'pipeline_versions.project_id']),
        ForeignKeyConstraint(['maker_id', 'tenant_id'], ['accounts.id', 'accounts.tenant_id']),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    maker_id: Mapped[str] = mapped_column(String(36))
    version_id: Mapped[str] = mapped_column(String(36))
    tool: Mapped[str] = mapped_column(String(64))
    environment: Mapped[str] = mapped_column(String(32))
    risk_level: Mapped[str] = mapped_column(String(2))
    frozen: Mapped[dict] = mapped_column(JSON)
    frozen_sha256: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ApprovalDecision(Base):
    __tablename__ = 'approval_decisions'
    __table_args__ = (
        UniqueConstraint('preparation_id', 'slot'),
        UniqueConstraint('preparation_id', 'checker_id'),
        ForeignKeyConstraint(['preparation_id', 'tenant_id', 'project_id'],
                             ['preparations.id', 'preparations.tenant_id', 'preparations.project_id']),
        ForeignKeyConstraint(['checker_id', 'tenant_id'], ['accounts.id', 'accounts.tenant_id']),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    preparation_id: Mapped[str] = mapped_column(String(36))
    slot: Mapped[str] = mapped_column(String(16))
    checker_id: Mapped[str] = mapped_column(String(36))
    checker_name: Mapped[str] = mapped_column(String(100))
    decision: Mapped[str] = mapped_column(String(16))
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    frozen_sha256: Mapped[str] = mapped_column(String(64))


class PrepareInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    version_id: str = Field(min_length=1, max_length=36)
    tool: str = Field(default='pipeline.publish', min_length=1, max_length=64)
    environment: Literal['local'] = 'local'


class DecisionInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    slot: Literal['data', 'security']
    decision: Literal['approve', 'reject']


def current_role(db, tenant_id, project_id, account_id, role):
    account = db.scalar(select(Account).where(Account.id == account_id, Account.tenant_id == tenant_id)
                        .execution_options(populate_existing=True))
    member = db.scalar(select(Membership).where(Membership.account_id == account_id,
                       Membership.tenant_id == tenant_id, Membership.project_id == project_id)
                       .execution_options(populate_existing=True))
    return bool(account and account.enabled and member and role in member.roles)


def require_current_role(db, tenant_id, project_id, account_id, role):
    if not current_role(db, tenant_id, project_id, account_id, role):
        raise HTTPException(403, '账号已禁用、项目角色已撤销或不具备所需职责')


def get_preparation(db, tenant_id, project_id, preparation_id, lock=False):
    query = select(Preparation).where(Preparation.id == preparation_id,
                   Preparation.tenant_id == tenant_id, Preparation.project_id == project_id)
    if lock:
        query = query.with_for_update()
    row = db.scalar(query.execution_options(populate_existing=True))
    if row is None:
        raise HTTPException(404, '当前项目不存在该准备单')
    return row


def decisions_for(db, row):
    return list(db.scalars(select(ApprovalDecision).where(ApprovalDecision.preparation_id == row.id)
                           .order_by(ApprovalDecision.slot)))


def current_execution_facts(db, policy, version):
    # Reassess the frozen definition against the latest independently collected
    # profiles, not candidate edits or ordinary samples/statistics/row counts.
    plan = {**version.frozen['normalized_plan']}
    for side in ('source', 'target'):
        ref = plan[side]
        latest = db.scalar(select(MetadataProfile).where(MetadataProfile.tenant_id == version.tenant_id,
                           MetadataProfile.project_id == version.project_id,
                           MetadataProfile.connection_id == ref['connection_id'])
                           .order_by(MetadataProfile.created_at.desc(), MetadataProfile.id.desc()).limit(1))
        if latest is None:
            raise HTTPException(409, '缺少受管探查事实，请重新探查')
        plan[side] = {**ref, 'profile_id': latest.id}
    assessment = assess_plan(db, policy, version.tenant_id, version.project_id, plan)
    if not assessment['passed']:
        raise HTTPException(409, '当前资源、Schema、凭据或计划门禁已变化，请重新探查并准备')
    return assessment['execution_facts']


def check_execution_resources(db, policy, secret_provider, version, actor, role, preparation_id=None):
    """Explicit managed P3 preflight, separate from local frozen validation.

    Called immediately before Prepare/Approve/Commit/Worker submission, inside
    the caller's tenant-first transaction. Records success/denial, never stores
    samples or touches data objects. Callers catch HTTPException into a response
    so denial evidence commits. No GET/list invokes this tool.
    """
    lock_tenant(db, version.tenant_id)
    receipt = {'checked_at': datetime.now(timezone.utc).isoformat(), 'risk_level': 'P3',
               'tool': 'metadata.read', 'policy_version': POLICY_VERSION,
               'validity': '仅此推进步骤有效；下一次推进必须重新执行受管检查',
               'resources': {}, 'budget_per_resource': {'max_tables': MAX_TABLES,
                   'sample_rows_per_table': SAMPLE_ROWS, 'query_timeout_seconds': QUERY_TIMEOUT_SECONDS,
                   'max_seconds': TOTAL_SECONDS}}
    try:
        if actor.tenant_id != version.tenant_id:
            raise HTTPException(403, '无权检查其他租户的资源')
        require_current_role(db, version.tenant_id, version.project_id, actor.id, role)
        facts = current_execution_facts(db, policy, version)
        for side in ('source', 'target'):
            asset = facts[side]
            resource = policy.get(asset['resource_id'])
            secret = db.get(SecretReference, asset['secret_reference_id'], populate_existing=True)
            try:
                # Existing managed exit enforces fixed budgets and read-only
                # grants. Samples/statistics are discarded before returning.
                content = inspect_resource(resource, secret_provider.open(secret))
            except (ProbeFailure, InvalidTag) as error:
                code = error.code if isinstance(error, ProbeFailure) else 'secret_unavailable'
                raise HTTPException(409, f'受管只读检查未通过：{code}') from None
            schema = [{'name': table['name'], 'columns': table['columns']} for table in content['tables']]
            observed = fingerprint(schema)
            receipt['resources'][side] = {**{key: asset[key] for key in ('connection_id', 'resource_id',
                'resource_fingerprint', 'secret_reference_id', 'secret_version')}, 'schema_hash': observed}
            if observed != asset['schema_hash']:
                raise HTTPException(409, '外部Schema已变化，请重新探查、冻结版本并准备与审批')
    except HTTPException as error:
        append_event(db, actor, 'preparation.resources_check_denied', {**receipt, 'reason': error.detail},
                     version.project_id, version_id=version.id, preparation_id=preparation_id)
        raise
    append_event(db, actor, 'preparation.resources_checked', receipt, version.project_id,
                 version_id=version.id, preparation_id=preparation_id)
    return {'facts': facts, 'receipt': receipt}


def validate_preparation(db, policy, row, require_approved=True, check_expiry=True, now=None):
    """Reusable live validation; no mutation. Caller holds tenant then row lock
    when accepting an authorization. Worker uses check_expiry=False after Commit;
    caller MUST additionally bind/allowlist tool, risk, subject and manifest.
    """
    now = now or datetime.now(timezone.utc)
    reasons = []
    eligible = {'maker': current_role(db, row.tenant_id, row.project_id, row.maker_id, 'maker')}
    if not eligible['maker']:
        reasons.append('申请人当前账号或 Maker 资格失效')
    if check_expiry and now >= row.expires_at:
        reasons.append('准备单已超过24小时有效期，请重新准备与审批')
    if sha256(canonical_json(row.frozen)) != row.frozen_sha256:
        reasons.append('准备单冻结指纹不一致')
    frozen = row.frozen
    if (row.tenant_id, row.project_id, row.tool, row.environment, row.risk_level, row.maker_id, row.version_id,
            row.created_at.isoformat(), row.expires_at.isoformat()) != (
            frozen['tenant_id'], frozen['project_id'], frozen['tool'], frozen['environment'], frozen['risk_level'],
            frozen['maker_id'], frozen['version_id'], frozen['created_at'], frozen['expires_at']):
        reasons.append('准备单绑定主体、工具、环境或版本不一致')
    risk = TOOLS.get(row.tool, ('P0', '未登记工具，禁止授权'))[0]
    if row.risk_level != risk or frozen['policy_version'] != POLICY_VERSION or frozen['compiler_versions'] != VERSIONS:
        reasons.append('适用策略或工具风险已变化，请重新准备与审批')
    version = get_version(db, row.tenant_id, row.project_id, row.version_id)
    if (version.execution_sha256 != frozen['execution_sha256'] or version.bundle_sha256 != frozen['bundle_sha256']
            or version.artifact_hashes != frozen['artifact_hashes']
            or sha256(canonical_json(version.frozen['execution_facts'])) != frozen['execution_sha256']
            or any(sha256(content) != version.artifact_hashes[name] for name, content in version.artifacts.items())):
        reasons.append('绑定版本或制品指纹已漂移')
    try:
        current = current_execution_facts(db, policy, version)
        if current != frozen['execution_facts']:
            reasons.append('Schema、资源身份、凭据版本或执行事实已漂移，请重新准备与审批')
    except HTTPException as error:
        reasons.append(error.detail)
    decisions = decisions_for(db, row)
    for decision in decisions:
        eligible[decision.slot] = current_role(db, row.tenant_id, row.project_id, decision.checker_id, SLOTS[decision.slot])
        if not eligible[decision.slot]:
            reasons.append(f'{decision.slot} 审批人的当前账号或职责已失效')
        if decision.checker_id == row.maker_id or decision.frozen_sha256 != row.frozen_sha256:
            reasons.append('审批独立性或冻结指纹不一致')
    if len({d.checker_id for d in decisions}) != len(decisions):
        reasons.append('同一账号不可承担两个审批槽')
    if risk == 'P0':
        reasons.append('P0：未登记工具，禁止授权')
        state = 'denied'
    elif reasons:
        state = 'expired' if check_expiry and now >= row.expires_at else 'invalidated'
    elif any(d.decision == 'reject' for d in decisions):
        state = 'rejected'
        reasons.append('已有具名拒绝决定，须重新准备')
    elif risk == 'P3':
        state = 'auto_authorized'
    elif {d.slot for d in decisions if d.decision == 'approve'} == set(SLOTS):
        state = 'approved'
    else:
        state = 'pending'
        if require_approved:
            reasons.append('尚缺独立数据与安全双审批')
    return {'valid': not reasons, 'reasons': reasons, 'state': state, 'current_eligibility': eligible}


def preparation_view(db, policy, row):
    result = {key: getattr(row, key) for key in ('id', 'tenant_id', 'project_id', 'maker_id', 'version_id',
              'tool', 'environment', 'risk_level', 'frozen', 'frozen_sha256', 'created_at', 'expires_at')}
    result['decisions'] = [{key: getattr(d, key) for key in ('id', 'slot', 'checker_id', 'checker_name', 'decision',
                           'decided_at', 'frozen_sha256')} for d in decisions_for(db, row)]
    result['validation'] = validate_preparation(db, policy, row)
    return result


def register_preparation_routes(app, Db, Actor, project_scope):
    prefix = '/api/tenants/{tenant_id}/projects/{project_id}/preparations'
    authorize = operation_authorizer(project_scope, 'preparation.denied', '仅当前项目 Maker 可生成准备单')

    @app.post(prefix, status_code=201)
    def prepare(tenant_id: str, project_id: str, body: PrepareInput, request: Request, db: Db, actor: Actor):
        lock_tenant(db, actor.tenant_id)
        denied = authorize(db, actor, tenant_id, project_id, True)
        if denied:
            return denied
        try:
            require_current_role(db, tenant_id, project_id, actor.id, 'maker')
            version = get_version(db, tenant_id, project_id, body.version_id)
            checked = check_execution_resources(db, request.app.state.connector_policy,
                                                request.app.state.secret_provider, version, actor, 'maker')
            facts = checked['facts']
            if sha256(canonical_json(facts)) != version.execution_sha256:
                raise HTTPException(409, '版本冻结事实与当前受管检查不同，请重新冻结版本')
        except HTTPException as error:
            append_event(db, actor, 'preparation.create_denied', {'reason': error.detail}, project_id)
            return JSONResponse(status_code=error.status_code, content={'detail': error.detail})
        risk, reason = TOOLS.get(body.tool, ('P0', '未登记工具，禁止授权'))
        created = datetime.now(timezone.utc)
        expires = created + timedelta(hours=24)
        frozen = {'maker_id': actor.id, 'tenant_id': tenant_id, 'project_id': project_id,
                  'version_id': version.id, 'execution_sha256': version.execution_sha256,
                  'bundle_sha256': version.bundle_sha256, 'artifact_hashes': version.artifact_hashes,
                  'execution_facts': facts, 'profile_refs': version.frozen['profile_refs'],
                  'plan': version.frozen['normalized_plan'], 'hocon': version.frozen['hocon'], 'dag': version.frozen['dag'],
                  'tool': body.tool, 'environment': body.environment, 'risk_level': risk, 'risk_reason': reason,
                  'policy_version': POLICY_VERSION, 'compiler_versions': VERSIONS,
                  'approval_slots': list(SLOTS) if risk in ('P1', 'P2') else [],
                  'classification': '受管业务数据；样本脱敏；禁止凭据与原始样本外传',
                  'quality': facts['plan']['quality'],
                  'budget': {'max_tables': MAX_TABLES, 'sample_rows_per_table': SAMPLE_ROWS,
                             'max_columns': MAX_COLUMNS, 'query_timeout_seconds': QUERY_TIMEOUT_SECONDS,
                             'max_seconds': TOTAL_SECONDS} if risk == 'P3' else {
                                 key: min(value, TRIAL_LIMITS[key]) if risk == 'P2' and key in TRIAL_LIMITS else value
                                 for key, value in facts['plan']['budget'].items()},
                  'authorized_operation': {'tool': body.tool, 'data_plane_mutation': risk in ('P1', 'P2'),
                                           'tools': ['metadata.read'] if risk == 'P3' else facts['plan']['tools']},
                  'metrics': {'source_read': '连接器读取记录；源端边界，含后续拒绝',
                              'evaluated': '解析及映射后按稳定业务键去重的质量判定记录',
                              'valid_rejected': '同一判定阶段与记录单元：evaluated = valid + rejected；拒绝按记录去重',
                              'rule_failures': '规则失败次数独立计量，不等于拒绝记录数',
                              'reject_ratio': 'rejected / evaluated；零分母暂无数据',
                              'amplification': '最终输出记录 / source_read；输入为连接器读取边界，输出为sink接收边界',
                              'bytes': 'sink累计接收写入字节；不以估算、网络或源扫描量代替',
                              'retry': '同运行按稳定业务键累计去重；重试不重复计数；缺少可靠指标不得推进'},
                  'impact': {'source': facts['source'], 'target': facts['target'],
                             'mode': '只读，无数据面变更' if risk == 'P3' else '有界全量；隔离构建；正式发布须运行质量门禁'},
                  'recovery': {'prepublication': '确认引擎终止后清理本次未发布资源',
                               'postpublication': '独立新申请并双审批；仅最近保留快照，默认7天；身份不明拒绝恢复'},
                  'managed_check': checked['receipt'],
                  'checks': '查询页核对最新受管Profile与本地冻结事实；每次推进前独立P3受管检查实时核对外部Schema及只读权限，记录证据；样本统计丢弃，无外部变更',
                  'created_at': created.isoformat(), 'expires_at': expires.isoformat()}
        row = Preparation(tenant_id=tenant_id, project_id=project_id, maker_id=actor.id, version_id=version.id,
                          tool=body.tool, environment=body.environment, risk_level=risk, frozen=frozen,
                          frozen_sha256=sha256(canonical_json(frozen)), created_at=created, expires_at=expires)
        db.add(row)
        db.flush()
        append_event(db, actor, 'preparation.created', {'risk_level': risk, 'tool': body.tool,
                     'frozen_sha256': row.frozen_sha256, 'policy_version': POLICY_VERSION}, project_id,
                     version_id=version.id, preparation_id=row.id, artifact_hashes=version.artifact_hashes)
        return preparation_view(db, request.app.state.connector_policy, row)

    @app.get(prefix)
    def preparations(tenant_id: str, project_id: str, request: Request, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        return [preparation_view(db, request.app.state.connector_policy, row) for row in db.scalars(select(Preparation)
                .where(Preparation.tenant_id == tenant_id, Preparation.project_id == project_id)
                .order_by(Preparation.created_at.desc()))]

    @app.get(prefix + '/{preparation_id}')
    def preparation(tenant_id: str, project_id: str, preparation_id: str, request: Request, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        return preparation_view(db, request.app.state.connector_policy, get_preparation(db, tenant_id, project_id, preparation_id))

    @app.post(prefix + '/{preparation_id}/decisions')
    def decide(tenant_id: str, project_id: str, preparation_id: str, body: DecisionInput, request: Request, db: Db, actor: Actor):
        lock_tenant(db, actor.tenant_id)
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        row = get_preparation(db, tenant_id, project_id, preparation_id, lock=True)
        try:
            require_current_role(db, tenant_id, project_id, actor.id, SLOTS[body.slot])
            if actor.id == row.maker_id:
                raise HTTPException(403, '申请人不可自批')
            existing = decisions_for(db, row)
            if any(d.checker_id == actor.id and d.slot != body.slot for d in existing):
                raise HTTPException(403, '同一账号不可承担两个审批槽')
            validation = validate_preparation(db, request.app.state.connector_policy, row, require_approved=False)
            if not validation['valid'] or row.risk_level not in ('P1', 'P2'):
                raise HTTPException(409, '；'.join(validation['reasons']) or '此风险等级不接受人工审批')
            if any(d.slot == body.slot for d in existing):
                raise HTTPException(409, '该职责槽已有不可修改的具名决定')
            check_execution_resources(db, request.app.state.connector_policy, request.app.state.secret_provider,
                                      get_version(db, tenant_id, project_id, row.version_id), actor,
                                      SLOTS[body.slot], preparation_id=row.id)
            decided_at = datetime.now(timezone.utc)
            if decided_at >= row.expires_at:
                raise HTTPException(409, '准备单已超过24小时有效期，请重新准备与审批')
        except HTTPException as error:
            append_event(db, actor, 'approval.denied', {'slot': body.slot, 'reason': error.detail}, project_id,
                         version_id=row.version_id, preparation_id=row.id)
            return JSONResponse(status_code=error.status_code, content={'detail': error.detail})
        db.add(ApprovalDecision(tenant_id=tenant_id, project_id=project_id, preparation_id=row.id,
                               slot=body.slot, checker_id=actor.id, checker_name=actor.display_name,
                               decision=body.decision, decided_at=decided_at, frozen_sha256=row.frozen_sha256))
        db.flush()
        append_event(db, actor, 'approval.decided', {'slot': body.slot, 'decision': body.decision,
                     'frozen_sha256': row.frozen_sha256}, project_id, version_id=row.version_id, preparation_id=row.id)
        return preparation_view(db, request.app.state.connector_policy, row)

    @app.api_route(prefix + '/{preparation_id}', methods=['PUT', 'PATCH', 'DELETE'])
    def immutable(tenant_id: str, project_id: str, preparation_id: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        row = get_preparation(db, tenant_id, project_id, preparation_id)
        append_event(db, actor, 'preparation.mutation_denied', {'reason': 'immutable'}, project_id, preparation_id=row.id)
        return JSONResponse(status_code=409, content={'detail': '准备单与审批不可修改或续期；请重新准备'})
