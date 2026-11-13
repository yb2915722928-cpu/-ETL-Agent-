"""Append-only PipelineVersions and scoped, frozen artifact reads."""
from datetime import datetime
import difflib
import hashlib

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, ForeignKeyConstraint, Integer, JSON, String, UniqueConstraint, func, select
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base
from .evidence import append_event
from .models import new_id
from .studio import get_candidate
from .studio_plan import assess_plan, canonical_json, freeze_facts


def sha256(content):
    return hashlib.sha256(content.encode('utf-8')).hexdigest()


class PipelineVersion(Base):
    __tablename__ = 'pipeline_versions'
    __table_args__ = (
        UniqueConstraint('id', 'tenant_id', 'project_id'),
        UniqueConstraint('candidate_id', 'candidate_revision', 'execution_sha256'),
        ForeignKeyConstraint(['project_id', 'tenant_id'], ['projects.id', 'projects.tenant_id']),
        ForeignKeyConstraint(['owner_id', 'tenant_id'], ['accounts.id', 'accounts.tenant_id']),
        ForeignKeyConstraint(['candidate_id', 'tenant_id', 'project_id'],
                             ['studio_candidates.id', 'studio_candidates.tenant_id', 'studio_candidates.project_id']),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    owner_id: Mapped[str] = mapped_column(String(36))
    candidate_id: Mapped[str] = mapped_column(String(36))
    candidate_revision: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String(100))
    execution_sha256: Mapped[str] = mapped_column(String(64))
    bundle_sha256: Mapped[str] = mapped_column(String(64))
    frozen: Mapped[dict] = mapped_column(JSON)
    artifacts: Mapped[dict] = mapped_column(JSON)
    artifact_hashes: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class FreezeInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    candidate_id: str = Field(min_length=1, max_length=36)
    expected_revision: int = Field(ge=1)


def version_view(row, *, detail=True):
    result = {key: getattr(row, key) for key in ('id', 'tenant_id', 'project_id', 'owner_id', 'candidate_id',
              'candidate_revision', 'name', 'execution_sha256', 'bundle_sha256', 'artifact_hashes', 'created_at')}
    if detail:
        result.update(frozen=row.frozen, artifacts=row.artifacts)
    return result


def get_version(db, tenant_id, project_id, version_id):
    row = db.scalar(select(PipelineVersion).where(PipelineVersion.id == version_id,
                    PipelineVersion.tenant_id == tenant_id, PipelineVersion.project_id == project_id))
    if row is None:
        raise HTTPException(404, '当前项目不存在该冻结版本')
    return row


def register_version_routes(app, Db, Actor, project_scope):
    prefix = '/api/tenants/{tenant_id}/projects/{project_id}/versions'

    def authorize(db, actor, tenant_id, project_id, write=False):
        try:
            member = project_scope(db, actor, tenant_id, project_id)
            if write and 'maker' not in member.roles:
                raise HTTPException(403, '仅当前项目 Maker 可冻结候选')
        except HTTPException as error:
            append_event(db, actor, 'version.denied', {'reason': 'scope_or_role_denied', 'write': write})
            return JSONResponse(status_code=error.status_code, content={'detail': error.detail})

    @app.post(prefix, status_code=201)
    def freeze(tenant_id: str, project_id: str, body: FreezeInput, request: Request, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id, True)
        if denied:
            return denied
        try:
            candidate = get_candidate(db, tenant_id, project_id, body.candidate_id, lock=True)
            if candidate.revision != body.expected_revision:
                raise HTTPException(409, '候选修订号已变化，请重新读取后冻结')
            assessment = assess_plan(db, request.app.state.connector_policy, tenant_id, project_id, candidate.plan)
            frozen = freeze_facts(assessment)
        except HTTPException as error:
            append_event(db, actor, 'version.freeze_denied', {'reason': 'candidate_revision_or_current_gates',
                         'status': error.status_code}, project_id)
            return JSONResponse(status_code=error.status_code, content={'detail': error.detail})
        execution_sha = sha256(canonical_json(frozen['execution_facts']))
        existing = db.scalar(select(PipelineVersion).where(PipelineVersion.candidate_id == candidate.id,
                             PipelineVersion.candidate_revision == candidate.revision,
                             PipelineVersion.execution_sha256 == execution_sha))
        if existing is not None:
            append_event(db, actor, 'version.freeze_reused', {'candidate_id': candidate.id,
                         'candidate_revision': candidate.revision}, project_id, version_id=existing.id,
                         artifact_hashes=existing.artifact_hashes)
            return version_view(existing)
        artifacts = {'plan.json': canonical_json(frozen['normalized_plan']), 'review.conf': frozen['hocon'],
                     'dag.json': canonical_json(frozen['dag']), 'facts.json': canonical_json(frozen['execution_facts'])}
        manifest = {'canonicalization': 'sorted-json-v1', 'versions': frozen['versions'],
                    'execution_sha256': execution_sha, 'artifacts': {key: sha256(value) for key, value in artifacts.items()}}
        artifacts['manifest.json'] = canonical_json(manifest)
        hashes = {key: sha256(value) for key, value in artifacts.items()}
        row = PipelineVersion(tenant_id=tenant_id, project_id=project_id, owner_id=actor.id, candidate_id=candidate.id,
                              candidate_revision=candidate.revision, name=candidate.name, frozen=frozen,
                              execution_sha256=execution_sha, bundle_sha256=hashes['manifest.json'],
                              artifacts=artifacts, artifact_hashes=hashes)
        db.add(row)
        db.flush()
        append_event(db, actor, 'version.frozen', {'candidate_id': candidate.id, 'candidate_revision': candidate.revision,
                     'execution_sha256': execution_sha, 'bundle_sha256': row.bundle_sha256}, project_id,
                     version_id=row.id, artifact_hashes=hashes)
        return version_view(row)

    @app.get(prefix)
    def versions(tenant_id: str, project_id: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        return [version_view(row, detail=False) for row in db.scalars(select(PipelineVersion).where(
                PipelineVersion.tenant_id == tenant_id, PipelineVersion.project_id == project_id)
                .order_by(PipelineVersion.created_at.desc(), PipelineVersion.id))]

    @app.get(prefix + '/{version_id}')
    def version(tenant_id: str, project_id: str, version_id: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        return version_view(get_version(db, tenant_id, project_id, version_id))

    @app.api_route(prefix + '/{version_id}', methods=['PUT', 'PATCH', 'DELETE'])
    def immutable(tenant_id: str, project_id: str, version_id: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        row = get_version(db, tenant_id, project_id, version_id)
        append_event(db, actor, 'version.mutation_denied', {'reason': 'immutable'}, project_id, version_id=row.id)
        return JSONResponse(status_code=409, content={'detail': '冻结版本不可修改或删除；请修订候选并创建新版本'})

    @app.get(prefix + '/{version_id}/artifacts/{artifact}')
    def artifact(tenant_id: str, project_id: str, version_id: str, artifact: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        row = get_version(db, tenant_id, project_id, version_id)
        if artifact not in row.artifacts:
            raise HTTPException(404, '制品不存在')
        append_event(db, actor, 'version.artifact_read', {'artifact': artifact}, project_id,
                     version_id=row.id, artifact_hashes={artifact: row.artifact_hashes[artifact]})
        return Response(row.artifacts[artifact], media_type='text/plain' if artifact == 'review.conf' else 'application/json',
                        headers={'Content-Disposition': f'attachment; filename="{row.id}-{artifact}"',
                                 'X-Content-SHA256': row.artifact_hashes[artifact], 'Cache-Control': 'no-store'})

    @app.get(prefix + '/{version_id}/diff/{other_id}')
    def diff(tenant_id: str, project_id: str, version_id: str, other_id: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        before = get_version(db, tenant_id, project_id, version_id)
        after = get_version(db, tenant_id, project_id, other_id)
        sections = {}
        a, b = before.frozen['execution_facts'], after.frozen['execution_facts']
        for key, left, right in (
            ('mapping', a['plan']['mappings'], b['plan']['mappings']),
            ('schema', {side: a[side]['schema'] for side in ('source', 'target')},
                       {side: b[side]['schema'] for side in ('source', 'target')}),
            ('quality', a['plan']['quality'], b['plan']['quality']),
        ):
            sections[key] = {'changed': left != right, 'before': left, 'after': right}
        left, right = before.artifacts['review.conf'], after.artifacts['review.conf']
        sections['hocon'] = {'changed': left != right, 'unified': ''.join(difflib.unified_diff(
            left.splitlines(keepends=True), right.splitlines(keepends=True), fromfile=before.id, tofile=after.id))}
        append_event(db, actor, 'version.diff_read', {'other_version_id': after.id}, project_id, version_id=before.id)
        return {'before_id': before.id, 'after_id': after.id,
                'execution_changed': before.execution_sha256 != after.execution_sha256, 'sections': sections}
