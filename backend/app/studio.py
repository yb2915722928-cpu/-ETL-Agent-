"""Project-scoped editable Candidates. No engine, model, freeze or publish route."""
from datetime import datetime

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, ForeignKeyConstraint, Integer, JSON, String, UniqueConstraint, func, select
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base
from .operation_authorization import operation_authorizer
from .evidence import append_event
from .models import new_id
from .studio_plan import assess_plan, canonical_json
import hashlib


class Candidate(Base):
    __tablename__ = 'studio_candidates'
    __table_args__ = (
        UniqueConstraint('id', 'tenant_id', 'project_id'),
        ForeignKeyConstraint(['project_id', 'tenant_id'], ['projects.id', 'projects.tenant_id']),
        ForeignKeyConstraint(['owner_id', 'tenant_id'], ['accounts.id', 'accounts.tenant_id']),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    owner_id: Mapped[str] = mapped_column(String(36))
    name: Mapped[str] = mapped_column(String(100))
    revision: Mapped[int] = mapped_column(Integer, default=1)
    plan: Mapped[dict] = mapped_column(JSON)
    assessment: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class ValidateInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    plan: dict


class CandidateInput(ValidateInput):
    name: str = Field(min_length=1, max_length=100)


class CandidateUpdate(CandidateInput):
    expected_revision: int = Field(ge=1)


def get_candidate(db, tenant_id, project_id, candidate_id, *, lock=False):
    query = select(Candidate).where(Candidate.id == candidate_id, Candidate.tenant_id == tenant_id,
                                     Candidate.project_id == project_id)
    candidate = db.scalar(query.with_for_update() if lock else query)
    if candidate is None:
        raise HTTPException(404, '当前项目不存在该候选')
    return candidate


def candidate_view(candidate):
    return {key: getattr(candidate, key) for key in ('id', 'tenant_id', 'project_id', 'owner_id', 'name',
            'revision', 'plan', 'assessment', 'created_at', 'updated_at')}


def register_studio_routes(app, Db, Actor, project_scope):
    prefix = '/api/tenants/{tenant_id}/projects/{project_id}/studio'

    authorize = operation_authorizer(project_scope, 'studio.denied', '仅当前项目 Maker 可设计与校验候选')

    @app.post(prefix + '/validate')
    def validate(tenant_id: str, project_id: str, body: ValidateInput, request: Request, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id, True)
        if denied:
            return denied
        result = assess_plan(db, request.app.state.connector_policy, tenant_id, project_id, body.plan)
        append_event(db, actor, 'studio.validated', {'passed': result['passed'],
                     'failed_gates': [g['gate'] for g in result['gates'] if not g['passed']]}, project_id)
        return result

    @app.get(prefix + '/candidates')
    def candidates(tenant_id: str, project_id: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        return [candidate_view(row) for row in db.scalars(select(Candidate).where(
            Candidate.tenant_id == tenant_id, Candidate.project_id == project_id).order_by(Candidate.created_at.desc()))]

    @app.post(prefix + '/candidates', status_code=201)
    def create(tenant_id: str, project_id: str, body: CandidateInput, request: Request, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id, True)
        if denied:
            return denied
        assessment = assess_plan(db, request.app.state.connector_policy, tenant_id, project_id, body.plan)
        candidate = Candidate(tenant_id=tenant_id, project_id=project_id, owner_id=actor.id, name=body.name,
                              plan=assessment['normalized_plan'] or body.plan, assessment=assessment)
        db.add(candidate)
        db.flush()
        append_event(db, actor, 'studio.candidate_created', {'candidate_id': candidate.id,
                     'revision': candidate.revision, 'passed': assessment['passed']}, project_id)
        return candidate_view(candidate)

    @app.get(prefix + '/candidates/{candidate_id}')
    def candidate(tenant_id: str, project_id: str, candidate_id: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        return candidate_view(get_candidate(db, tenant_id, project_id, candidate_id))

    @app.put(prefix + '/candidates/{candidate_id}')
    def update(tenant_id: str, project_id: str, candidate_id: str, body: CandidateUpdate, request: Request, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id, True)
        if denied:
            return denied
        candidate = get_candidate(db, tenant_id, project_id, candidate_id, lock=True)
        if candidate.owner_id != actor.id:
            raise HTTPException(403, '仅候选创建者可修改；可另存为自己的候选')
        if candidate.revision != body.expected_revision:
            raise HTTPException(409, '候选修订号已变化，请重新读取后编辑')
        assessment = assess_plan(db, request.app.state.connector_policy, tenant_id, project_id, body.plan)
        candidate.name = body.name
        candidate.plan = assessment['normalized_plan'] or body.plan
        candidate.assessment = assessment
        candidate.revision += 1
        db.flush()
        append_event(db, actor, 'studio.candidate_updated', {'candidate_id': candidate.id,
                     'revision': candidate.revision, 'passed': assessment['passed']}, project_id)
        return candidate_view(candidate)

    @app.get(prefix + '/candidates/{candidate_id}/artifacts/{artifact}')
    def download(tenant_id: str, project_id: str, candidate_id: str, artifact: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        candidate = get_candidate(db, tenant_id, project_id, candidate_id)
        if artifact == 'plan.json':
            content, media_type = canonical_json(candidate.plan), 'application/json'
        elif artifact in ('review.conf', 'dag.json'):
            value = candidate.assessment['hocon' if artifact == 'review.conf' else 'dag']
            if value is None:
                raise HTTPException(409, '候选门禁未通过，无可审查的派生制品')
            content = value if artifact == 'review.conf' else canonical_json(value)
            media_type = 'text/plain' if artifact == 'review.conf' else 'application/json'
        else:
            raise HTTPException(404, '制品不存在')
        return Response(content, media_type=media_type, headers={
            'Content-Disposition': f'attachment; filename="{candidate.id}-{artifact}"',
            'X-Content-SHA256': hashlib.sha256(content.encode()).hexdigest()})
