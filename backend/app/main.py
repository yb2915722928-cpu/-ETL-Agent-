import os
import platform
import secrets
import shutil
from contextlib import asynccontextmanager
from enum import Enum
from typing import Annotated

import fastapi
import psutil
import sqlalchemy
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, SecretStr
from redis.exceptions import RedisError
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session as DbSession

from .db import Base, Session, engine
from .evidence import lock_tenant
from .models import Account, IdentityEvent, Membership, Project, Tenant
from .security import COOKIE, SESSION_TTL, append_event, hash_password, session_key, sessions, verify_password


@asynccontextmanager
async def lifespan(app):
    from .evidence_signing import EvidenceSigner
    from .migrations import initialize_database
    from .managed_secrets import SecretProvider
    from .connector_policy import ResourcePolicy
    app.state.evidence_signer = EvidenceSigner()
    app.state.secret_provider = SecretProvider()
    app.state.connector_policy = ResourcePolicy()
    initialize_database()
    yield


app = FastAPI(title='ETL-Agent 身份与项目协作', version='0.1.0', lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def invalid_input(request, exc):
    # Pydantic's default error includes the original input, including passwords.
    return JSONResponse(status_code=422, content={'detail': '输入不符合要求，请检查字段和密码长度'})


@app.exception_handler(IntegrityError)
async def conflict(request, exc):
    return JSONResponse(status_code=409, content={'detail': '名称已存在或关联不合法'})


@app.exception_handler(SQLAlchemyError)
async def database_unavailable(request, exc):
    return JSONResponse(status_code=503, content={'detail': '数据库暂不可用'})


@app.exception_handler(RedisError)
async def sessions_unavailable(request, exc):
    return JSONResponse(status_code=503, content={'detail': '会话服务暂不可用'})


@app.get('/api/health')
def health(response: Response):
    report = {'status': 'ok', 'application': app.version,
              'runtime': {'python': platform.python_version(), 'fastapi': fastapi.__version__,
                          'sqlalchemy': sqlalchemy.__version__},
              'resources': {'cpu_count': os.cpu_count(),
                            'memory_total_bytes': psutil.virtual_memory().total,
                            'memory_available_bytes': psutil.virtual_memory().available,
                            'disk_free_bytes': shutil.disk_usage('.').free,
                            'process_rss_bytes': psutil.Process().memory_info().rss}}
    try:
        with engine.connect() as db:
            report['postgresql'] = {'query': db.scalar(text('SELECT 1')),
                                    'version': db.scalar(text('SHOW server_version')),
                                    'database_bytes': db.scalar(text('SELECT pg_database_size(current_database())'))}
    except SQLAlchemyError:
        report['postgresql'] = {'status': 'unavailable'}
        report['status'] = 'degraded'
    try:
        report['redis'] = {'ping': sessions.ping(), 'version': sessions.info('server')['redis_version'],
                           'used_memory_bytes': sessions.info('memory')['used_memory']}
    except RedisError:
        report['redis'] = {'status': 'unavailable'}
        report['status'] = 'degraded'
    if report['status'] != 'ok':
        response.status_code = 503
    return report


class Role(str, Enum):
    project_admin = 'project_admin'
    maker = 'maker'
    data_checker = 'data_checker'
    security_checker = 'security_checker'
    operator = 'operator'
    auditor = 'auditor'


Name = Annotated[str, Field(min_length=1, max_length=100)]
Slug = Annotated[str, Field(min_length=1, max_length=64, pattern=r'^[a-zA-Z0-9_-]+$')]
Password = Annotated[SecretStr, Field(min_length=12, max_length=128)]


class LoginInput(BaseModel):
    tenant: Slug
    username: Slug
    password: Password


class AccountInput(BaseModel):
    username: Slug
    display_name: Name
    password: Password
    tenant_admin: bool = False


class AccountPatch(BaseModel):
    enabled: bool | None = None
    tenant_admin: bool | None = None
    password: Password | None = None


class ProjectInput(BaseModel):
    slug: Slug
    name: Name


class MemberInput(BaseModel):
    roles: Annotated[list[Role], Field(min_length=1, max_length=6)]


def database():
    with Session.begin() as db:
        yield db


Db = Annotated[DbSession, Depends(database)]


def current_actor(request: Request, db: Db):
    token = request.cookies.get(COOKIE)
    account_id = sessions.get(session_key(token)) if token else None
    actor = db.get(Account, account_id) if account_id else None
    if actor is None or not actor.enabled:
        raise HTTPException(401, '请登录，账号可能已禁用或会话已到期')
    return actor


Actor = Annotated[Account, Depends(current_actor)]


def tenant_scope(actor, tenant_id):
    if actor.tenant_id != tenant_id:
        raise HTTPException(403, '无权访问此租户')


def tenant_admin(actor, tenant_id):
    tenant_scope(actor, tenant_id)
    if not actor.tenant_admin:
        raise HTTPException(403, '仅租户管理员可管理账号和创建项目')


def project_scope(db, actor, tenant_id, project_id, manage=False):
    tenant_scope(actor, tenant_id)
    membership = db.scalar(select(Membership).where(
        Membership.project_id == project_id, Membership.account_id == actor.id,
        Membership.tenant_id == tenant_id).execution_options(populate_existing=True))
    if membership is None or not membership.roles:
        raise HTTPException(403, '不属于此项目或项目角色已撤销')
    if manage and Role.project_admin.value not in membership.roles:
        raise HTTPException(403, '仅当前项目管理员可管理成员')
    return membership


def account_view(account):
    return {'id': account.id, 'username': account.username, 'display_name': account.display_name,
            'enabled': account.enabled, 'tenant_admin': account.tenant_admin}


def lock_identity_mutation(db, actor):
    # Same entry lock as Prepare/Approve/Commit: revocation cannot pass a
    # concurrent authorization that still sees the old account or membership.
    lock_tenant(db, actor.tenant_id)
    db.refresh(actor)
    if not actor.enabled:
        raise HTTPException(401, '账号已禁用')


def owned_account(db, tenant_id, account_id):
    target = db.scalar(select(Account).where(Account.id == account_id, Account.tenant_id == tenant_id))
    if target is None:
        raise HTTPException(404, '此租户内不存在该账号')
    return target


@app.post('/api/login')
def login(body: LoginInput, response: Response, db: Db):
    tenant = db.scalar(select(Tenant).where(Tenant.slug == body.tenant))
    actor = db.scalar(select(Account).where(Account.tenant_id == tenant.id,
                      Account.username == body.username)) if tenant else None
    if actor is None or not actor.enabled or not verify_password(body.password.get_secret_value(), actor.password_hash):
        raise HTTPException(401, '租户、账号或密码不正确，或账号已禁用')
    token = secrets.token_urlsafe(32)
    append_event(db, actor, 'session.login', {})
    sessions.setex(session_key(token), SESSION_TTL, actor.id)
    response.set_cookie(COOKIE, token, max_age=SESSION_TTL, httponly=True, samesite='strict',
                        secure=os.environ.get('COOKIE_SECURE', 'false').lower() == 'true', path='/')
    return account_view(actor)


@app.post('/api/logout')
def logout(request: Request, response: Response, db: Db, actor: Actor):
    sessions.delete(session_key(request.cookies[COOKIE]))
    append_event(db, actor, 'session.logout', {})
    response.delete_cookie(COOKIE, path='/')
    return {'message': '已退出登录'}


@app.get('/api/me')
def me(db: Db, actor: Actor):
    tenant = db.get(Tenant, actor.tenant_id)
    return {'account': account_view(actor), 'tenant': {'id': tenant.id, 'slug': tenant.slug, 'name': tenant.name}}


@app.get('/api/tenants/{tenant_id}/projects')
def projects(tenant_id: str, db: Db, actor: Actor):
    tenant_scope(actor, tenant_id)
    rows = db.execute(select(Project, Membership).join(Membership, Membership.project_id == Project.id).where(
        Project.tenant_id == tenant_id, Membership.account_id == actor.id)).all()
    return [{'id': project.id, 'slug': project.slug, 'name': project.name, 'roles': member.roles,
             'can_manage_members': 'project_admin' in member.roles} for project, member in rows if member.roles]


@app.post('/api/tenants/{tenant_id}/projects', status_code=201)
def create_project(tenant_id: str, body: ProjectInput, db: Db, actor: Actor):
    tenant_admin(actor, tenant_id)
    project = Project(tenant_id=tenant_id, slug=body.slug, name=body.name)
    db.add(project)
    db.flush()
    db.add(Membership(tenant_id=tenant_id, project_id=project.id, account_id=actor.id, roles=['project_admin']))
    append_event(db, actor, 'project.created', {'name': project.name, 'roles': ['project_admin']}, project.id)
    return {'id': project.id, 'slug': project.slug, 'name': project.name}


@app.get('/api/tenants/{tenant_id}/accounts')
def accounts(tenant_id: str, db: Db, actor: Actor):
    tenant_admin(actor, tenant_id)
    return [account_view(a) for a in db.scalars(select(Account).where(Account.tenant_id == tenant_id).order_by(Account.username))]


@app.post('/api/tenants/{tenant_id}/accounts', status_code=201)
def create_account(tenant_id: str, body: AccountInput, db: Db, actor: Actor):
    tenant_admin(actor, tenant_id)
    account = Account(tenant_id=tenant_id, username=body.username, display_name=body.display_name,
                      password_hash=hash_password(body.password.get_secret_value()), tenant_admin=body.tenant_admin)
    db.add(account)
    db.flush()
    append_event(db, actor, 'account.created', {'target_account_id': account.id, 'tenant_admin': account.tenant_admin})
    return account_view(account)


@app.patch('/api/tenants/{tenant_id}/accounts/{account_id}')
def update_account(tenant_id: str, account_id: str, body: AccountPatch, db: Db, actor: Actor):
    lock_identity_mutation(db, actor)
    tenant_admin(actor, tenant_id)
    account = owned_account(db, tenant_id, account_id)
    changes = {'target_account_id': account.id}
    for name in ('enabled', 'tenant_admin'):
        value = getattr(body, name)
        if value is not None:
            setattr(account, name, value)
            changes[name] = value
    if body.password is not None:
        account.password_hash = hash_password(body.password.get_secret_value())
        changes['password_changed'] = True
    append_event(db, actor, 'account.updated', changes)
    return account_view(account)


@app.get('/api/tenants/{tenant_id}/projects/{project_id}/members')
def members(tenant_id: str, project_id: str, db: Db, actor: Actor):
    project_scope(db, actor, tenant_id, project_id)
    rows = db.execute(select(Account, Membership).join(Membership, Membership.account_id == Account.id).where(
        Membership.tenant_id == tenant_id, Membership.project_id == project_id).order_by(Account.username)).all()
    return [{**account_view(account), 'roles': membership.roles, 'revision': membership.revision}
            for account, membership in rows]


@app.get('/api/tenants/{tenant_id}/projects/{project_id}/eligible-accounts')
def eligible_accounts(tenant_id: str, project_id: str, db: Db, actor: Actor):
    project_scope(db, actor, tenant_id, project_id, manage=True)
    return [{'id': a.id, 'username': a.username, 'display_name': a.display_name, 'enabled': a.enabled}
            for a in db.scalars(select(Account).where(Account.tenant_id == tenant_id).order_by(Account.username))]


@app.put('/api/tenants/{tenant_id}/projects/{project_id}/members/{account_id}')
def set_member(tenant_id: str, project_id: str, account_id: str, body: MemberInput, db: Db, actor: Actor):
    lock_identity_mutation(db, actor)
    project_scope(db, actor, tenant_id, project_id, manage=True)
    owned_account(db, tenant_id, account_id)
    member = db.get(Membership, (project_id, account_id), populate_existing=True)
    roles = sorted({role.value for role in body.roles})
    before = member.roles[:] if member else []
    if member is None:
        member = Membership(tenant_id=tenant_id, project_id=project_id, account_id=account_id, roles=roles)
        db.add(member)
    else:
        member.roles = roles
        member.revision += 1
    append_event(db, actor, 'membership.assigned', {'target_account_id': account_id,
                 'previous_roles': before, 'roles': roles}, project_id)
    return {'account_id': account_id, 'roles': member.roles, 'revision': member.revision}


@app.delete('/api/tenants/{tenant_id}/projects/{project_id}/members/{account_id}')
def remove_member(tenant_id: str, project_id: str, account_id: str, db: Db, actor: Actor):
    lock_identity_mutation(db, actor)
    project_scope(db, actor, tenant_id, project_id, manage=True)
    member = db.get(Membership, (project_id, account_id), populate_existing=True)
    if member is None or member.tenant_id != tenant_id:
        raise HTTPException(404, '项目成员不存在')
    previous = member.roles[:]
    db.delete(member)
    append_event(db, actor, 'membership.removed', {'target_account_id': account_id, 'previous_roles': previous}, project_id)
    return {'message': '已移除成员'}


@app.get('/api/tenants/{tenant_id}/identity-events')
def identity_events(tenant_id: str, db: Db, actor: Actor):
    tenant_admin(actor, tenant_id)
    return [{'id': e.id, 'sequence': e.sequence, 'occurred_at': e.occurred_at,
             'actor_id': e.actor_id, 'project_id': e.project_id, 'action': e.action, 'facts': e.facts}
            for e in db.scalars(select(IdentityEvent).where(IdentityEvent.tenant_id == tenant_id).order_by(IdentityEvent.sequence))]


from .evidence_api import register_evidence_routes
register_evidence_routes(app, Db, Actor, tenant_admin, project_scope)

from .connectors import register_connector_routes
register_connector_routes(app, Db, Actor, project_scope)

from .studio import register_studio_routes
register_studio_routes(app, Db, Actor, project_scope)

from .versions import register_version_routes
register_version_routes(app, Db, Actor, project_scope)

from .preparations import register_preparation_routes
register_preparation_routes(app, Db, Actor, project_scope)

from .execution import register_execution_routes
from . import execution_runtime  # Register additive execution tables before startup migration.
register_execution_routes(app, Db, Actor, project_scope)
