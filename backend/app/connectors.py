"""Project-scoped persisted connections and immutable metadata profiles.

Downstream callers reuse get_connection/get_profile with an already-authorized
project scope. Only managed execution code receives the SecretProvider.
"""
from cryptography.exceptions import InvalidTag
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy import select

from .connector_policy import fingerprint, resource_view
from .connector_probe import POLICY_VERSION, ProbeFailure, inspect_resource
from .evidence import append_event
from .models import Connection, MetadataProfile, SecretReference, new_id


class ConnectionInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    name: str = Field(min_length=1, max_length=100)
    resource_id: str = Field(min_length=1, max_length=64, pattern=r'^[a-zA-Z_][a-zA-Z0-9_-]*$')
    password: SecretStr = Field(min_length=1, max_length=512)


class ProbeInput(BaseModel):
    model_config = ConfigDict(extra='forbid')


def get_connection(db, tenant_id, project_id, connection_id):
    connection = db.scalar(select(Connection).where(Connection.id == connection_id,
                           Connection.tenant_id == tenant_id, Connection.project_id == project_id))
    if connection is None:
        raise HTTPException(404, '当前项目不存在该连接')
    return connection


def get_profile(db, tenant_id, project_id, profile_id):
    profile = db.scalar(select(MetadataProfile).where(MetadataProfile.id == profile_id,
                        MetadataProfile.tenant_id == tenant_id, MetadataProfile.project_id == project_id))
    if profile is None:
        raise HTTPException(404, '当前项目不存在该元数据档案')
    return profile


def connection_view(connection):
    return {key: getattr(connection, key) for key in (
        'id', 'tenant_id', 'project_id', 'name', 'kind', 'direction', 'resource_id', 'resource_fingerprint',
        'secret_reference_id', 'secret_version', 'status', 'created_at')}


def profile_view(profile):
    return {key: getattr(profile, key) for key in (
        'id', 'tenant_id', 'project_id', 'connection_id', 'secret_reference_id', 'secret_version',
        'resource_id', 'resource_fingerprint', 'schema_hash', 'created_at', 'content')}


def register_connector_routes(app, Db, Actor, project_scope):
    prefix = '/api/tenants/{tenant_id}/projects/{project_id}'

    def authorize(db, actor, tenant_id, project_id, write=False):
        try:
            member = project_scope(db, actor, tenant_id, project_id)
            if write and 'maker' not in member.roles:
                raise HTTPException(403, '仅当前项目Maker可登记连接和进行P3探查')
        except HTTPException as error:
            append_event(db, actor, 'connector.denied', {'reason': 'scope_or_role_denied', 'write': write})
            return JSONResponse(status_code=error.status_code, content={'detail': error.detail})

    @app.get(prefix + '/connector-resources')
    def resources(tenant_id: str, project_id: str, request: Request, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        return [resource_view(key, resource) for key, resource in request.app.state.connector_policy.resources().items()]

    @app.get(prefix + '/connections')
    def connections(tenant_id: str, project_id: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        return [connection_view(row) for row in db.scalars(select(Connection).where(
            Connection.tenant_id == tenant_id, Connection.project_id == project_id).order_by(Connection.created_at))]

    @app.post(prefix + '/connections', status_code=201)
    def create_connection(tenant_id: str, project_id: str, body: ConnectionInput, request: Request, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id, write=True)
        if denied:
            return denied
        resource = request.app.state.connector_policy.get(body.resource_id)
        if resource is None:
            append_event(db, actor, 'connection.rejected', {'reason': 'resource_not_allowlisted'}, project_id)
            return JSONResponse(status_code=403, content={'detail': '该资源未在部署白名单中授权'})
        secret = SecretReference(id=new_id(), tenant_id=tenant_id, project_id=project_id, version=1)
        secret.nonce, secret.ciphertext = request.app.state.secret_provider.seal(secret, body.password.get_secret_value())
        db.add(secret)
        db.flush()
        connection = Connection(tenant_id=tenant_id, project_id=project_id, name=body.name,
                                resource_id=body.resource_id, resource_fingerprint=fingerprint(resource),
                                kind=resource['kind'], direction=resource['direction'],
                                secret_reference_id=secret.id, secret_version=secret.version)
        db.add(connection)
        db.flush()
        append_event(db, actor, 'connection.registered', {
            'connection_id': connection.id, 'resource_id': connection.resource_id,
            'secret_reference_id': secret.id, 'secret_version': secret.version}, project_id)
        return connection_view(connection)

    @app.get(prefix + '/connections/{connection_id}')
    def connection(tenant_id: str, project_id: str, connection_id: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        return connection_view(get_connection(db, tenant_id, project_id, connection_id))

    @app.get(prefix + '/connections/{connection_id}/profiles')
    def profiles(tenant_id: str, project_id: str, connection_id: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        get_connection(db, tenant_id, project_id, connection_id)
        return [profile_view(row) for row in db.scalars(select(MetadataProfile).where(
            MetadataProfile.tenant_id == tenant_id, MetadataProfile.project_id == project_id,
            MetadataProfile.connection_id == connection_id).order_by(MetadataProfile.created_at.desc()))]

    @app.get(prefix + '/profiles/{profile_id}')
    def profile(tenant_id: str, project_id: str, profile_id: str, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        return profile_view(get_profile(db, tenant_id, project_id, profile_id))

    @app.post(prefix + '/connections/{connection_id}/probe', status_code=201)
    def probe(tenant_id: str, project_id: str, connection_id: str, body: ProbeInput, request: Request, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id, write=True)
        if denied:
            return denied
        connection = get_connection(db, tenant_id, project_id, connection_id)
        resource = request.app.state.connector_policy.get(connection.resource_id)
        facts = {'connection_id': connection.id, 'resource_id': connection.resource_id,
                 'secret_reference_id': connection.secret_reference_id, 'secret_version': connection.secret_version,
                 'risk_level': 'P3', 'policy_version': POLICY_VERSION}
        if resource is None or fingerprint(resource) != connection.resource_fingerprint:
            append_event(db, actor, 'connection.probe_rejected', {**facts, 'reason': 'resource_policy_changed'}, project_id)
            return JSONResponse(status_code=409, content={'detail': '资源白名单已变更，请登记新连接后探查'})
        secret = db.get(SecretReference, connection.secret_reference_id)
        try:
            plaintext = request.app.state.secret_provider.open(secret)
            content = inspect_resource(resource, plaintext)
        except (ProbeFailure, InvalidTag) as error:
            code = error.code if isinstance(error, ProbeFailure) else 'secret_unavailable'
            connection.status = 'failed'
            append_event(db, actor, 'connection.probe_failed', {**facts, 'reason': code}, project_id)
            return JSONResponse(status_code=422 if code == 'readonly_required' else 502,
                                content={'detail': str(error) if isinstance(error, ProbeFailure) else '凭据解密失败，请核对部署主密钥', 'code': code})
        schema = [{'name': table['name'], 'columns': table['columns']} for table in content['tables']]
        profile = MetadataProfile(tenant_id=tenant_id, project_id=project_id, connection_id=connection.id,
                                  secret_reference_id=secret.id, secret_version=secret.version,
                                  resource_id=connection.resource_id, resource_fingerprint=connection.resource_fingerprint,
                                  schema_hash=fingerprint(schema), content=content)
        db.add(profile)
        db.flush()
        connection.status = 'ready'
        append_event(db, actor, 'connection.probed', {**facts, 'profile_id': profile.id,
                     'schema_hash': profile.schema_hash, 'tables_count': len(content['tables']),
                     'queries_used': content['budget']['queries_used']}, project_id)
        return profile_view(profile)
