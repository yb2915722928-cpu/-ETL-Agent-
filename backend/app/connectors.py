"""Project-scoped persisted connections and immutable metadata profiles.

Downstream callers reuse get_connection/get_profile with an already-authorized
project scope. Only managed execution code receives the SecretProvider.
"""
import hashlib

from cryptography.exceptions import InvalidTag
from fastapi import HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy import select

from .connector_policy import fingerprint, resource_view
from .connector_probe import POLICY_VERSION, ProbeFailure, inspect_resource
from .csv_probe import MAX_CSV_BYTES, SourceParseFailure
from .csv_storage import inspect_stored_csv, save_csv
from .operation_authorization import operation_authorizer
from .evidence import append_event
from .models import Connection, MetadataProfile, SecretReference, new_id


class ConnectionInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    name: str = Field(min_length=1, max_length=100)
    resource_id: str = Field(min_length=1, max_length=64, pattern=r'^[a-zA-Z_][a-zA-Z0-9_-]*$')
    password: SecretStr = Field(min_length=1, max_length=512)


class WriterCredentialInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    credential: SecretStr = Field(alias='pass' + 'word', min_length=1, max_length=512)


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
        'secret_reference_id', 'secret_version', 'writer_secret_reference_id', 'writer_secret_version',
        'storage_id', 'source_sha256', 'source_bytes', 'status', 'created_at')}


def profile_view(profile):
    return {key: getattr(profile, key) for key in (
        'id', 'tenant_id', 'project_id', 'connection_id', 'secret_reference_id', 'secret_version',
        'resource_id', 'resource_fingerprint', 'schema_hash', 'created_at', 'content')}


def register_connector_routes(app, Db, Actor, project_scope):
    prefix = '/api/tenants/{tenant_id}/projects/{project_id}'

    authorize = operation_authorizer(project_scope, 'connector.denied', '仅当前项目Maker可登记连接和进行P3探查')

    @app.get(prefix + '/connector-resources')
    def resources(tenant_id: str, project_id: str, request: Request, db: Db, actor: Actor):
        denied = authorize(db, actor, tenant_id, project_id)
        if denied:
            return denied
        return [resource_view(key, resource) for key, resource in request.app.state.connector_policy.resources().items()
                if resource['kind'] != 'csv' or project_id in resource['projects']]

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
        if resource['kind'] == 'csv':
            append_event(db, actor, 'connection.rejected', {'reason': 'csv_requires_managed_upload'}, project_id)
            return JSONResponse(status_code=403, content={'detail': 'CSV须通过项目受管上传入口登记'})
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

    @app.post(prefix + '/connections/{connection_id}/credentials/execution_writer', status_code=201)
    def bind_writer(tenant_id: str, project_id: str, connection_id: str, body: WriterCredentialInput,
                    request: Request, db: Db, actor: Actor):
        from .evidence import lock_tenant
        from .writer_credentials import writer_resource
        lock_tenant(db, actor.tenant_id)
        denied = authorize(db, actor, tenant_id, project_id, write=True)
        if denied:
            return denied
        connection = get_connection(db, tenant_id, project_id, connection_id)
        resource = request.app.state.connector_policy.get(connection.resource_id)
        try:
            if (connection.kind != 'doris' or connection.direction != 'target' or resource is None
                    or fingerprint(resource) != connection.resource_fingerprint):
                raise HTTPException(409, 'writer只能绑定当前部署白名单内的Doris目标')
            writer_resource(resource)
            if connection.writer_secret_reference_id is not None:
                raise HTTPException(409, 'execution_writer已绑定；凭据轮转须使用受管轮转流程')
        except HTTPException as error:
            append_event(db, actor, 'connection.writer_rejected', {'connection_id': connection.id,
                         'reason': error.detail}, project_id)
            return JSONResponse(status_code=error.status_code, content={'detail': error.detail})
        secret = SecretReference(id=new_id(), tenant_id=tenant_id, project_id=project_id, version=1)
        secret.nonce, secret.ciphertext = request.app.state.secret_provider.seal(secret, body.credential.get_secret_value())
        db.add(secret)
        db.flush()
        connection.writer_secret_reference_id, connection.writer_secret_version = secret.id, secret.version
        db.flush()
        append_event(db, actor, 'connection.writer_bound', {'connection_id': connection.id,
                     'slot': 'execution_writer', 'secret_reference_id': secret.id, 'secret_version': secret.version}, project_id)
        return connection_view(connection)

    @app.post(prefix + '/csv-uploads', status_code=201)
    async def upload_csv(tenant_id: str, project_id: str, request: Request, db: Db, actor: Actor,
                         resource_id: str = Query(min_length=1, max_length=64),
                         name: str = Query(min_length=1, max_length=100)):
        denied = authorize(db, actor, tenant_id, project_id, write=True)
        if denied:
            return denied
        resource = request.app.state.connector_policy.get(resource_id)
        if resource is None or resource['kind'] != 'csv' or project_id not in resource['projects']:
            append_event(db, actor, 'connection.rejected', {'reason': 'csv_resource_not_allowlisted'}, project_id)
            return JSONResponse(status_code=403, content={'detail': '该项目未获受管CSV目录授权'})
        if request.headers.get('content-type', '').split(';')[0] not in ('text/csv', 'application/octet-stream'):
            return JSONResponse(status_code=415, content={'detail': '请以原始CSV字节上传，格式为UTF-8'})
        data = bytearray()
        async for chunk in request.stream():
            if len(data) + len(chunk) > MAX_CSV_BYTES:
                append_event(db, actor, 'connection.rejected', {'reason': 'csv_size_exceeded'}, project_id)
                return JSONResponse(status_code=413, content={'detail': 'CSV文件不得超过2 MiB'})
            data.extend(chunk)
        connection = Connection(id=new_id(), tenant_id=tenant_id, project_id=project_id, name=name,
                                resource_id=resource_id, resource_fingerprint=fingerprint(resource),
                                kind='csv', direction='source', storage_id=new_id(),
                                source_sha256=hashlib.sha256(data).hexdigest(), source_bytes=len(data))
        try:
            save_csv(resource, connection, data)
        except (OSError, ProbeFailure):
            append_event(db, actor, 'connection.rejected', {'reason': 'csv_storage_unavailable'}, project_id)
            return JSONResponse(status_code=502, content={'detail': '受管CSV存储不可用', 'code': 'external_unavailable'})
        db.add(connection)
        db.flush()
        append_event(db, actor, 'connection.registered', {'connection_id': connection.id,
                     'resource_id': resource_id, 'source_ref': 'csv:' + connection.storage_id,
                     'source_sha256': connection.source_sha256, 'source_bytes': connection.source_bytes}, project_id)
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
        if (resource is None or fingerprint(resource) != connection.resource_fingerprint or
                (resource['kind'] == 'csv' and project_id not in resource['projects'])):
            append_event(db, actor, 'connection.probe_rejected', {**facts, 'reason': 'resource_policy_changed'}, project_id)
            return JSONResponse(status_code=409, content={'detail': '资源白名单已变更，请登记新连接后探查'})
        try:
            if connection.kind == 'csv':
                content = inspect_stored_csv(resource, connection)
            else:
                secret = db.get(SecretReference, connection.secret_reference_id)
                plaintext = request.app.state.secret_provider.open(secret)
                content = inspect_resource(resource, plaintext)
        except SourceParseFailure as error:
            connection.status = 'failed'
            append_event(db, actor, 'connection.probe_failed', {**facts, **error.detail()}, project_id)
            return JSONResponse(status_code=422, content=error.detail())
        except (ProbeFailure, InvalidTag) as error:
            code = error.code if isinstance(error, ProbeFailure) else 'secret_unavailable'
            connection.status = 'failed'
            append_event(db, actor, 'connection.probe_failed', {**facts, 'reason': code}, project_id)
            return JSONResponse(status_code=422 if code == 'readonly_required' else 502,
                                content={'detail': str(error) if isinstance(error, ProbeFailure) else '凭据解密失败，请核对部署主密钥',
                                         'code': code, 'source_ref': 'csv:' + connection.storage_id if connection.kind == 'csv' else connection.resource_id})
        schema = [{'name': table['name'], 'columns': table['columns']} for table in content['tables']]
        profile = MetadataProfile(tenant_id=tenant_id, project_id=project_id, connection_id=connection.id,
                                  secret_reference_id=connection.secret_reference_id, secret_version=connection.secret_version,
                                  resource_id=connection.resource_id, resource_fingerprint=connection.resource_fingerprint,
                                  schema_hash=fingerprint(schema), content=content)
        db.add(profile)
        db.flush()
        connection.status = 'ready'
        append_event(db, actor, 'connection.probed', {**facts, 'profile_id': profile.id,
                     'schema_hash': profile.schema_hash, 'tables_count': len(content['tables']),
                     'queries_used': content['budget']['queries_used']}, project_id)
        return profile_view(profile)
