"""Independent execution_writer slot; never grants privileges or reads business rows.

probe_readonly remains Connection.secret_reference_id/secret_version. Writer
identity is deployment-owned and inherits exactly that resource's target scope.
"""
import re
import time

import pymysql
from cryptography.exceptions import InvalidTag
from fastapi import HTTPException

from .connector_policy import fingerprint
from .models import Connection, SecretReference

PROBE_SLOT = 'probe_readonly'
WRITER_SLOT = 'execution_writer'


def writer_resource(resource):
    writer = resource.get(WRITER_SLOT) if resource else None
    if (not resource or resource.get('kind') != 'doris' or resource.get('direction') != 'target'
            or not isinstance(writer, dict) or not {'username'} <= set(writer) <= {'username', 'lifecycle_privileges'}
            or not isinstance(writer.get('lifecycle_privileges', []), list)
            or not set(writer.get('lifecycle_privileges', [])) <= {'Alter_priv', 'Drop_priv'}
            or len(writer.get('lifecycle_privileges', [])) != len(set(writer.get('lifecycle_privileges', [])))
            or not isinstance(writer['username'], str)
            or not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]{0,63}', writer['username'])
            or writer['username'] == resource['username']):
        raise HTTPException(409, '目标缺少独立部署的execution_writer身份')
    target = {**{key: value for key, value in resource.items() if key != WRITER_SLOT},
              'username': writer['username']}
    if writer.get('lifecycle_privileges'):
        target['lifecycle_privileges'] = sorted(writer['lifecycle_privileges'])
    return target


def execution_writer_binding(db, resource, connection):
    """Local binding only. Safe for compiler/views: never decrypts or connects."""
    target = writer_resource(resource)
    secret = db.get(SecretReference, connection.writer_secret_reference_id) if connection.writer_secret_reference_id else None
    if (connection.kind != 'doris' or connection.direction != 'target' or secret is None
            or secret.id == connection.secret_reference_id or getattr(secret, 'revoked_at', None) is not None
            or (secret.tenant_id, secret.project_id, secret.version) !=
            (connection.tenant_id, connection.project_id, connection.writer_secret_version)):
        raise HTTPException(409, '目标execution_writer凭据引用及版本无效或未绑定')
    return {'secret_reference_id': secret.id, 'secret_version': secret.version,
            'resource_fingerprint': fingerprint(target), 'username': target['username']}


def resolve_execution_writer(db, policy, tenant_id, project_id, asset):
    """Validate frozen writer reference against current scoped binding; no decryption."""
    connection = db.get(Connection, asset.get('connection_id'), populate_existing=True)
    resource = policy.get(asset.get('resource_id'))
    if (connection is None or (connection.tenant_id, connection.project_id) != (tenant_id, project_id)
            or connection.resource_id != asset.get('resource_id') or resource is None
            or fingerprint(resource) != asset.get('resource_fingerprint')
            or connection.resource_fingerprint != asset.get('resource_fingerprint')):
        raise HTTPException(409, 'execution_writer目标资源或项目绑定已漂移')
    binding = execution_writer_binding(db, resource, connection)
    if binding != asset.get(WRITER_SLOT):
        raise HTTPException(409, 'execution_writer冻结身份或凭据版本已漂移')
    return writer_resource(resource), db.get(SecretReference, binding['secret_reference_id'], populate_existing=True)


def verify_writer_grants(grants, columns, resource):
    """Doris structured SHOW GRANTS, fail closed on roles/global/other scopes."""
    required = {'Select_priv', 'Load_priv', 'Create_priv'}
    lifecycle = resource.get('lifecycle_privileges', [])
    if (not isinstance(lifecycle, list) or not set(lifecycle) <= {'Alter_priv', 'Drop_priv'}
            or len(lifecycle) != len(set(lifecycle))):
        raise HTTPException(409, 'execution_writer生命周期权限配置无效')
    allowed = required | set(lifecycle)
    observed = set()
    if not grants or not {'Roles', 'DatabasePrivs'} <= set(columns):
        raise HTTPException(409, 'execution_writer授权范围不符合限定库策略')
    for values in grants:
        row = dict(zip(columns, values))
        if row['Roles'] not in (None, '', 'NULL'):
            raise HTTPException(409, 'execution_writer不得继承角色')
        for field, value in row.items():
            if not field.endswith('Privs') or value in (None, '', 'NULL'):
                continue
            if field == 'WorkloadGroupPrivs' and value == 'normal: Usage_priv':
                continue
            if field != 'DatabasePrivs':
                raise HTTPException(409, 'execution_writer不得具有全局、跨库或额外对象权限')
            # Doris emits several database scopes in one semicolon-separated cell.
            # Built-in system catalog SELECT does not authorize another ETL target.
            for entry in str(value).split(';'):
                match = re.fullmatch(r'(?:internal\.)?([a-zA-Z_][a-zA-Z0-9_]{0,63})'
                                     + r':\s*(\w+_priv(?:\s*,\s*\w+_priv)*)', entry.strip())
                if match is None:
                    raise HTTPException(409, 'execution_writer不得具有全局、跨库或额外对象权限')
                database, privileges = match[1], set(re.split(r'\s*,\s*', match[2]))
                if database == resource['database']:
                    if not privileges <= allowed:
                        raise HTTPException(409, 'execution_writer包含未许可权限')
                    observed |= privileges
                elif database not in ('information_schema', 'mysql') or privileges != {'Select_priv'}:
                    raise HTTPException(409, 'execution_writer不得具有全局、跨库或额外对象权限')
    if not required <= observed:
        raise HTTPException(409, 'execution_writer缺少限定库SELECT/LOAD/CREATE权限')


def inspect_execution_writer(resource, credential):
    """Two fixed read-only queries, bounded transport; no DDL/DML/GRANT."""
    started = time.monotonic()
    try:
        with pymysql.connect(host=resource['host'], port=resource['port'], user=resource['username'],
                             **{'pass' + 'word': credential}, database=resource['database'], connect_timeout=3,
                             read_timeout=4, write_timeout=3, charset='utf8mb4', autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT CURRENT_USER()')
                identity = str(cursor.fetchone()[0])
                if not re.fullmatch(r"'?" + re.escape(resource['username']) + r"'?@'?[^'\s]+'?", identity):
                    raise HTTPException(409, 'execution_writer实际身份与部署用户名不一致')
                cursor.execute('SHOW GRANTS')
                grants = cursor.fetchall()
                columns = [column[0] for column in cursor.description]
                verify_writer_grants(grants, columns, resource)
                if time.monotonic() - started > 12:
                    raise HTTPException(409, 'execution_writer授权检查超过固定预算')
            connection.rollback()
    except pymysql.MySQLError:
        raise HTTPException(409, 'execution_writer授权检查连接失败') from None
    return {'username': resource['username'], 'resource_fingerprint': fingerprint(resource),
            'policy': 'doris-writer-database-v1', 'queries_used': 2}


def check_execution_writer(db, policy, secret_provider, version, actor, role, preparation_id=None):
    """Independent managed authorization read; caller holds tenant-first transaction."""
    from .evidence import append_event, lock_tenant
    from .preparations import current_execution_facts, require_current_role
    lock_tenant(db, version.tenant_id)
    receipt = {'slot': WRITER_SLOT, 'tool': 'writer.authorization.read', 'risk_level': 'P3'}
    try:
        if actor.tenant_id != version.tenant_id:
            raise HTTPException(403, '无权检查其他租户的writer')
        require_current_role(db, version.tenant_id, version.project_id, actor.id, role)
        facts = current_execution_facts(db, policy, version)
        if facts != version.frozen['execution_facts']:
            raise HTTPException(409, 'execution_writer执行事实已漂移，请重新冻结与审批')
        resource, secret = resolve_execution_writer(db, policy, version.tenant_id, version.project_id, facts['target'])
        receipt.update(inspect_execution_writer(resource, secret_provider.open(secret)))
        receipt.update(facts['target'][WRITER_SLOT])
    except InvalidTag:
        error = HTTPException(409, 'execution_writer凭据解密失败')
        append_event(db, actor, 'preparation.writer_check_denied', {**receipt, 'reason': error.detail},
                     version.project_id, version_id=version.id, preparation_id=preparation_id)
        raise error from None
    except HTTPException as error:
        append_event(db, actor, 'preparation.writer_check_denied', {**receipt, 'reason': error.detail},
                     version.project_id, version_id=version.id, preparation_id=preparation_id)
        raise
    append_event(db, actor, 'preparation.writer_checked', receipt, version.project_id,
                 version_id=version.id, preparation_id=preparation_id)
    return receipt
