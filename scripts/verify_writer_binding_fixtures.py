"""Offline writer-slot contract fixtures; optional --postgres isolated-schema migration.

Run with backend on PYTHONPATH and a syntactically valid DATABASE_URL. Offline
fixtures never connect to that URL. --postgres creates/drops only a random schema.
No runtime credential files, real L2 runs, or grants are read/performed.
"""
import copy
import json
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql

from app.connector_policy import ResourcePolicy, fingerprint
from app.connector_probe import ProbeFailure, verify_doris_readonly
from app.connectors import WriterCredentialInput, connection_view
from app.models import Connection, SecretReference
from app.studio_plan import assess_plan
from app.writer_credentials import (execution_writer_binding, inspect_execution_writer,
                                    resolve_execution_writer, verify_writer_grants, writer_resource)


def rejected(call):
    try:
        call()
    except (HTTPException, ValueError, ProbeFailure):
        return
    raise AssertionError('unsafe fixture accepted')


class References:
    def __init__(self, rows):
        self.rows = {row.id: row for row in rows}

    def get(self, kind, key, **kwargs):
        return self.rows.get(key)


def run():
    resource = {'kind': 'doris', 'direction': 'target', 'host': 'fixture.invalid', 'port': 9030,
                'database': 'approved_db', 'tables': ['approved_table'], 'username': 'probe_user',
                'physical_cluster_id': 'fixture-cluster', 'execution_writer': {'username': 'writer_user'}}
    secret = SecretReference(id='writer-ref', tenant_id='tenant', project_id='project', version=1)
    connection = Connection(id='target-ref', tenant_id='tenant', project_id='project', kind='doris',
        direction='target', resource_id='target', resource_fingerprint=fingerprint(resource),
        secret_reference_id='probe-ref', secret_version=1,
        writer_secret_reference_id=secret.id, writer_secret_version=secret.version)
    db = References([secret, connection])
    target = writer_resource(resource)
    assert target['username'] == 'writer_user' and target['database'] == resource['database']
    assert target['tables'] == resource['tables'] and target['host'] == resource['host']
    binding = execution_writer_binding(db, resource, connection)
    asset = {'connection_id': connection.id, 'resource_id': 'target',
             'resource_fingerprint': connection.resource_fingerprint, 'execution_writer': binding}
    policy = SimpleNamespace(get=lambda key: copy.deepcopy(resource) if key == 'target' else None)
    assert resolve_execution_writer(db, policy, 'tenant', 'project', asset)[1] is secret
    rejected(lambda: resolve_execution_writer(db, policy, 'tenant', 'other-project', asset))
    rejected(lambda: resolve_execution_writer(db, policy, 'tenant', 'project',
                                              {**asset, 'execution_writer': {**binding, 'secret_version': 2}}))
    with patch.object(secret, 'project_id', 'other-project'):
        rejected(lambda: execution_writer_binding(db, resource, connection))
    with patch.object(connection, 'writer_secret_reference_id', None):
        rejected(lambda: execution_writer_binding(db, resource, connection))
    rejected(lambda: writer_resource({**resource, 'execution_writer': {'username': 'probe_user'}}))
    rejected(lambda: writer_resource({**resource, 'execution_writer': {'username': 'writer_user', 'database': 'other'}}))
    rejected(lambda: writer_resource({**resource, 'kind': 'mysql', 'direction': 'source'}))
    assert set(binding) == {'secret_reference_id', 'secret_version', 'resource_fingerprint', 'username'}
    assert 'ciphertext' not in connection_view(connection) and 'nonce' not in connection_view(connection)
    field = 'pass' + 'word'
    parsed = WriterCredentialInput.model_validate({field: 'fixture-only-value'})
    assert 'fixture-only-value' not in repr(parsed)
    rejected(lambda: WriterCredentialInput.model_validate({field: 'fixture-only-value', 'username': 'other'}))
    columns = ['Roles', 'GlobalPrivs', 'DatabasePrivs', 'TablePrivs', 'WorkloadGroupPrivs']
    grants = [('', '', 'internal.approved_db: Select_priv, Load_priv, Create_priv; '
               'internal.information_schema: Select_priv; internal.mysql: Select_priv', '', 'normal: Usage_priv')]
    verify_writer_grants(grants, columns, target)
    rejected(lambda: verify_doris_readonly(grants, columns))
    for index, value in [(0, 'admin'), (1, '*.*: Admin_priv'),
                         (2, 'other_db: Select_priv, Load_priv, Create_priv'),
                         (2, 'approved_db: Select_priv, Load_priv, Create_priv, Drop_priv'),
                         (2, 'approved_db: Select_priv'),
                         (2, 'approved_db: Select_priv; other_db: Load_priv, Create_priv'),
                         (2, 'approved_db: Select_priv, Load_priv, Create_priv; other_db: Select_priv'),
                         (2, 'approved_db: Select_priv, Load_priv, Create_priv; mysql: Load_priv'),
                         (2, 'approved_db: Select_priv, Load_priv, Create_priv; information_schema: Create_priv'),
                         (3, 'approved_db.other: Select_priv')]:
        changed = list(grants[0])
        changed[index] = value
        rejected(lambda: verify_writer_grants([changed], columns, target))
    rejected(lambda: verify_writer_grants([], columns, target))
    lifecycle_resource = writer_resource({**resource, 'execution_writer': {
        'username': 'writer_user', 'lifecycle_privileges': ['Alter_priv', 'Drop_priv']}})
    assert lifecycle_resource['lifecycle_privileges'] == ['Alter_priv', 'Drop_priv']
    lifecycle_grants = [('', '', 'internal.approved_db: Select_priv,Load_priv,Create_priv,Alter_priv,Drop_priv',
                         '', 'normal: Usage_priv')]
    verify_writer_grants(lifecycle_grants, columns, lifecycle_resource)
    rejected(lambda: verify_writer_grants(lifecycle_grants, columns, target))
    rejected(lambda: writer_resource({**resource, 'execution_writer': {
        'username': 'writer_user', 'lifecycle_privileges': ['Admin_priv']}}))
    rejected(lambda: writer_resource({**resource, 'execution_writer': {
        'username': 'writer_user', 'lifecycle_privileges': ['Drop_priv', 'Drop_priv']}}))
    class Cursor:
        description = [(name,) for name in columns]
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql): queries.append(sql)
        def fetchone(self): return ("'writer_user'@'%'",)
        def fetchall(self): return grants
    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def cursor(self): return Cursor()
        def rollback(self): pass
    queries = []
    with patch('app.writer_credentials.pymysql.connect', return_value=Session()):
        receipt = inspect_execution_writer(target, 'fixture-only-value')
    assert queries == ['SELECT CURRENT_USER()', 'SHOW GRANTS']
    assert 'fixture-only-value' not in json.dumps(receipt)
    with tempfile.TemporaryDirectory(prefix='writer-policy-fixture-') as directory:
        path = Path(directory) / 'resources.json'
        path.write_text(json.dumps({'target': resource}), encoding='utf-8')
        with patch.dict(os.environ, {'CONNECTOR_RESOURCES_FILE': str(path), 'EXECUTION_ADAPTER': 'real'}):
            assert ResourcePolicy().get('target')['execution_writer'] == {'username': 'writer_user'}
            path.write_text(json.dumps({'target': {**resource, 'physical_cluster_id': ''}}), encoding='utf-8')
            rejected(ResourcePolicy)
    # Compiler crosses the production seam with profiles, not handcrafted facts.
    col = {'name': 'id', 'type': 'int', 'nullable': False, 'key': 'PRI'}
    table = {'name': 'approved_table', 'columns': [col]}
    profile = SimpleNamespace(id='target-profile', connection_id=connection.id,
        secret_reference_id='probe-ref', secret_version=1, resource_id='target',
        resource_fingerprint=connection.resource_fingerprint,
        schema_hash=fingerprint([table]), content={'tables': [table]})
    source_resource = {**resource, 'kind': 'mysql', 'direction': 'source'}
    source_resource.pop('execution_writer')
    source = SimpleNamespace(**{key: getattr(connection, key) for key in ('tenant_id', 'project_id',)},
        id='source-ref', kind='mysql', direction='source', status='ready', resource_id='source',
        resource_fingerprint=fingerprint(source_resource), secret_reference_id='source-secret', secret_version=1)
    source_profile = SimpleNamespace(**{**vars(profile), 'id': 'source-profile', 'connection_id': source.id,
        'resource_id': 'source', 'resource_fingerprint': source.resource_fingerprint,
        'secret_reference_id': source.secret_reference_id})
    source_secret = SecretReference(id='source-secret', tenant_id='tenant', project_id='project', version=1)
    probe_secret = SecretReference(id='probe-ref', tenant_id='tenant', project_id='project', version=1)
    db.rows.update({row.id: row for row in (source_secret, probe_secret)})
    connection.status = 'ready'
    compiler_policy = SimpleNamespace(get=lambda key: resource if key == 'target' else source_resource)
    plan = {'source': {'connection_id': source.id, 'profile_id': source_profile.id, 'table': 'approved_table'},
            'target': {'connection_id': connection.id, 'profile_id': profile.id, 'table': 'approved_table'},
            'mappings': [{'source': 'id', 'target': 'id'}]}
    with patch('app.studio_plan.get_connection', side_effect=lambda db, t, p, key: source if key == source.id else connection), \
            patch('app.studio_plan.get_profile', side_effect=lambda db, t, p, key: source_profile if key == source_profile.id else profile):
        with patch.dict(os.environ, {'EXECUTION_ADAPTER': 'real'}):
            assessment = assess_plan(db, compiler_policy, 'tenant', 'project', plan)
            assert assessment['passed'], assessment['gates']
            assert assessment['execution_facts']['target']['execution_writer'] == binding
            with patch.object(connection, 'writer_secret_reference_id', None):
                assert not assess_plan(db, compiler_policy, 'tenant', 'project', plan)['passed']
        with patch.dict(os.environ, {'EXECUTION_ADAPTER': 'l1'}), patch.object(connection, 'writer_secret_reference_id', None):
            assessment = assess_plan(db, compiler_policy, 'tenant', 'project', plan)
            assert assessment['passed'] and 'execution_writer' not in assessment['execution_facts']['target']
    # Exercise the production Maker handler with an encrypted fake-process store.
    from app.connectors import register_connector_routes
    from app.managed_secrets import SecretProvider
    routes = {}
    class Routes:
        def __getattr__(self, method):
            def register(path, **kwargs):
                def capture(handler):
                    routes[(method, path)] = handler
                    return handler
                return capture
            return register
    member = SimpleNamespace(roles=['maker'])
    register_connector_routes(Routes(), object, object, lambda *args: member)
    bind = routes[('post', '/api/tenants/{tenant_id}/projects/{project_id}/connections/{connection_id}/credentials/execution_writer')]
    import base64
    with patch.dict(os.environ, {'SECRET_MASTER_KEY': base64.b64encode(bytes(32)).decode()}):
        provider = SecretProvider()
    db.add = lambda row: db.rows.update({row.id: row})
    db.flush = lambda: None
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(connector_policy=policy, secret_provider=provider)))
    actor = SimpleNamespace(id='maker', tenant_id='tenant')
    with patch('app.evidence.lock_tenant'), patch('app.connectors.get_connection', return_value=connection), \
            patch('app.connectors.append_event'), patch('app.operation_authorization.append_event'):
        connection.writer_secret_reference_id, connection.writer_secret_version = None, None
        probe_before = (connection.secret_reference_id, connection.secret_version)
        view = bind('tenant', 'project', connection.id, parsed, request, db, actor)
        encrypted = db.get(SecretReference, view['writer_secret_reference_id'])
        assert provider.open(encrypted) == 'fixture-only-value'
        assert b'fixture-only-value' not in encrypted.ciphertext
        assert (connection.secret_reference_id, connection.secret_version) == probe_before
        assert bind('tenant', 'project', connection.id, parsed, request, db, actor).status_code == 409
        member.roles = ['auditor']
        assert bind('tenant', 'project', connection.id, parsed, request, db, actor).status_code == 403
    ddl = str(CreateTable(Connection.__table__).compile(dialect=postgresql.dialect()))
    assert 'connection_writer_secret_fk' in ddl and 'connection_writer_shape' in ddl
    print(json.dumps({'passed': ['deployment scope cannot expand; independent target writer only',
        'cross-project and frozen writer-version drift denied', 'safe references and secret input redaction',
        'writer grants reject roles/global/cross-db/extra privileges; metadata still rejects LOAD',
        'writer authorization emits only two fixed read queries', 'real compiler requires/freezes writer; L1 compatible',
        'Maker handler encrypts initial writer only; repeat/non-Maker denied; probe slot unchanged',
        'PostgreSQL composite FK and slot shape compiled'], 'real_l2_claim': False}))


def migration_fixture():
    import uuid
    from app import migrations
    from app.db import Base
    from app.models import Project, SchemaMigration, Tenant
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import Session
    name = 'writer_fixture_' + uuid.uuid4().hex
    admin = create_engine(os.environ['DATABASE_URL'], hide_parameters=True)
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA {name}'))
    isolated = create_engine(os.environ['DATABASE_URL'], hide_parameters=True,
                             connect_args={'options': '-csearch_path=' + name})
    try:
        with isolated.begin() as conn:
            Base.metadata.create_all(conn)
        with Session(isolated) as db, db.begin():
            db.add(Tenant(id='fixture-tenant', slug='fixture-tenant', name='Fixture'))
            db.flush()
            db.add_all([Project(id=key, tenant_id='fixture-tenant', slug=key, name='Fixture')
                        for key in ('fixture-project', 'other-project')])
            db.flush()
            db.add_all([SecretReference(id=key, tenant_id='fixture-tenant', project_id=project, version=1,
                                       nonce=b'fixture-nonce', ciphertext=b'fixture-ciphertext')
                        for key, project in [('probe', 'fixture-project'), ('writer', 'fixture-project'),
                                             ('other-writer', 'other-project')]])
            db.flush()
            db.add(Connection(id='existing-target', tenant_id='fixture-tenant', project_id='fixture-project',
                name='Existing', resource_id='target', kind='doris', direction='target',
                resource_fingerprint='f' * 64, secret_reference_id='probe', secret_version=1))
        with isolated.begin() as conn:
            conn.execute(text('ALTER TABLE connections DROP CONSTRAINT connection_writer_secret_fk, '
                              'DROP CONSTRAINT connection_writer_shape, DROP COLUMN writer_secret_reference_id, '
                              'DROP COLUMN writer_secret_version'))
        with patch.object(migrations, 'engine', isolated):
            migrations.initialize_database()
            migrations.initialize_database()
        with Session(isolated) as db:
            assert db.get(SchemaMigration, 'v13-execution-writer-slot') is not None
            constraints = set(db.scalars(text("SELECT conname FROM pg_constraint WHERE conrelid='connections'::regclass")))
            assert {'connection_writer_secret_fk', 'connection_writer_shape'} <= constraints
            row = db.get(Connection, 'existing-target')
            assert row.name == 'Existing' and row.secret_reference_id == 'probe'
            assert row.writer_secret_reference_id is None and row.writer_secret_version is None
            row.writer_secret_reference_id, row.writer_secret_version = 'writer', 1
            db.flush()
            for invalid in [('other-writer', 1), ('writer', 2), ('writer', None), ('probe', 1), (None, 1)]:
                try:
                    with db.begin_nested():
                        row.writer_secret_reference_id, row.writer_secret_version = invalid
                        db.flush()
                except IntegrityError:
                    db.refresh(row)
                else:
                    raise AssertionError('invalid database writer binding accepted')
        print(json.dumps({'passed': ['existing-schema migration twice preserves old connection and probe refs',
                                     'PostgreSQL rejects cross-project/version/partial/reused-probe writer bindings'],
                          'real_l2_claim': False}))
    finally:
        isolated.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA {name} CASCADE'))
        admin.dispose()


if __name__ == '__main__':
    run()
    if '--postgres' in sys.argv:
        migration_fixture()
