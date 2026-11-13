"""Isolated V07 failure fixtures; run in the API image with /tests on PYTHONPATH.

Uses the production HTTP app and real PostgreSQL, cloned into a disposable schema.
No fault endpoints are added. Clock/resource failures are test-process fixtures.
External reads use existing real managed connectors except the single Schema reply
fixture. No root control-plane rows, source data, or deployment policy are changed.
"""
import copy
import json
import tempfile
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch

import uvicorn
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from app import main, preparations
from app.connector_policy import ResourcePolicy
from app.db import Base, engine
from app.evidence_signing import EvidenceSigner
from app.managed_secrets import SecretProvider
from app.models import Connection, MetadataProfile, SecretReference
from verify_identity import Client


def run():
    schema = 'v07_fixture_' + uuid4().hex
    isolated = create_engine(engine.url, connect_args={'options': f'-csearch_path={schema}'})
    session = sessionmaker(isolated, expire_on_commit=False)
    with engine.begin() as db:
        db.execute(text(f'CREATE SCHEMA {schema}'))
        Base.metadata.create_all(db.execution_options(schema_translate_map={None: schema}))
        for table in Base.metadata.sorted_tables:
            columns = ','.join(f'"{column.name}"' for column in table.columns)
            db.execute(text(f'INSERT INTO {schema}."{table.name}" ({columns}) SELECT {columns} FROM public."{table.name}"'))
    server = None
    thread = None
    try:
        with session() as db:
            assert db.scalar(text('SELECT current_schema()')) == schema
        def fixture_database():
            with session.begin() as db:
                yield db
        main.app.dependency_overrides[main.database] = fixture_database
        main.app.state.secret_provider = SecretProvider()
        main.app.state.evidence_signer = EvidenceSigner()
        policy = ResourcePolicy()
        original_policy = policy.path.read_text(encoding='utf-8')
        with tempfile.TemporaryDirectory(prefix='v07-policy-') as directory:
            policy.path = Path(directory) / 'resources.json'
            policy.path.write_text(original_policy, encoding='utf-8')
            main.app.state.connector_policy = policy
            server = uvicorn.Server(uvicorn.Config(main.app, host='127.0.0.1', port=18008,
                                    lifespan='off', log_level='error'))
            thread = threading.Thread(target=server.run, daemon=True)
            thread.start()
            for _ in range(100):
                if server.started:
                    break
                time.sleep(0.05)
            assert server.started, 'isolated API did not start'
            credentials = json.loads(Path('/run/identity/bootstrap-demo.json').read_text())
            maker, data, auditor = [Client('http://127.0.0.1:18008', []) for _ in range(3)]
            maker.login(credentials, 'maker')
            data.login(credentials, 'data_checker')
            auditor.login(credentials, 'auditor')
            tenant, project = credentials['tenant_id'], credentials['project_id']
            scope = f'/api/tenants/{tenant}/projects/{project}'
            version_id = maker.call('GET', scope + '/versions')[0]['id']
            version = maker.call('GET', scope + '/versions/' + version_id)
            base = scope + '/preparations'
            def prepare():
                return maker.call('POST', base, {'version_id': version_id, 'tool': 'pipeline.publish'}, 201)
            row = prepare()
            target = base + '/' + row['id']
            approved = {'slot': 'data', 'decision': 'approve'}
            real_clock = preparations.datetime
            class ExpiredClock(datetime):
                @classmethod
                def now(cls, tz=None):
                    return real_clock.now(tz) + timedelta(hours=25)
            with patch.object(preparations, 'datetime', ExpiredClock):
                expired = maker.call('GET', target)
                assert expired['validation']['state'] == 'expired'
                data.call('POST', target + '/decisions', approved, 409)
            # A new profile containing only sample/statistics differences does
            # not alter execution facts or invalidate a frozen preparation.
            source = version['frozen']['execution_facts']['source']
            with session.begin() as db:
                profile = db.scalar(select(MetadataProfile).where(MetadataProfile.connection_id == source['connection_id'])
                                    .order_by(MetadataProfile.created_at.desc(), MetadataProfile.id.desc()).limit(1))
                contents = copy.deepcopy(profile.content)
                contents['statistics'] = {'fixture': 'changed ordinary data'}
                contents['samples'] = [{'fixture': 'changed sample'}]
                fresh = MetadataProfile(tenant_id=profile.tenant_id, project_id=profile.project_id,
                    connection_id=profile.connection_id, secret_reference_id=profile.secret_reference_id,
                    secret_version=profile.secret_version, resource_id=profile.resource_id,
                    resource_fingerprint=profile.resource_fingerprint, schema_hash=profile.schema_hash, content=contents)
                db.add(fresh)
                db.flush()
                profile_id = fresh.id
            assert maker.call('GET', target)['validation']['state'] == 'pending'
            with session.begin() as db:
                db.get(MetadataProfile, profile_id).schema_hash = '0' * 64
            assert maker.call('GET', target)['validation']['state'] == 'invalidated'
            data.call('POST', target + '/decisions', approved, 409)
            with session.begin() as db:
                db.get(MetadataProfile, profile_id).schema_hash = source['schema_hash']
            # Independent local resource-policy/compiler/secret bindings drift.
            policy.path.write_text('{}', encoding='utf-8')
            assert maker.call('GET', target)['validation']['state'] == 'invalidated'
            policy.path.write_text(original_policy, encoding='utf-8')
            with patch.dict(preparations.VERSIONS, {'policy': 'fixture-policy-v2'}):
                assert maker.call('GET', target)['validation']['state'] == 'invalidated'
            with session.begin() as db:
                connection = db.get(Connection, source['connection_id'])
                secret = db.get(SecretReference, connection.secret_reference_id)
                replacement = SecretReference(tenant_id=secret.tenant_id, project_id=secret.project_id,
                    version=secret.version + 1, nonce=secret.nonce, ciphertext=secret.ciphertext)
                db.add(replacement)
                db.flush()
                connection.secret_reference_id, connection.secret_version = replacement.id, replacement.version
            assert maker.call('GET', target)['validation']['state'] == 'invalidated'
            with session.begin() as db:
                connection = db.get(Connection, source['connection_id'])
                connection.secret_reference_id, connection.secret_version = source['secret_reference_id'], source['secret_version']
            # Explicit managed Schema-check failure: no fault route, response
            # differs only inside this process. Plain views never call the exit.
            real_probe = preparations.inspect_resource
            def changed_schema(resource, credential):
                content = copy.deepcopy(real_probe(resource, credential))
                content['tables'][0]['columns'].append({'name': 'fixture_drift', 'type': 'int', 'nullable': True})
                return content
            with patch.object(preparations, 'inspect_resource', side_effect=AssertionError('GET must stay local')):
                maker.call('GET', target)
                maker.call('GET', base)
            with patch.object(preparations, 'inspect_resource', changed_schema):
                data.call('POST', target + '/decisions', approved, 409)
                maker.call('POST', base, {'version_id': version_id, 'tool': 'pipeline.publish'}, 409)
            events = auditor.call('GET', scope + '/evidence')['events']
            assert any(event['action'] == 'preparation.resources_check_denied' for event in events)
            assert not maker.call('GET', target)['decisions']
            print(json.dumps({'passed': ['isolated PostgreSQL HTTP expiry/no renewal',
                'schema/resource-policy/compiler/secret drift invalidation',
                'ordinary samples/statistics excluded from drift',
                'GET/list perform no external calls; explicit managed Schema denial persists evidence'],
                'fixtures': 'test-process clock and managed Schema reply; disposable real PostgreSQL schema',
                'no_production_fault_routes': True}, ensure_ascii=False, indent=2))
    finally:
        if server:
            server.should_exit = True
        if thread:
            thread.join(timeout=5)
        main.app.dependency_overrides.clear()
        isolated.dispose()
        with engine.begin() as db:
            db.execute(text(f'DROP SCHEMA {schema} CASCADE'))


if __name__ == '__main__':
    run()
