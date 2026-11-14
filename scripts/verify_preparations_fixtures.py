"""Isolated V07 failure fixtures; run in the API image with /tests on PYTHONPATH.

Uses the production HTTP app and real PostgreSQL, cloned into a disposable schema.
No fault endpoints are added. Clock/resource failures are test-process fixtures.
External reads use existing real managed connectors except the single Schema reply
fixture. No root control-plane rows, source data, or deployment policy are changed.
"""
import copy
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select

from app import main, preparations
from app.connector_policy import ResourcePolicy
from isolated_api_fixture import isolated_api
from app.models import Connection, MetadataProfile, SecretReference
from verify_identity import Client


def run():
    with isolated_api('v07_fixture', 18008) as (session, _schema):
        policy = ResourcePolicy()
        original_policy = policy.path.read_text(encoding='utf-8')
        with tempfile.TemporaryDirectory(prefix='v07-policy-') as directory:
            policy.path = Path(directory) / 'resources.json'
            policy.path.write_text(original_policy, encoding='utf-8')
            main.app.state.connector_policy = policy
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
            # Cross the exact deadline during real managed connector I/O. Both
            # decision values share the same immutable Preparation deadline.
            boundary = prepare()
            boundary_path = base + '/' + boundary['id']
            expires_at = datetime.fromisoformat(boundary['expires_at'])
            class BoundaryClock(datetime):
                current = expires_at - timedelta(seconds=1)
                @classmethod
                def now(cls, tz=None):
                    return cls.current.astimezone(tz or timezone.utc)
            real_boundary_probe = preparations.inspect_resource
            def crossing_probe(*args):
                result = real_boundary_probe(*args)
                BoundaryClock.current = expires_at
                return result
            for decision in ('approve', 'reject'):
                BoundaryClock.current = expires_at - timedelta(seconds=1)
                with patch.object(preparations, 'datetime', BoundaryClock), patch.object(
                        preparations, 'inspect_resource', crossing_probe):
                    data.call('POST', boundary_path + '/decisions', {**approved, 'decision': decision}, 409)
                unchanged = maker.call('GET', boundary_path)
                assert not unchanged['decisions'] and unchanged['expires_at'] == boundary['expires_at']
            boundary_events = [event for event in auditor.call('GET', scope + '/evidence')['events']
                               if event['preparation_id'] == boundary['id']]
            assert sum(event['action'] == 'approval.denied' for event in boundary_events) == 2
            assert not any(event['action'] == 'approval.decided' for event in boundary_events)
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
                'approve/reject crossing exact expiry during managed I/O: 409, no decision/approval.decided, durable denial, unchanged deadline',
                'schema/resource-policy/compiler/secret drift invalidation',
                'ordinary samples/statistics excluded from drift',
                'GET/list perform no external calls; explicit managed Schema denial persists evidence'],
                'fixtures': 'test-process clock and managed Schema reply; disposable real PostgreSQL schema',
                'no_production_fault_routes': True}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    run()
