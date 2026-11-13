"""V08 isolated HTTP fixtures with real PostgreSQL commit failure and public-key verification.

Run in deployed API image with scripts mounted/copied to /tests, PYTHONPATH=/app:/tests.
Only the disposable schema is written; clocks and managed-check wrapper are process-local.
"""
import copy
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from unittest.mock import patch
from uuid import uuid4

import uvicorn
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app import execution, main, preparations
from app.command_signing import COMMAND_FORMAT, CAPABILITY_FORMAT, verify_envelope
from app.connector_policy import ResourcePolicy
from app.db import Base, engine
from app.evidence_signing import EvidenceSigner
from app.managed_secrets import SecretProvider
from verify_identity import Client


def run():
    schema = 'v08_fixture_' + uuid4().hex
    isolated = create_engine(engine.url, connect_args={'options': f'-csearch_path={schema}'})
    session = sessionmaker(isolated, expire_on_commit=False)
    with engine.begin() as db:
        db.execute(text(f'CREATE SCHEMA {schema}'))
        Base.metadata.create_all(db.execution_options(schema_translate_map={None: schema}))
        for table in Base.metadata.sorted_tables:
            columns = ','.join(f'"{column.name}"' for column in table.columns)
            db.execute(text(f'INSERT INTO {schema}."{table.name}" ({columns}) SELECT {columns} FROM public."{table.name}"'))
    server = thread = None
    try:
        with session() as db:
            assert db.scalar(text('SELECT current_schema()')) == schema
        def fixture_database():
            with session.begin() as db:
                yield db
        main.app.dependency_overrides[main.database] = fixture_database
        main.app.state.secret_provider = SecretProvider()
        main.app.state.evidence_signer = EvidenceSigner()
        main.app.state.connector_policy = ResourcePolicy()
        server = uvicorn.Server(uvicorn.Config(main.app, host='127.0.0.1', port=18009, lifespan='off', log_level='critical'))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        for _ in range(100):
            if server.started:
                break
            time.sleep(0.05)
        assert server.started
        credentials = json.loads(Path('/run/identity/bootstrap-demo.json').read_text())
        clients = {}
        for role in ('maker', 'data_checker', 'security_checker', 'operator', 'auditor', 'admin'):
            clients[role] = Client('http://127.0.0.1:18009', [])
            clients[role].login(credentials, role)
        maker, data, security, operator, auditor, admin = [clients[r] for r in clients]
        scope = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
        version_id = maker.call('GET', scope + '/versions')[0]['id']
        def prepare(tool='pipeline.publish'):
            row = maker.call('POST', scope + '/preparations', {'version_id': version_id, 'tool': tool}, 201)
            path = scope + '/preparations/' + row['id']
            for client, slot in ((data, 'data'), (security, 'security')):
                client.call('POST', path + '/decisions', {'slot': slot, 'decision': 'approve'})
            envelope = operator.call('POST', path + '/capabilities', {}, 201)['capability']
            return row, path, {'capability': envelope, 'idempotency_key': str(uuid4())}
        passed = []
        row, path, body = prepare('pipeline.trial')
        assert all(body['capability']['payload']['budget'][key] <= limit for key, limit in {'max_rows': 10000, 'max_bytes': 16777216, 'max_seconds': 120}.items())
        real_clock = execution.datetime
        class Clock(datetime):
            offset = timedelta()
            @classmethod
            def now(cls, tz=None):
                return real_clock.now(tz) + cls.offset
        original_check = execution.check_execution_resources
        def slow_check(*args, **kwargs):
            result = original_check(*args, **kwargs)
            Clock.offset = timedelta(seconds=301)
            return result
        with patch.object(execution, 'datetime', Clock), patch.object(execution, 'check_execution_resources', slow_check):
            operator.call('POST', path + '/commit', body, 409)
        assert not any(run['preparation_id'] == row['id'] for run in auditor.call('GET', scope + '/runs'))
        passed.append('capability expires during bounded real managed preflight: final acceptance denies without consuming')
        # Ordinary expiration and Preparation expiration also deny at Commit.
        Clock.offset = timedelta(seconds=300)
        with patch.object(execution, 'datetime', Clock):
            operator.call('POST', path + '/commit', body, 409)
        Clock.offset = timedelta(hours=25)
        with patch.object(preparations, 'datetime', Clock):
            operator.call('POST', path + '/commit', body, 409)
        Clock.offset = timedelta()
        def preparation_expires(*args, **kwargs):
            result = original_check(*args, **kwargs)
            Clock.offset = timedelta(hours=25)
            return result
        with patch.object(execution, 'datetime', Clock), patch.object(execution, 'check_execution_resources', preparation_expires):
            operator.call('POST', path + '/commit', body, 409)
        Clock.offset = timedelta()
        with patch.object(preparations, 'POLICY_VERSION', 'isolated-policy-v2'):
            operator.call('POST', path + '/commit', body, 409)
        real_probe = preparations.inspect_resource
        def changed_schema(resource, credential):
            result = copy.deepcopy(real_probe(resource, credential))
            result['tables'][0]['columns'].append({'name': 'v08_drift', 'type': 'int', 'nullable': True})
            return result
        with patch.object(preparations, 'inspect_resource', changed_schema):
            operator.call('POST', path + '/commit', body, 409)
        passed.append('Preparation deadline during managed I/O, current policy-version drift and real-resource Schema reply drift all deny before consumption')
        # A second current Operator gets its own subject-bound Capability. It
        # cannot use the first subject's token or change an accepted run subject.
        security_path = scope + '/members/' + credentials['accounts']['security_checker']['id']
        admin.call('PUT', security_path, {'roles': ['security_checker', 'operator']})
        other_capability = security.call('POST', path + '/capabilities', {}, 201)['capability']
        security.call('POST', path + '/commit', {**body, 'idempotency_key': str(uuid4())}, 403)
        # Current authorization and frozen facts are rechecked after issuance.
        maker_account = f"/api/tenants/{credentials['tenant_id']}/accounts/{credentials['accounts']['maker']['id']}"
        admin.call('PATCH', maker_account, {'enabled': False})
        operator.call('POST', path + '/commit', body, 409)
        admin.call('PATCH', maker_account, {'enabled': True})
        operator_account = f"/api/tenants/{credentials['tenant_id']}/accounts/{credentials['accounts']['operator']['id']}"
        admin.call('PATCH', operator_account, {'enabled': False})
        operator.call('POST', path + '/commit', body, 401)
        admin.call('PATCH', operator_account, {'enabled': True})
        checker_path = scope + '/members/' + credentials['accounts']['data_checker']['id']
        admin.call('PUT', checker_path, {'roles': ['auditor']})
        operator.call('POST', path + '/commit', body, 409)
        admin.call('PUT', checker_path, {'roles': ['data_checker']})
        operator_path = scope + '/members/' + credentials['accounts']['operator']['id']
        admin.call('PUT', operator_path, {'roles': ['auditor']})
        operator.call('POST', path + '/commit', body, 403)
        admin.call('PUT', operator_path, {'roles': ['operator']})
        with session.begin() as db:
            frozen = db.execute(text('SELECT frozen_sha256 FROM preparations WHERE id=:id'), {'id': row['id']}).scalar_one()
            db.execute(text('UPDATE preparations SET frozen_sha256=:hash WHERE id=:id'), {'hash': '0' * 64, 'id': row['id']})
        operator.call('POST', path + '/commit', body, 403)
        with session.begin() as db:
            db.execute(text('UPDATE preparations SET frozen_sha256=:hash WHERE id=:id'), {'hash': frozen, 'id': row['id']})
        altered = copy.deepcopy(body)
        altered['capability']['payload']['tool'] = 'metadata.read'
        operator.call('POST', path + '/commit', altered, 403)
        passed.append('Commit rechecks expiry, current Operator/Checker roles, frozen bindings and signed tampering')
        # Genuine PostgreSQL failure at transaction COMMIT, after business inserts
        # AND both evidence projections/heads. This is not a Python exception stub.
        def snapshot():
            with session() as db:
                counts = {name: db.scalar(text(f'SELECT count(*) FROM {name}')) for name in (
                    'execution_runs', 'managed_commands', 'execution_outbox', 'capability_consumptions',
                    'commit_idempotency', 'identity_events', 'evidence_events')}
                heads = db.execute(text('SELECT tenant_id,scope,sequence,event_hash FROM evidence_heads ORDER BY tenant_id,scope')).all()
                tenants = db.execute(text('SELECT id,evidence_seq FROM tenants ORDER BY id')).all()
                return counts, heads, tenants
        before = snapshot()
        with session.begin() as db:
            db.execute(text("CREATE FUNCTION fail_acceptance() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                "IF NEW.action='commit.accepted' THEN RAISE EXCEPTION 'v08 isolated commit failure'; END IF; RETURN NEW; END $$"))
            db.execute(text('CREATE CONSTRAINT TRIGGER fail_acceptance AFTER INSERT ON evidence_events '
                            'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_acceptance()'))
        operator.call('POST', path + '/commit', body, 503)
        assert snapshot() == before, 'partial business/evidence/chain head facts escaped rollback'
        assert not any(run['preparation_id'] == row['id'] for run in auditor.call('GET', scope + '/runs'))
        with session.begin() as db:
            db.execute(text('DROP TRIGGER fail_acceptance ON evidence_events'))
            db.execute(text('DROP FUNCTION fail_acceptance()'))
        accepted = operator.call('POST', path + '/commit', body, 201)
        assert accepted['managed_command']['payload']['budget'] == row['frozen']['budget']
        assert operator.call('POST', path + '/commit', body) == accepted
        security.call('POST', path + '/commit', {'capability': other_capability, 'idempotency_key': str(uuid4())}, 409)
        assert auditor.call('GET', scope + '/runs/' + accepted['id'])['subject'] == credentials['accounts']['operator']['id']
        admin.call('PUT', security_path, {'roles': ['security_checker']})
        passed.append('second Operator cannot use another subject token or create a new Run/change original subject after acceptance')
        passed.append('real PostgreSQL deferred-trigger COMMIT failure rolls back all five acceptance tables, evidence rows, chain heads and tenant sequence; same jti/key subsequently succeeds')
        # Receipt retry requires current role, but does not renew/reconsume an
        # already accepted command when the original Capability later expires.
        Clock.offset = timedelta(hours=25)
        with patch.object(execution, 'datetime', Clock), patch.object(preparations, 'datetime', Clock):
            assert operator.call('POST', path + '/commit', body) == accepted
        admin.call('PUT', operator_path, {'roles': ['auditor']})
        operator.call('POST', path + '/commit', body, 403)
        admin.call('PUT', operator_path, {'roles': ['operator']})
        public = main.app.state.evidence_signer.public['public_key']
        command = accepted['managed_command']
        assert verify_envelope(command, public, COMMAND_FORMAT)['run_id'] == accepted['id']
        for envelope, format_ in ((command, CAPABILITY_FORMAT), (body['capability'], COMMAND_FORMAT)):
            try:
                verify_envelope(envelope, public, format_)
                raise AssertionError('cross-format substitution accepted')
            except ValueError:
                pass
        # Independent worker-like process: no signing seed or app/database config.
        environment = {k: v for k, v in os.environ.items() if k not in ('EVIDENCE_SIGNING_KEY', 'SECRET_MASTER_KEY', 'DATABASE_URL')}
        program = "import json,sys; from app.command_signing import verify_envelope,COMMAND_FORMAT; x=json.load(sys.stdin); assert verify_envelope(x['envelope'],x['public'],COMMAND_FORMAT)['run_id']==x['run_id']"
        subprocess.run(['python', '-c', program], input=json.dumps({'envelope': command, 'public': public,
                       'run_id': accepted['id']}), text=True, env=environment, check=True)
        passed.append('expired-capability identical retry returns original receipt after current-role check; accepted command verifies in public-key-only process; formats cannot substitute')
        events = auditor.call('GET', scope + '/evidence')['events']
        assert sum(event['action'] == 'commit.accepted' and event['run_id'] == accepted['id'] for event in events) == 1
        return {'passed': passed, 'fixtures': 'disposable real PostgreSQL schema, test-process clock, real deferred constraint trigger; public HTTP application', 'no_production_fault_routes': True}
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
    print(json.dumps(run(), ensure_ascii=False, indent=2))
