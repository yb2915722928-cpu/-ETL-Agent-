"""Offline local-HTTP contract checks; no real Doris/L2 qualification."""
import base64
from contextlib import contextmanager
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time
from unittest.mock import patch

from app.connector_policy import fingerprint
from app.doris_transport import DorisStreamTransport, object_identity_id
from app.managed_load import FrozenLoadBinding


def main():
    requests = []
    scenario = {'status': 200, 'raw': b'{"Label":"fixture"}'}
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def log_message(self, *args):
            pass
        def do_PUT(self):
            body = self.rfile.read(int(self.headers['Content-Length']))
            requests.append((self.path, dict(self.headers), body))
            self.send_response(scenario['status'])
            if scenario['status'] == 307:
                self.send_header('Location', '/credential-exfiltration')
            framing = scenario.get('framing', 'length')
            if scenario.get('close'):
                self.send_header('Connection', 'close')
                self.close_connection = True
            if framing.startswith('chunked'):
                self.send_header('Transfer-Encoding', 'chunked')
            elif framing != 'unframed':
                extra = 7 if framing == 'length_truncated' else 0
                self.send_header('Content-Length', str(len(scenario['raw']) + extra))
            self.end_headers()
            try:
                raw = scenario['raw']
                if framing.startswith('chunked'):
                    if framing == 'chunked_partial_data':
                        raw = ('%x\r\n' % (len(raw) + 7)).encode() + raw
                    else:
                        raw = ('%x\r\n' % len(raw)).encode() + raw + b'\r\n'
                        if framing == 'chunked':
                            raw += b'0\r\n\r\n'
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    secret = 'fixture-writer-secret'
    resource = {'kind': 'doris', 'direction': 'target', 'physical_cluster_id': 'cluster',
                'database': 'db', 'username': 'writer', 'tables': ['target']}
    identity = {'physical_cluster_id': 'cluster', 'database': 'db', 'table': 'shadow_fixture',
                'marker': 'etl_agent_object:command', 'partition_ids': ['7'],
                'schema': [{'name': 'id', 'type': 'bigint', 'nullable': False}]}
    identity['schema_sha256'] = fingerprint(identity['schema'])
    binding = FrozenLoadBinding('command', 'run', 'job', 'a' * 64, 'b' * 64, 'cluster', 'db',
                                'shadow_fixture', object_identity_id(identity), 'writer-ref', 3, ('be1',))
    route = {'host': '127.0.0.1', 'port': server.server_port, 'backend_id': 'be1'}
    resolved = []
    def resolve(frozen):
        assert frozen.writer_reference_id == 'writer-ref' and frozen.writer_version == 3
        resolved.append(frozen)
        return resource, secret, identity, route
    transport = DorisStreamTransport(resolve)
    label = 'etl_' + 'c' * 64
    body = b'{"id":"9007199254740993"}\n{"id":"2"}\n'
    def unknown(call):
        try:
            call()
        except OSError as error:
            assert str(error) == 'doris_stream_transport_unknown'
            assert secret not in str(error) and error.__suppress_context__
        else:
            raise AssertionError('expected unknown')
    try:
        receipt = transport.send(binding, label, body)
        assert receipt.raw == scenario['raw'] and receipt.object_id == binding.object_id and receipt.backend_id == 'be1'
        path, headers, observed = requests[-1]
        assert path == '/api/db/shadow_fixture/_stream_load' and observed == body
        assert headers['Authorization'] == 'Basic ' + base64.b64encode(('writer:' + secret).encode()).decode()
        expected = {'format': 'json', 'read_json_by_line': 'true', 'strip_outer_array': 'false',
                    'strict_mode': 'true', 'max_filter_ratio': '0', 'num_as_string': 'true',
                    'label': label, 'two_phase_commit': 'false'}
        assert all(headers.get(key) == value for key, value in expected.items())
        # The journal route ID differs from the approved creator ID. Its complete
        # frozen object hash still binds that original marker and partition set.
        route_binding = replace(binding, command_id='rejected-route-journal')
        assert transport.send(route_binding, label, body).object_id == binding.object_id
        # Real TCP connects before authority is checked. A denied callback must
        # send no HTTP bytes and must never be called through a retry.
        import http.client
        original_connect = http.client.HTTPConnection.connect
        connected, callbacks = [], []
        def connect(connection):
            original_connect(connection)
            connected.append(connection)
        def guarded(frozen):
            assert frozen is binding and connected and connected[-1].sock is not None
            callbacks.append(frozen)
        before = len(requests)
        with patch('app.doris_transport.http.client.HTTPConnection.connect', connect):
            @contextmanager
            def denied(frozen):
                guarded(frozen)
                raise ValueError('send_authorization_denied')
                yield  # Admission denial must happen before guard entry completes.
            unknown(lambda: DorisStreamTransport(resolve, before_send=denied).send(binding, label, body))
            assert len(requests) == before and len(callbacks) == 1
            @contextmanager
            def failed(frozen):
                guarded(frozen)
                raise ValueError(secret)
                yield
            unknown(lambda: DorisStreamTransport(resolve, before_send=failed).send(binding, label, body))
            assert len(requests) == before and len(callbacks) == 2
            # A short real callback crosses a shortened test deadline; the
            # actual production budget remains 1.5 seconds.
            @contextmanager
            def expired(frozen):
                guarded(frozen)
                time.sleep(0.04)
                yield
            with patch('app.doris_transport.HTTP_BUDGET_SECONDS', 0.02):
                unknown(lambda: DorisStreamTransport(resolve, before_send=expired).send(binding, label, body))
            assert len(requests) == before and len(callbacks) == 3
            exited = []
            @contextmanager
            def allowed(frozen):
                guarded(frozen)
                yield
                # The original HTTP response has completed before guard exit.
                assert len(requests) == before + 1
                exited.append(True)
            assert DorisStreamTransport(resolve, before_send=allowed).send(binding, label, body).raw == scenario['raw']
            assert len(requests) == before + 1 and len(callbacks) == 4 and exited == [True]
        # Doris closes its socket when the declared response is consumed. Do
        # not touch that socket again after length reaches zero.
        for framing in ('length', 'chunked'):
            scenario.update(status=200, raw=b'{"complete":true}', close=True, framing=framing)
            before = len(requests)
            assert transport.send(binding, label, body).raw == scenario['raw']
            assert len(requests) == before + 1
        # Even syntactically complete JSON cannot redeem incomplete HTTP framing.
        for framing in ('length_truncated', 'chunked_no_terminator', 'chunked_partial_data', 'unframed'):
            scenario.update(framing=framing)
            before = len(requests)
            unknown(lambda: transport.send(binding, label, body))
            assert len(requests) == before + 1
        scenario.update(framing='length', close=False)
        for status in (307, 500, 401):
            scenario.update(status=status, raw=secret.encode())
            before = len(requests)
            unknown(lambda: transport.send(binding, label, body))
            assert len(requests) == before + 1
        scenario.update(status=200, raw=b'x' * 65537)
        unknown(lambda: transport.send(binding, label, body))
        before = len(requests)
        for bad in (replace(binding, object_id='d' * 64), replace(binding, backend_ids=('other',)),
                    replace(binding, database='other'), replace(binding, table='../escape')):
            unknown(lambda: transport.send(bad, label, body))
        for malformed in (b'[1,2]', b'{"id":1}\npartial', b'{"id":NaN}', b'{"id":1,"id":2}', b''):
            unknown(lambda: transport.send(binding, label, malformed))
        assert len(requests) == before
        def revoked(frozen):
            raise ValueError(secret)
        unknown(lambda: DorisStreamTransport(revoked).send(binding, label, body))
        with patch('app.doris_transport.http.client.HTTPConnection') as connection:
            connection.return_value.connect.side_effect = OSError(secret)
            unknown(lambda: transport.send(binding, label, body))
            assert connection.call_count == 1 and connection.call_args.kwargs['timeout'] < 2
            connection.return_value.request.assert_not_called()
        assert len(resolved) > len(requests)  # Resolver called afresh even for denied calls.
        print(json.dumps({'passed': ['single direct BE PUT and fixed JSON/non-2PC headers',
            'redirect/non-2xx are unknown; exactly one request, no follow or retry',
            'frozen physical object/backend drift and malformed JSON denied before I/O',
            'Connection close succeeds only with complete Content-Length or terminated chunks',
            'premature Content-Length/chunk EOF and unframed response remain unknown without retry',
            'response bounded; secret-bearing resolver/network errors redacted',
            'fresh resolver on each send; HTTP timeout below two seconds',
            'after-connect before-send callback denial/error/budget exhaustion send zero PUTs',
            'successful callback sends once within the shared HTTP deadline'], 'real_l2_claim': False}))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def postgres_guard():
    """Real PostgreSQL current authority and tenant lock; injected network/object I/O."""
    import copy
    import http.client
    import os
    from pathlib import Path
    from tempfile import TemporaryDirectory
    from sqlalchemy import select
    from sqlalchemy.exc import OperationalError
    from app import real_runtime
    from app.execution import ManagedCommand
    from app.execution_runtime import EngineAttempt
    from app.models import Membership, SecretReference, Tenant
    from app.real_ingress import IngressBinding
    from verify_real_runtime_fixtures import database, seed, Objects, Engine

    puts, locked = [], []
    store = {}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_PUT(self):
            self.rfile.read(int(self.headers['Content-Length']))
            puts.append(self.path)
            # This independent connection cannot acquire the tenant row while
            # the production before_send context owns its authorization lock.
            with store['sessions'].begin() as db:
                try:
                    db.scalar(select(Tenant).where(Tenant.id == 'tenant').with_for_update(nowait=True))
                except OperationalError as error:
                    assert error.orig.sqlstate == '55P03'
                    locked.append(True)
                else:
                    locked.append(False)
            raw = b'{"Status":"Success"}'
            self.send_response(200)
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    receipt = json.dumps({'schema': 'managed-network-receipt/v1', 'resource_state': 'kept',
        'verification_phase': 'isolated_network_observed',
        'engine': {'ip': '192.0.2.4', 'rest_port': 8080, 'file_encoding': 'UTF-8'},
        'gateway': {'ip': '192.0.2.5', 'ingress_port': 8080},
        'http_connector_sha256': real_runtime.HTTP_CONNECTOR['sha256'],
        'fixture': 'network/object seams, not real L2'}, sort_keys=True, separators=(',', ':')).encode()
    try:
        with database() as (sessions, signer), TemporaryDirectory(prefix='transport-guard-fixture-') as directory:
            store['sessions'] = sessions
            receipt_path = Path(directory) / 'receipt.json'
            receipt_path.write_bytes(receipt)
            with patch.dict(os.environ, {'REAL_NETWORK_RECEIPT_FILE': str(receipt_path)}), \
                 patch('app.preparations.current_execution_facts', side_effect=lambda db, policy, version:
                       copy.deepcopy(version.frozen['execution_facts'])), \
                 patch('app.real_runtime._external_authorization'), \
                 patch('app.real_runtime._network_verified', side_effect=lambda entry, raw: raw == receipt):
                cid, policy, provider, manifest = seed(sessions, signer, receipt)
                manifest['resources']['target']['backend_route'] = {
                    'host': '127.0.0.1', 'port': server.server_port, 'backend_id': 'be-1'}
                objects, engine = Objects(sessions), Engine(sessions, cid)
                assert real_runtime.submit_real(cid, sessions=sessions, policy=policy, secrets=provider,
                    manifest=manifest, client_factory=lambda _: engine,
                    network_verifier=lambda entry, raw: raw == receipt,
                    objects_factory=objects.factory) == 'submitted'
                with sessions() as db:
                    facts = db.get(IngressBinding, cid).facts
                    command, attempt = db.get(ManagedCommand, cid), db.get(EngineAttempt, cid)
                    binding = real_runtime._load_bindings(attempt.request, command.envelope['payload'], command.envelope)[0]
                    writer_id = binding.writer_reference_id
                service = real_runtime.ingress_service(manifest, sessions, policy=policy, secrets=provider)
                adapter = service.load_factory(facts).adapter
                original_connect = http.client.HTTPConnection.connect
                def revoked_connect(connection):
                    original_connect(connection)
                    with sessions.begin() as db:
                        if store['revoke'] == 'role':
                            db.get(Membership, ('project', 'operator')).roles = []
                        else:
                            from datetime import datetime, timezone
                            db.get(SecretReference, writer_id).revoked_at = datetime.now(timezone.utc)
                # No authority function is patched: these run the production
                # resolver plus production post-connect _authorized transaction.
                with patch('app.real_resources.DorisObjects', side_effect=objects.factory):
                    for mode in ('role', 'reference'):
                        store['revoke'] = mode
                        before = len(puts)
                        with patch('app.doris_transport.http.client.HTTPConnection.connect', revoked_connect):
                            try:
                                adapter.send(binding, 'etl_' + 'd' * 64, b'{"id":1}\n')
                            except OSError as error:
                                assert str(error) == 'doris_stream_transport_unknown'
                            else:
                                raise AssertionError('post-connect revoked authority sent PUT')
                        assert len(puts) == before
                        with sessions.begin() as db:
                            if mode == 'role':
                                member = db.scalar(select(Membership).where(Membership.account_id == 'operator',
                                                                            Membership.project_id == 'project'))
                                member.roles = ['operator']
                            else:
                                db.get(SecretReference, writer_id).revoked_at = None
                    assert adapter.send(binding, 'etl_' + 'e' * 64, b'{"id":1}\n').raw == b'{"Status":"Success"}'
                    assert len(puts) == 1 and locked == [True]
        print(json.dumps({'postgres_guard_passed': [
            'production guard denies committed role revocation during TCP connect with zero PUTs',
            'production guard denies committed writer revocation during TCP connect with zero PUTs',
            'production tenant row lock remains held through original HTTP receipt'],
            'network_verified': False, 'real_l2_claim': False}))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


if __name__ == '__main__':
    import sys
    main()
    if '--postgres' in sys.argv:
        postgres_guard()
