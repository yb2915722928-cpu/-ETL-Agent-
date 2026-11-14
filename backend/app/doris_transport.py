"""One direct BE Stream Load attempt; identity/route provenance is an external gate.

resolve(binding) runs on EVERY send. It must reload the original frozen writer
reference/version (including revoked_at), verify the frozen deployment resource
fingerprint and scoped shadow identity, then decrypt only in memory. It returns
(writer_resource, plaintext_secret, DorisObjects identity, trusted BE route).
Routes are deployment-owned {host: IP literal, port: int, backend_id: str}; never
HTTP input or a server redirect. Trusted BE mapping and exclusive network access
remain deployment evidence, not guarantees established by this module.

object_id is fingerprint(the entire DorisObjects.inspect_shadow identity), which
includes physical cluster/database/table, creation marker, partitions and schema.
Only the journal may parse/persist the returned raw response with its whitelist.

before_send(binding), when supplied, returns a context manager that reauthorizes
AFTER TCP connect and holds authorization through the original bounded receipt.
The default null guard supports diagnostics and never establishes L2 authority.
"""
import base64
from contextlib import nullcontext
import http.client
import ipaddress
import json
import re
import time

from .connector_policy import fingerprint
from .managed_load import TransportReceipt


MAX_BODY_BYTES = 16 * 1024 * 1024
MAX_RECEIPT_BYTES = 65536
HTTP_BUDGET_SECONDS = 1.5


def object_identity_id(identity):
    """Hash the complete inspected identity, never just the reusable table name."""
    return fingerprint(identity)


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate_json_key')
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError('nonfinite_json')


def _validate(binding, label, body, resource, secret, identity, route):
    if (not isinstance(label, str) or not re.fullmatch(r'etl_[0-9a-f]{64}', label)
            or not isinstance(body, bytes) or not 0 < len(body) <= MAX_BODY_BYTES):
        raise ValueError('invalid_batch')
    # A batch consists exclusively of complete JSON object records, one per line.
    for line in body.decode('utf-8').splitlines():
        if not line.strip() or not isinstance(json.loads(line, object_pairs_hook=_pairs,
                                                       parse_constant=_invalid_constant), dict):
            raise ValueError('invalid_json_record')
    if (resource.get('kind') != 'doris' or resource.get('direction') != 'target'
            or resource.get('physical_cluster_id') != binding.physical_cluster_id
            or resource.get('database') != binding.database
            or not isinstance(resource.get('username'), str)
            or not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]{0,63}', resource['username'])
            or not isinstance(secret, str) or not secret):
        raise ValueError('invalid_writer_resource')
    for value in (binding.database, binding.table):
        if not isinstance(value, str) or not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]{0,127}', value):
            raise ValueError('invalid_object_name')
    if (not isinstance(identity, dict)
            or identity.get('physical_cluster_id') != binding.physical_cluster_id
            or identity.get('database') != binding.database or identity.get('table') != binding.table
            # Route journals have their own command IDs. The full frozen object
            # digest binds the original creating command's marker; the resolver
            # independently checks that creator against the signed main command.
            or not isinstance(identity.get('marker'), str)
            or not re.fullmatch(r'etl_agent_object:[a-zA-Z0-9-]{1,64}', identity['marker'])
            or not identity.get('partition_ids') or not identity.get('schema')
            or identity.get('schema_sha256') != fingerprint(identity['schema'])
            or object_identity_id(identity) != binding.object_id):
        raise ValueError('object_identity_drift')
    if (not isinstance(route, dict) or set(route) != {'host', 'port', 'backend_id'}
            or not isinstance(route['backend_id'], str) or route['backend_id'] not in binding.backend_ids
            or type(route['port']) is not int or not 1 <= route['port'] <= 65535
            or not isinstance(route['host'], str)):
        raise ValueError('untrusted_route')
    # Avoid unbounded DNS lookups; the trusted resolver supplies a fixed BE IP.
    ipaddress.ip_address(route['host'])


class DorisStreamTransport:
    def __init__(self, resolve, *, before_send=None):
        self._resolve = resolve
        self._before_send = before_send

    def send(self, binding, label, body):
        connection = None
        secret = None
        headers = None
        try:
            resource, secret, identity, route = self._resolve(binding)
            _validate(binding, label, body, resource, secret, identity, route)
            auth = base64.b64encode((resource['username'] + ':' + secret).encode('utf-8')).decode('ascii')
            headers = {'Authorization': 'Basic ' + auth, 'Content-Type': 'application/json',
                       'format': 'json', 'read_json_by_line': 'true', 'strip_outer_array': 'false',
                       'strict_mode': 'true', 'max_filter_ratio': '0', 'num_as_string': 'true',
                       'label': label, 'two_phase_commit': 'false', 'Connection': 'close'}
            deadline = time.monotonic() + HTTP_BUDGET_SECONDS
            connection = http.client.HTTPConnection(route['host'], route['port'], timeout=HTTP_BUDGET_SECONDS)
            connection.connect()
            connection_socket = connection.sock
            def remaining():
                budget = deadline - time.monotonic()
                if budget <= 0:
                    raise TimeoutError()
                connection_socket.settimeout(budget)
            remaining()
            # The production guard reauthorizes after TCP connect and holds its
            # tenant transaction through this one request and original receipt.
            # Entry, body and exit all consume the same transport deadline.
            guard = self._before_send(binding) if self._before_send is not None else nullcontext()
            with guard:
                remaining()
                # No redirect handling or retry middleware exists here.
                connection.request('PUT', '/api/' + binding.database + '/' + binding.table + '/_stream_load',
                                   body=body, headers=headers)
                remaining()
                response = connection.getresponse()
                if not 200 <= response.status < 300:
                    raise OSError('http_unknown')
                # EOF alone cannot prove a complete receipt. Require explicit
                # length or a completed chunked stream; reject ambiguous framing.
                if ((response.length is None and not response.chunked)
                        or (response.chunked and response.getheader('Content-Length') is not None)):
                    raise OSError('response_unknown')
                chunks, size = [], 0
                while response.length != 0:
                    remaining()
                    chunk = response.read1(min(8192, MAX_RECEIPT_BYTES + 1 - size))
                    if not chunk:
                        if response.length not in (None, 0):
                            raise OSError('response_unknown')
                        break
                    chunks.append(chunk)
                    size += len(chunk)
                    if size > MAX_RECEIPT_BYTES:
                        raise OSError('response_unknown')
                raw = b''.join(chunks)
                if time.monotonic() >= deadline:
                    raise OSError('response_unknown')
            if time.monotonic() >= deadline:
                raise OSError('response_unknown')
            return TransportReceipt(raw, binding.physical_cluster_id, binding.database, binding.table,
                                    binding.object_id, route['backend_id'])
        except Exception:
            # Never propagate resolver, HTTP status/reason, server body or credentials.
            # Journal marks this attempt unknown permanently and must never resend it.
            raise OSError('doris_stream_transport_unknown') from None
        finally:
            if connection is not None:
                connection.close()
            secret = headers = None
