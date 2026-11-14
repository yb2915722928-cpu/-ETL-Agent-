"""Frozen real-ingress resource callbacks; no network qualification or write grant.

The deployment owner supplies a trusted manifest. Its receipt hash is a binding,
not evidence this module measured network exclusivity. Old attempts lacking the
versioned route description and signed transport contract are denied. Both
routes remain bound to their approved schemas and freshly inspected identities.
"""
from dataclasses import asdict, is_dataclass
import hashlib
import ipaddress
import json
import os
import re

from sqlalchemy import select

from .command_signing import COMMAND_FORMAT, signing_bytes, verify_envelope
from .connector_policy import ResourcePolicy, fingerprint
from .db import Session
from .doris_objects import DorisObjects, object_marker, table_schema
from .engine_adapter import job_name
from .evidence import lock_tenant
from .execution import ExecutionRun, ManagedCommand, authorization_bindings
from .execution_runtime import EngineAttempt
from .managed_load import LoadRun
from .managed_secrets import SecretProvider
from .preparations import get_preparation, require_current_role, validate_preparation
from .quality_runtime import QualityRun
from .real_contract import REAL_EXECUTION_VERSION, real_execution_contract
from .real_engine import shadow_table
from .run_control import check_bound_secrets
from .versions import get_version
from .writer_credentials import resolve_execution_writer


def _sha(value):
    return hashlib.sha256(signing_bytes(value)).hexdigest()


def _require(condition, code):
    if not condition:
        raise ValueError(code)


def _digest(value):
    return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


def _schema_contract(columns):
    schema = table_schema(columns)
    for column in schema:
        # Integer display widths are metadata decoration, never a cast. Keep
        # the original observed schema in the object identity and its digest.
        match = re.fullmatch(r'(tinyint|smallint|int|integer|bigint)(?:\(\d+\))?', column['type'])
        if match:
            column['type'] = 'int' if match[1] == 'integer' else match[1]
    return schema


class RealResourceResolver:
    def __init__(self, *, sessions=Session, policy=None, secrets=None, manifest=None):
        self.sessions = sessions
        self.policy = policy if policy is not None else ResourcePolicy()
        self.secrets = secrets if secrets is not None else SecretProvider()
        # No global key, secret, caller token or live connection is cached.
        self.manifest = manifest

    def _authorized(self, db, command_id):
        command = db.get(ManagedCommand, command_id)
        _require(command is not None, 'real_command_missing')
        lock_tenant(db, command.tenant_id)
        db.refresh(command)
        run = db.get(ExecutionRun, command.run_id)
        _require(run is not None and (run.tenant_id, run.project_id) ==
                 (command.tenant_id, command.project_id), 'real_run_scope_mismatch')
        prep = get_preparation(db, run.tenant_id, run.project_id, run.preparation_id, lock=True)
        payload = verify_envelope(command.envelope, os.environ['COMMAND_PUBLIC_KEY'], COMMAND_FORMAT)
        expected = {'command_id': command.id, 'run_id': run.id,
                    'accepted_at': run.accepted_at.isoformat(), **authorization_bindings(prep, run.subject)}
        _require(all(payload.get(key) == value for key, value in expected.items()), 'real_signed_binding_mismatch')
        _require(os.environ.get('EXECUTION_ADAPTER') == 'real' and prep.environment == 'local'
                 and prep.tool in ('pipeline.trial', 'pipeline.publish')
                 and payload['authorized_operation'].get('data_plane_mutation') is True,
                 'real_execution_not_authorized')
        facts = prep.frozen['execution_facts']
        contract = payload.get('real_execution_contract')
        try:
            derived = real_execution_contract(prep.frozen['plan'], facts['source']['schema'], facts['target']['schema'])
            facts_contract = real_execution_contract(facts['plan'], facts['source']['schema'], facts['target']['schema'])
        except (KeyError, TypeError, ValueError):
            raise ValueError('real_execution_contract_mismatch') from None
        _require(isinstance(contract, dict) and contract == derived == facts_contract
                 and contract == facts.get('real_execution_contract')
                 and contract.get('version') == REAL_EXECUTION_VERSION
                 and contract.get('quality_stage_version') == 'quality-v1',
                 'real_execution_contract_mismatch')
        version = get_version(db, run.tenant_id, run.project_id, prep.version_id)
        db.refresh(version)
        _require(version.frozen.get('execution_facts') == facts
                 and version.frozen.get('normalized_plan') == prep.frozen['plan'],
                 'real_version_frozen_mismatch')
        require_current_role(db, run.tenant_id, run.project_id, run.subject, 'operator')
        _require(validate_preparation(db, self.policy, prep, check_expiry=False)['valid'],
                 'real_preparation_invalid')
        check_bound_secrets(db, command)
        attempt = db.get(EngineAttempt, command.id)
        _require(attempt is not None and _sha(attempt.request) == attempt.request_sha256,
                 'real_attempt_hash_mismatch')
        request = attempt.request
        _require(request.get('adapter') == 'real' and request.get('command_id') == command.id
                 and request.get('run_id') == run.id and request.get('tenant_id') == run.tenant_id
                 and request.get('project_id') == run.project_id
                 and request.get('correlation_id') == attempt.correlation_id
                 and request.get('engine_job_id') == attempt.engine_job_id
                 and isinstance(attempt.engine_job_id, str) and bool(attempt.engine_job_id)
                 and request.get('engine_job_name') == job_name(command.id)
                 and all(request.get(key) == payload[key] for key in (
                     'tool', 'environment', 'bundle_sha256', 'artifact_hashes', 'budget',
                     'authorized_operation', 'runtime_supervision_contract', 'real_execution_contract')), 'real_attempt_binding_mismatch')
        manifest = self.manifest() if callable(self.manifest) else self.manifest
        _require(isinstance(manifest, dict) and manifest.get('format') == 'etl-real-network-manifest-v1'
                 and _sha(manifest) == request.get('real_manifest_sha256'), 'real_manifest_mismatch')
        target = payload['resources']['target']
        entry = manifest.get('resources', {}).get(target['resource_id'])
        _require(isinstance(entry, dict), 'real_manifest_resource_missing')
        resource, reference = resolve_execution_writer(db, self.policy, run.tenant_id, run.project_id, target)
        _require(entry.get('resource_fingerprint') == target['resource_fingerprint']
                 and entry.get('writer_resource_fingerprint') == fingerprint(resource)
                 and entry.get('physical_cluster_id') == resource['physical_cluster_id']
                 and entry.get('engine_url') == request.get('engine_url')
                 and entry.get('ingress_url') == request.get('ingress_url')
                 and isinstance(entry.get('engine_url'), str) and bool(entry['engine_url'])
                 and isinstance(entry.get('ingress_url'), str) and bool(entry['ingress_url'])
                 and _digest(entry.get('network_receipt_sha256')), 'real_manifest_resource_mismatch')
        backend = entry.get('backend_route')
        _require(isinstance(backend, dict) and set(backend) == {'host', 'port', 'backend_id'}
                 and isinstance(backend['host'], str) and type(backend['port']) is int
                 and 1 <= backend['port'] <= 65535 and isinstance(backend['backend_id'], str)
                 and bool(backend['backend_id']), 'real_backend_route_invalid')
        ipaddress.ip_address(backend['host'])
        routes = request.get('real_routes')
        _require(request.get('real_routes_version') == 'etl-real-routes-v1'
                 and isinstance(routes, dict) and set(routes) == {'valid', 'rejected'}, 'real_routes_missing')
        target_table = prep.frozen['plan']['target']['table']
        handles = []
        for name, route in routes.items():
            _require(isinstance(route, dict) and set(route) == {
                'command_id', 'object_identity', 'target_table', 'object_run_id', 'object_command_id',
                'writer_secret_reference_id', 'writer_secret_version', 'resource_fingerprint',
                'physical_cluster_id', 'backend_route'}, 'real_route_shape_invalid')
            handle = route['command_id']
            object_run = run.id + contract[name + '_object_run_suffix']
            identity = route['object_identity']
            _require(isinstance(handle, str) and 0 < len(handle) <= 36 and handle != command.id
                     and route['target_table'] == target_table and target_table in resource['tables']
                     and route['object_run_id'] == object_run and route['object_command_id'] == command.id
                     and route['writer_secret_reference_id'] == reference.id
                     and type(route['writer_secret_version']) is int
                     and route['writer_secret_version'] == reference.version
                     and route['resource_fingerprint'] == target['resource_fingerprint']
                     and route['physical_cluster_id'] == resource['physical_cluster_id']
                     and route['backend_route'] == backend, 'real_route_binding_mismatch')
            _require(isinstance(identity, dict) and set(identity) == {
                'physical_cluster_id', 'database', 'table', 'marker', 'partition_ids', 'schema', 'schema_sha256'}
                and identity['physical_cluster_id'] == resource['physical_cluster_id']
                and identity['database'] == resource['database']
                and identity['table'] == shadow_table(target_table, object_run)
                and identity['marker'] == object_marker(command.id)
                and isinstance(identity['partition_ids'], list) and bool(identity['partition_ids'])
                and all(isinstance(x, str) and x for x in identity['partition_ids'])
                and len(set(identity['partition_ids'])) == len(identity['partition_ids'])
                and isinstance(identity['schema'], list) and bool(identity['schema'])
                and identity['schema_sha256'] == fingerprint(identity['schema']), 'real_object_binding_mismatch')
            _require(all(isinstance(column, dict) and set(column) == {'name', 'type', 'nullable'}
                         and isinstance(column['name'], str) and isinstance(column['type'], str)
                         and type(column['nullable']) is bool for column in identity['schema']),
                     'real_object_schema_mismatch')
            _require(_schema_contract(identity['schema']) == _schema_contract(contract[name + '_schema']),
                     'real_object_schema_mismatch')
            handles.append(handle)
        _require(len(set(handles)) == 2, 'real_route_command_collision')
        quality = db.get(QualityRun, run.id)
        primary = contract['source_primary_key']
        _require(primary and all(column.get('nullable') is False
                 for column in payload['resources']['source']['schema']['columns'] if column['name'] in primary),
                 'real_source_primary_key_missing')
        quality_contract = {'plan': prep.frozen['plan'], 'primary_key_fields': primary,
            'route_commands': {name: route['command_id'] for name, route in routes.items()},
            'stage_version': contract['quality_stage_version'],
            'cleaning': contract['cleaning'], 'filters': contract['filters']}
        _require(quality is not None and quality.contract == quality_contract
                 and quality.contract_sha256 == _sha(quality_contract), 'real_quality_contract_mismatch')
        return command, run, payload, request, routes, resource, reference, quality

    def authorize(self, facts):
        facts = asdict(facts) if is_dataclass(facts) else facts
        _require(isinstance(facts, dict) and set(facts) == {'command_id', 'run_id', 'engine_job_id',
                 'route_commands', 'quality_contract_sha256', 'source_primary_key'}, 'real_ingress_shape_invalid')
        with self.sessions.begin() as db:
            command, run, _, request, routes, _, _, quality = self._authorized(db, facts['command_id'])
            expected = {'command_id': command.id, 'run_id': run.id, 'engine_job_id': request['engine_job_id'],
                'route_commands': {name: route['command_id'] for name, route in routes.items()},
                'quality_contract_sha256': quality.contract_sha256,
                'source_primary_key': quality.contract['primary_key_fields']}
            _require(signing_bytes(facts) == signing_bytes(expected), 'real_ingress_binding_mismatch')

    def resolve(self, binding):
        with self.sessions.begin() as db:
            load = db.get(LoadRun, binding.command_id)
            _require(load is not None and load.binding == json.loads(signing_bytes(asdict(binding)))
                     and load.binding_sha256 == _sha(load.binding), 'real_load_binding_mismatch')
            commands = list(db.scalars(select(ManagedCommand).where(ManagedCommand.run_id == binding.run_id)))
            _require(len(commands) == 1, 'real_main_command_missing')
            command, run, payload, request, routes, resource, reference, quality = self._authorized(db, commands[0].id)
            names = [name for name, route in routes.items() if route['command_id'] == binding.command_id]
            _require(len(names) == 1, 'real_route_command_missing')
            name = names[0]
            route, identity = routes[name], routes[name]['object_identity']
            _require(binding.run_id == run.id and binding.engine_job_id == request['engine_job_id']
                     and binding.command_sha256 == _sha(command.envelope)
                     and binding.contract_sha256 == _sha(payload['runtime_supervision_contract'])
                     and binding.quality_contract_sha256 == quality.contract_sha256
                     and binding.physical_cluster_id == identity['physical_cluster_id']
                     and binding.database == identity['database'] and binding.table == identity['table']
                     and binding.object_id == fingerprint(identity)
                     and binding.writer_reference_id == reference.id and type(binding.writer_version) is int
                     and binding.writer_version == reference.version
                     and tuple(binding.backend_ids) == (route['backend_route']['backend_id'],),
                     'real_load_route_mismatch')
            plaintext = self.secrets.open(reference)
            current = DorisObjects(resource, plaintext).inspect_shadow(route['target_table'],
                        route['object_run_id'], route['object_command_id'])
            _require(current == identity, 'real_object_identity_changed')
            return resource, plaintext, current, dict(route['backend_route'])
