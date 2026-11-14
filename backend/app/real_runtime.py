"""Durable, deployment-gated real submission and internal ingress construction.

Only a prepared attempt can reserve its single SeaTunnel POST. A committed
submission reservation, or a committed object CREATE intent, is never retried.
Deployment manifests are trusted files, not caller assertions of qualification.
This module grants neither publication, cleanup, nor ownership release.
"""
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import hmac
import ipaddress
import json
import os
from pathlib import Path
import secrets as random_secrets
from urllib.parse import urlsplit

from sqlalchemy import DateTime, ForeignKey, JSON, String, func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Mapped, mapped_column

from .command_signing import COMMAND_FORMAT, signing_bytes, verify_envelope
from .connector_policy import ResourcePolicy, fingerprint
from .db import Base, Session
from .doris_objects import DorisObjects, object_marker
from .doris_transport import DorisStreamTransport
from .engine_adapter import JOB_STATES, SeaTunnelClient, job_name
from .evidence import lock_tenant
from .execution import ExecutionRun, ManagedCommand, authorization_bindings
from .execution_runtime import EngineAttempt, TargetOwnership, record
from .managed_load import FrozenLoadBinding, LoadRun, ManagedLoad
from .managed_secrets import SecretProvider
from .models import Account, SecretReference, new_id
from .preparations import (check_execution_resources, get_preparation,
                           require_current_role, validate_preparation)
from .quality_runtime import QualityRun, QualityRuntime
from .real_contract import real_execution_contract
from .real_engine import real_runtime_pin, shadow_table, verify_real_runtime
from .real_ingress import IngressBinding, IngressBindingFacts, IngressService
from .real_job import HTTP_CONNECTOR, ingress_job_request
from .real_resources import RealResourceResolver, _schema_contract
from .run_control import control_row, effects_for
from .supervision_runtime import RuntimeSupervision
from .versions import get_version
from .writer_credentials import check_execution_writer, resolve_execution_writer


_ORIGINAL_FIELDS = ('subject', 'preparation_id', 'frozen_sha256', 'version_id', 'tool', 'environment',
    'risk_level', 'execution_sha256', 'bundle_sha256', 'artifact_hashes', 'resources',
    'authorized_operation', 'budget', 'runtime_supervision_contract', 'real_execution_contract',
    'policy_version', 'compiler_versions', 'accepted_at')


class RealObjectIntent(Base):
    __tablename__ = 'real_object_intents'
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_commands.id'), primary_key=True)
    route: Mapped[str] = mapped_column(String(16), primary_key=True)
    specification: Mapped[dict] = mapped_column(JSON)
    specification_sha256: Mapped[str] = mapped_column(String(64))
    phase: Mapped[str] = mapped_column(String(24))
    identity: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


def _sha(value):
    return hashlib.sha256(signing_bytes(value)).hexdigest()


def _require(condition, code):
    if not condition:
        raise ValueError(code)


@contextmanager
def _command_lock(sessions, command_id):
    # Same advisory namespace as execution_runtime.command_lock, but use the
    # injected database (including its isolated fixture schema).
    with sessions() as db, db.get_bind().connect() as connection:
        key = int.from_bytes(hashlib.sha256(command_id.encode()).digest()[:8], 'big', signed=True)
        acquired = connection.scalar(text('SELECT pg_try_advisory_lock(:key)'), {'key': key})
        connection.commit()
        try:
            yield acquired
        finally:
            if acquired:
                connection.execute(text('SELECT pg_advisory_unlock(:key)'), {'key': key})
                connection.commit()


def _manifest(manifest):
    if manifest is not None:
        value = manifest() if callable(manifest) else manifest
    else:
        path = os.environ.get('REAL_DEPLOYMENT_MANIFEST_FILE')
        _require(bool(path), 'deployment_manifest_missing')
        raw = Path(path).read_bytes()
        _require(0 < len(raw) <= 1048576, 'deployment_manifest_invalid')
        value = json.loads(raw)
    _require(isinstance(value, dict) and value.get('format') == 'etl-real-network-manifest-v1',
             'deployment_manifest_invalid')
    # Detach from caller mutation. No passwords or tokens belong in this input.
    return json.loads(signing_bytes(value))


def _token(command_id):
    key = os.environ.get('REAL_INGRESS_HMAC_KEY', '')
    _require(len(key.encode()) >= 32, 'ingress_hmac_key_missing')
    return hmac.new(key.encode(), ('etl-real-ingress-v1\0' + command_id).encode(), hashlib.sha256).hexdigest()


def _url(value):
    _require(isinstance(value, str), 'deployment_endpoint_invalid')
    parsed = urlsplit(value)
    _require(parsed.scheme == 'http' and bool(parsed.hostname) and parsed.username is None
             and parsed.password is None and parsed.path in ('', '/')
             and not parsed.query and not parsed.fragment, 'deployment_endpoint_invalid')
    return value.rstrip('/')


def _network_verified(entry, raw):
    # Default gate: the frozen receipt must be bound to this manifest entry and
    # corroborated by a live observation written by the trusted host observer.
    # No live witness means the route stays unqualified, never assumed safe.
    from .real_network import verify_live_network
    return verify_live_network(entry, raw)


def _gate(db, payload, supervision, manifest, network_verifier):
    _require(supervision is not None and supervision.contract == payload['runtime_supervision_contract']
             and supervision.contract.get('observation_seconds') == 2
             and supervision.contract.get('stale_after_seconds') == 6,
             'runtime_supervision_unavailable')
    target = payload['resources']['target']
    entry = manifest.get('resources', {}).get(target['resource_id'])
    _require(isinstance(entry, dict) and entry.get('resource_fingerprint') == target['resource_fingerprint'],
             'deployment_resource_mismatch')
    _url(entry.get('engine_url'))
    _url(entry.get('ingress_url'))
    backend = entry.get('backend_route')
    _require(isinstance(backend, dict) and set(backend) == {'host', 'port', 'backend_id'}
             and type(backend['port']) is int and 1 <= backend['port'] <= 65535
             and isinstance(backend['backend_id'], str) and bool(backend['backend_id']),
             'deployment_backend_invalid')
    ipaddress.ip_address(backend['host'])
    _require(manifest.get('runtime_pin') == real_runtime_pin(), 'runtime_pin_mismatch')
    verified, _ = verify_real_runtime(manifest.get('runtime_overview'),
        manifest.get('connector_digests', {}), manifest.get('driver_digest'))
    _require(verified and manifest.get('http_connector_sha256') == HTTP_CONNECTOR['sha256'],
             'runtime_artifacts_unverified')
    path = os.environ.get('REAL_NETWORK_RECEIPT_FILE')
    _require(bool(path), 'network_receipt_missing')
    raw = Path(path).read_bytes()
    _require(0 < len(raw) <= 1048576 and hashlib.sha256(raw).hexdigest() == entry.get('network_receipt_sha256'),
             'network_receipt_digest_mismatch')
    receipt = json.loads(raw)
    _require(isinstance(receipt, dict) and receipt.get('schema') == 'managed-network-receipt/v1'
             and receipt.get('resource_state') == 'kept'
             and receipt.get('verification_phase') == 'isolated_network_observed', 'network_receipt_not_live')
    engine, gateway = receipt.get('engine', {}), receipt.get('gateway', {})
    _require(engine.get('file_encoding') == 'UTF-8', 'engine_text_encoding_unqualified')
    _require(_url(entry['engine_url']) == 'http://' + str(engine.get('ip')) + ':' + str(engine.get('rest_port'))
             and _url(entry['ingress_url']) == 'http://' + str(gateway.get('ip')) + ':' + str(gateway.get('ingress_port'))
             and receipt.get('http_connector_sha256') == HTTP_CONNECTOR['sha256'], 'network_receipt_endpoint_mismatch')
    # The separate verifier must inspect the current network/container IDs,
    # IPs, namespace, firewall and capability pins; historical observations
    # and a caller-supplied success field cannot satisfy this default gate.
    verifier = network_verifier if network_verifier is not None else _network_verified
    _require(verifier(entry, raw) is True, 'network_route_unqualified')
    _token(payload['command_id'])
    # Instantiate the real local supervision/stop/byte-journal parts and prove
    # their tables exist before an owner or external object can be created.
    for model in (LoadRun, QualityRun, IngressBinding):
        db.execute(select(model).limit(0))
    from .real_control import advance_real_control
    _require(callable(advance_real_control), 'real_stop_unavailable')
    return entry


def _authorized(db, command_id, policy, secrets):
    command = db.get(ManagedCommand, command_id)
    _require(command is not None, 'real_command_missing')
    lock_tenant(db, command.tenant_id)
    db.refresh(command)
    run = db.get(ExecutionRun, command.run_id)
    _require(run is not None and (run.tenant_id, run.project_id) ==
             (command.tenant_id, command.project_id), 'real_run_scope_mismatch')
    prep = get_preparation(db, run.tenant_id, run.project_id, run.preparation_id, lock=True)
    payload = verify_envelope(command.envelope, os.environ['COMMAND_PUBLIC_KEY'], COMMAND_FORMAT)
    expected = {'command_id': command.id, 'run_id': run.id, 'accepted_at': run.accepted_at.isoformat(),
                **authorization_bindings(prep, run.subject)}
    _require(all(payload.get(key) == value for key, value in expected.items()), 'real_signed_binding_mismatch')
    _require(os.environ.get('EXECUTION_ADAPTER') == 'real' and prep.environment == 'local'
             and prep.tool in ('pipeline.trial', 'pipeline.publish')
             and payload['authorized_operation'].get('data_plane_mutation') is True,
             'real_execution_not_authorized')
    facts = prep.frozen['execution_facts']
    contract = real_execution_contract(prep.frozen['plan'], facts['source']['schema'], facts['target']['schema'])
    _require(contract == payload.get('real_execution_contract') == facts.get('real_execution_contract')
             and contract == real_execution_contract(facts['plan'], facts['source']['schema'], facts['target']['schema']),
             'real_execution_contract_mismatch')
    version = get_version(db, run.tenant_id, run.project_id, prep.version_id)
    db.refresh(version)
    _require(version.frozen.get('execution_facts') == facts
             and version.frozen.get('normalized_plan') == prep.frozen['plan'], 'real_version_frozen_mismatch')
    require_current_role(db, run.tenant_id, run.project_id, run.subject, 'operator')
    _require(validate_preparation(db, policy, prep, check_expiry=False)['valid'], 'real_preparation_invalid')
    assets = list(payload['resources'].values())
    assets += [asset['execution_writer'] for asset in assets if asset.get('execution_writer')]
    for asset in assets:
        reference = db.get(SecretReference, asset.get('secret_reference_id'), populate_existing=True)
        _require(reference is not None and reference.revoked_at is None
                 and (reference.tenant_id, reference.project_id, reference.version) ==
                 (run.tenant_id, run.project_id, asset.get('secret_version')), 'real_frozen_secret_invalid')
        secrets.open(reference)
    resource, reference = resolve_execution_writer(db, policy, run.tenant_id, run.project_id, facts['target'])
    return command, run, prep, payload, resource, reference


def _external_authorization(db, policy, secrets, version, run, prep):
    actor = db.get(Account, run.subject)
    check_execution_resources(db, policy, secrets, version, actor, 'operator', preparation_id=prep.id)
    check_execution_writer(db, policy, secrets, version, actor, 'operator', preparation_id=prep.id)


def _cancel(db, command, run):
    control = control_row(db, command.id)
    control.facts = {**control.facts, 'termination': {'confirmed': True, 'basis': 'cancelled_before_submission',
        'confirmed_at': datetime.now(timezone.utc).isoformat(), 'terminated_at': None}}
    control.wake_at = None
    for effect in effects_for(db, command.id):
        effect.phase, effect.result = 'confirmed', {'cancelled_before_submission': True}
    run.status = 'cancelled'
    record(db, run, 'control.cancelled_before_submission', {'command_id': command.id})
    return 'cancelled'


def _stopped(db, command_id):
    supervision = db.get(RuntimeSupervision, command_id)
    return supervision is not None and bool(supervision.stop_request)


def _identity_valid(identity, specification):
    _require(isinstance(identity, dict) and set(identity) == {'physical_cluster_id', 'database', 'table',
             'marker', 'partition_ids', 'schema', 'schema_sha256'}, 'real_object_identity_invalid')
    _require(all(identity.get(key) == specification[key] for key in ('physical_cluster_id', 'database', 'table', 'marker'))
             and isinstance(identity['partition_ids'], list) and bool(identity['partition_ids'])
             and all(isinstance(value, str) and value for value in identity['partition_ids'])
             and len(set(identity['partition_ids'])) == len(identity['partition_ids'])
             and isinstance(identity['schema'], list) and bool(identity['schema'])
             and all(isinstance(column, dict) and set(column) == {'name', 'type', 'nullable'}
                     and isinstance(column['name'], str) and isinstance(column['type'], str)
                     and type(column['nullable']) is bool for column in identity['schema'])
             and identity['schema_sha256'] == fingerprint(identity['schema'])
             and _schema_contract(identity['schema']) == _schema_contract(specification['schema']),
             'real_object_identity_invalid')


def _object(sessions, command_id, route, policy, secrets, manifest, objects_factory, network_verifier=None):
    # Return True only for the delivery that durably created this intent.
    with sessions.begin() as db:
        command, run, prep, payload, resource, reference = _authorized(db, command_id, policy, secrets)
        if _stopped(db, command_id):
            return False
        contract = payload['real_execution_contract']
        target = prep.frozen['plan']['target']['table']
        object_run = run.id + contract[route + '_object_run_suffix']
        specification = {'target_table': target, 'object_run_id': object_run, 'object_command_id': command_id,
            'physical_cluster_id': resource['physical_cluster_id'], 'database': resource['database'],
            'table': shadow_table(target, object_run), 'marker': object_marker(command_id),
            'schema': contract[route + '_schema']}
        intent = db.get(RealObjectIntent, (command_id, route))
        create = intent is None
        if create:
            now = datetime.now(timezone.utc)
            db.add(RealObjectIntent(command_id=command_id, route=route, specification=specification,
                specification_sha256=_sha(specification), phase='creating', created_at=now, updated_at=now))
        else:
            _require(intent.specification == specification and intent.specification_sha256 == _sha(specification),
                     'real_object_intent_changed')
    # CREATE reservation is committed before any external request. Death here
    # loses permission to CREATE; a later worker can only inspect, even if absent.
    with sessions.begin() as db:
        command, run, prep, payload, resource, reference = _authorized(db, command_id, policy, secrets)
        if _stopped(db, command_id):
            return False
        version = get_version(db, run.tenant_id, run.project_id, prep.version_id)
        _external_authorization(db, policy, secrets, version, run, prep)
        intent = db.get(RealObjectIntent, (command_id, route))
        entry = manifest['resources'][payload['resources']['target']['resource_id']]
        _require(entry['physical_cluster_id'] == resource['physical_cluster_id']
                 and entry['writer_resource_fingerprint'] == fingerprint(resource), 'deployment_writer_mismatch')
        objects = objects_factory(resource, secrets.open(reference))
        try:
            if create:
                identity = objects.create_shadow(target, object_run, command_id, specification['schema'],
                    before_mutation=lambda: _gate(db, payload, db.get(RuntimeSupervision, command_id),
                                                   manifest, network_verifier))
            else:
                identity = objects.inspect_shadow(target, object_run, command_id)
            _identity_valid(identity, specification)
            _require(intent.identity is None or intent.identity == identity, 'real_object_identity_changed')
            intent.identity, intent.phase, intent.reason = identity, 'ready', None
        except Exception:
            intent.phase, intent.reason = 'unknown', 'object_create_or_inspection_unknown'
            run.status = 'submission_unknown'
            return False
        finally:
            intent.updated_at = datetime.now(timezone.utc)
    return True


def _request(db, command, run, prep, payload, resource, reference, manifest):
    routes = {}
    target = payload['resources']['target']
    entry = manifest['resources'][target['resource_id']]
    for name in ('valid', 'rejected'):
        intent = db.get(RealObjectIntent, (command.id, name))
        _require(intent is not None and intent.phase == 'ready' and intent.identity is not None,
                 'real_object_not_ready')
        spec = intent.specification
        _identity_valid(intent.identity, spec)
        routes[name] = {'command_id': new_id(), 'object_identity': intent.identity,
            'target_table': spec['target_table'], 'object_run_id': spec['object_run_id'],
            'object_command_id': command.id, 'writer_secret_reference_id': reference.id,
            'writer_secret_version': reference.version, 'resource_fingerprint': target['resource_fingerprint'],
            'physical_cluster_id': resource['physical_cluster_id'], 'backend_route': entry['backend_route']}
    request = {'adapter': 'real', 'command_id': command.id, 'run_id': run.id, 'correlation_id': new_id(),
        'tenant_id': run.tenant_id, 'project_id': run.project_id, 'engine_job_id': str(random_secrets.randbelow(2 ** 63 - 1) + 1),
        'engine_job_name': job_name(command.id), 'engine_url': entry['engine_url'], 'ingress_url': entry['ingress_url'],
        'real_routes_version': 'etl-real-routes-v1', 'real_routes': routes, 'real_manifest_sha256': _sha(manifest)}
    for key in _ORIGINAL_FIELDS:
        request[key] = payload[key]
    return request


def _original(command, run, attempt):
    payload = verify_envelope(command.envelope, os.environ['COMMAND_PUBLIC_KEY'], COMMAND_FORMAT)
    request = attempt.request
    _require(_sha(request) == attempt.request_sha256 and request.get('adapter') == 'real'
             and request.get('command_id') == command.id == payload['command_id']
             and request.get('run_id') == run.id == payload['run_id']
             and request.get('tenant_id') == run.tenant_id == payload['tenant_id']
             and request.get('project_id') == run.project_id == payload['project_id']
             and request.get('correlation_id') == attempt.correlation_id
             and request.get('engine_job_id') == attempt.engine_job_id
             and request.get('engine_job_name') == job_name(command.id)
             and all(request.get(key) == payload.get(key) for key in _ORIGINAL_FIELDS),
             'real_original_attempt_mismatch')
    SeaTunnelClient.validate_job_id(attempt.engine_job_id)
    return payload, request


def _load_bindings(request, payload, envelope):
    routes = request['real_routes']
    # The exact original plan lives in QualityRun; its hash is attached by bind.
    result = []
    for name in ('valid', 'rejected'):
        route, identity = routes[name], routes[name]['object_identity']
        result.append(FrozenLoadBinding(command_id=route['command_id'], run_id=request['run_id'],
            engine_job_id=request['engine_job_id'], command_sha256=_sha(envelope),
            contract_sha256=_sha(payload['runtime_supervision_contract']), physical_cluster_id=identity['physical_cluster_id'],
            database=identity['database'], table=identity['table'], object_id=fingerprint(identity),
            writer_reference_id=route['writer_secret_reference_id'], writer_version=route['writer_secret_version'],
            backend_ids=(route['backend_route']['backend_id'],), quality_contract_sha256=request['quality_contract_sha256']))
    return result


def ingress_service(manifest=None, sessions=Session, *, policy=None, secrets=None):
    """Internal gateway/controller factory; observe/close require no new authority.

    POST delegates current authorization to RealResourceResolver on every request.
    The load factory only checks immutable local originals, so role revocation
    cannot prevent closing admission or observing an already reserved write.
    """
    # Gateway startup and stop/observe must also work when a deployment file
    # has disappeared. Only POST/resolve consumes current deployment authority.
    deployment = lambda: _manifest(manifest)
    resolver = RealResourceResolver(sessions=sessions, policy=policy, secrets=secrets, manifest=deployment)

    @contextmanager
    def before_send(binding):
        # Resolve has released its transaction before TCP connect. Reauthorize
        # under a fresh tenant lock, held until the single bounded PUT finishes.
        # Do not call resolver.resolve here: it opens another tenant transaction.
        with sessions.begin() as db:
            commands = list(db.scalars(select(ManagedCommand).where(ManagedCommand.run_id == binding.run_id)))
            _require(len(commands) == 1, 'real_main_command_missing')
            command, run, _, payload, writer, reference = _authorized(
                db, commands[0].id, resolver.policy, resolver.secrets)
            attempt = db.get(EngineAttempt, command.id)
            _require(attempt is not None, 'real_attempt_missing')
            _, request = _original(command, run, attempt)
            _require(binding in _load_bindings(request, payload, command.envelope), 'real_load_route_mismatch')
            frozen_writer = payload['resources']['target']['execution_writer']
            _require(reference.id == binding.writer_reference_id and reference.version == binding.writer_version
                     and fingerprint(writer) == frozen_writer['resource_fingerprint']
                     and writer['username'] == frozen_writer['username'], 'real_writer_binding_mismatch')
            current_manifest = deployment()
            _require(request.get('real_manifest_sha256') == _sha(current_manifest), 'deployment_manifest_changed')
            _gate(db, payload, db.get(RuntimeSupervision, command.id), current_manifest, None)
            yield

    loads = ManagedLoad(DorisStreamTransport(resolver.resolve, before_send=before_send), sessions=sessions)

    def factory(facts):
        with sessions() as db:
            command = db.get(ManagedCommand, facts['command_id'])
            _require(command is not None, 'real_command_missing')
            run, attempt = db.get(ExecutionRun, command.run_id), db.get(EngineAttempt, command.id)
            _require(attempt is not None, 'real_attempt_missing')
            payload, request = _original(command, run, attempt)
            expected = {'command_id': command.id, 'run_id': run.id, 'engine_job_id': attempt.engine_job_id,
                'route_commands': {name: route['command_id'] for name, route in request['real_routes'].items()},
                'quality_contract_sha256': request['quality_contract_sha256'],
                'source_primary_key': payload['real_execution_contract']['source_primary_key']}
            _require(signing_bytes(expected) == signing_bytes(facts), 'real_ingress_binding_mismatch')
            for binding in _load_bindings(request, payload, command.envelope):
                row = db.get(LoadRun, binding.command_id)
                expected_binding = json.loads(signing_bytes(asdict(binding)))
                _require(row is not None and row.binding == expected_binding
                         and row.binding_sha256 == _sha(expected_binding), 'real_load_binding_mismatch')
        return loads

    return IngressService(factory, resolver.authorize, sessions=sessions)


def _bind(sessions, command_id, policy, secrets, manifest):
    with sessions.begin() as db:
        command, run, prep, payload, _, _ = _authorized(db, command_id, policy, secrets)
        attempt = db.get(EngineAttempt, command_id)
        _require(attempt is not None and attempt.phase == 'prepared', 'real_attempt_not_prepared')
        _, request = _original(command, run, attempt)
        plan, primary = prep.frozen['plan'], payload['real_execution_contract']['source_primary_key']
        route_commands = {name: route['command_id'] for name, route in request['real_routes'].items()}
        run_id, envelope = run.id, command.envelope
    quality_sha = QualityRuntime(sessions).bind(run_id, plan, primary, route_commands,
        stage_version=payload['real_execution_contract']['quality_stage_version'],
        cleaning=payload['real_execution_contract']['cleaning'], filters=payload['real_execution_contract']['filters'])
    # Request is finalized while still PREPARED. The reservation below freezes
    # this hash before any POST; submitted/unknown attempts never mutate it.
    with sessions.begin() as db:
        attempt = db.get(EngineAttempt, command_id)
        _require(attempt.phase == 'prepared' and _sha(attempt.request) == attempt.request_sha256,
                 'real_attempt_not_prepared')
        request = {**attempt.request, 'quality_contract_sha256': quality_sha}
        attempt.request, attempt.request_sha256 = request, _sha(request)
    loads = ManagedLoad(DorisStreamTransport(RealResourceResolver(sessions=sessions, policy=policy,
        secrets=secrets, manifest=manifest).resolve), sessions=sessions)
    for binding in _load_bindings(request, payload, envelope):
        loads.bind(binding)
    service = ingress_service(manifest, sessions, policy=policy, secrets=secrets)
    service.bind(IngressBindingFacts(command_id, run_id, request['engine_job_id'], route_commands,
        quality_sha, tuple(primary)), hashlib.sha256(_token(command_id).encode()).hexdigest())
    return service


def _discover(sessions, command_id, client_factory):
    with sessions.begin() as db:
        command = db.get(ManagedCommand, command_id)
        lock_tenant(db, command.tenant_id)
        run, attempt = db.get(ExecutionRun, command.run_id), db.get(EngineAttempt, command_id)
        try:
            _, request = _original(command, run, attempt)
            _require(attempt.phase in ('submitting', 'submission_unknown', 'submitted'), 'real_attempt_phase_invalid')
            result = client_factory(request['engine_url']).observe(attempt.engine_job_id, request['engine_job_name'])
            _require(isinstance(result, dict) and result.get('job_id') == attempt.engine_job_id
                     and result.get('job_name') == request['engine_job_name']
                     and result.get('engine_state') in JOB_STATES - {'UNKNOWABLE'},
                     'real_engine_association_unknown')
            attempt.phase, attempt.reason = 'submitted', None
            # A control status is never replaced by a submission status.
            if run.status in ('prepared', 'submitting', 'submission_unknown'):
                run.status = 'submitted'
            run.engine_job_id = attempt.engine_job_id
        except Exception:
            attempt.phase, attempt.reason = 'submission_unknown', 'original_job_association_unknown'
            if run.status in ('prepared', 'submitting', 'submitted', 'submission_unknown'):
                run.status = 'submission_unknown'
        attempt.updated_at = datetime.now(timezone.utc)
        return run.status


def _client(url):
    return SeaTunnelClient(url, timeout=1.5)


def submit_real(command_id, *, sessions=Session, policy=None, secrets=None, manifest=None,
                client_factory=None, network_verifier=None, objects_factory=DorisObjects):
    """Prepare original bindings and submit once; caller must not take command lock.

    Explicit manifest/network-verifier injection is for isolated fixtures only.
    The default path requires deployment files and an actual measured proof.
    """
    policy = policy if policy is not None else ResourcePolicy()
    secrets = secrets if secrets is not None else SecretProvider()
    client_factory = client_factory if client_factory is not None else _client
    with _command_lock(sessions, command_id) as acquired:
        if not acquired:
            return 'busy'
        with sessions.begin() as db:
            command = db.get(ManagedCommand, command_id)
            if command is None:
                return 'missing'
            lock_tenant(db, command.tenant_id)
            run, attempt = db.get(ExecutionRun, command.run_id), db.get(EngineAttempt, command_id)
            if attempt is None and _stopped(db, command_id):
                return _cancel(db, command, run)
            if run.status in ('denied', 'blocked_target', 'cancelled', 'cleaned', 'supervision_unavailable',
                              'supervision_passed', 'published'):
                control_row(db, command_id).wake_at = None
                return run.status
            try:
                command, run, prep, payload, resource, reference = _authorized(db, command_id, policy, secrets)
            except Exception:
                if attempt is None:
                    run.status = 'denied'
                else:
                    try:
                        # Current write authority cannot erase an established
                        # original job. Control owns its revocation safety stop.
                        _original(command, run, attempt)
                        _require(attempt.phase in ('submitting', 'submission_unknown', 'submitted'),
                                 'real_attempt_phase_invalid')
                    except Exception:
                        run.status = 'submission_unknown'
                    else:
                        if run.status in ('prepared', 'submitting'):
                            run.status = 'submission_unknown'
                record(db, run, 'execution.real_denied', {'command_id': command_id, 'reason': 'original_authorization_invalid'})
                return run.status
            try:
                deployment = _manifest(manifest)
                entry = _gate(db, payload, db.get(RuntimeSupervision, command_id), deployment, network_verifier)
                _require(entry['physical_cluster_id'] == resource['physical_cluster_id']
                         and entry['writer_resource_fingerprint'] == fingerprint(resource), 'deployment_writer_mismatch')
                control_row(db, command_id)
                # Construction itself must succeed before owner creation.
                ingress_service(deployment, sessions, policy=policy, secrets=secrets)
            except Exception:
                run.status = 'submission_unknown' if attempt is not None else 'supervision_unavailable'
                control_row(db, command_id).wake_at = None
                record(db, run, 'execution.real_gate_denied', {'command_id': command_id,
                    'reason': 'deployment_supervision_unavailable', 'data_plane_mutation': False})
                return run.status
            recover = attempt is not None and attempt.phase != 'prepared'
            if attempt is not None:
                try:
                    _original(command, run, attempt)
                    _require(attempt.request.get('real_manifest_sha256') == _sha(deployment), 'deployment_manifest_changed')
                except Exception:
                    run.status = 'submission_unknown'
                    return run.status
            if attempt is None:
                identity = {'physical_cluster_id': resource['physical_cluster_id'], 'database': resource['database'],
                            'table': prep.frozen['plan']['target']['table']}
                target_key = _sha(identity)
                db.execute(insert(TargetOwnership).values(target_key=target_key, identity=identity,
                    command_id=command_id, acquired_at=datetime.now(timezone.utc))
                    .on_conflict_do_nothing(index_elements=['target_key']))
                owner = db.get(TargetOwnership, target_key)
                if owner.command_id != command_id:
                    run.status = 'blocked_target'
                    control_row(db, command_id).wake_at = None
                    return run.status
        if recover:
            return _discover(sessions, command_id, client_factory)
        try:
            if attempt is None:
                for route in ('valid', 'rejected'):
                    if not _object(sessions, command_id, route, policy, secrets, deployment, objects_factory, network_verifier):
                        return 'submission_unknown'
                with sessions.begin() as db:
                    command, run, prep, payload, resource, reference = _authorized(db, command_id, policy, secrets)
                    if _stopped(db, command_id):
                        run.status = 'cancelled'
                        return 'cancelled'
                    request = _request(db, command, run, prep, payload, resource, reference, deployment)
                    now = datetime.now(timezone.utc)
                    db.add(EngineAttempt(command_id=command_id, correlation_id=request['correlation_id'],
                        request=request, request_sha256=_sha(request), phase='prepared',
                        engine_job_id=request['engine_job_id'], created_at=now, updated_at=now))
                    run.status = 'prepared'
            service = _bind(sessions, command_id, policy, secrets, deployment)
            # Local bindings all committed before this irreversible reservation.
            with sessions.begin() as db:
                command, run, _, _, _, _ = _authorized(db, command_id, policy, secrets)
                if _stopped(db, command_id):
                    run.status = 'cancelled'
                    return 'cancelled'
                attempt = db.get(EngineAttempt, command_id)
                _require(attempt.phase == 'prepared', 'real_attempt_not_prepared')
                _original(command, run, attempt)
                attempt.phase, run.status = 'submitting', 'submitting'
                attempt.updated_at = db.scalar(select(func.clock_timestamp()))
                control = control_row(db, command_id)
                control.facts = {**control.facts, 'real_submission': {
                    'reserved_at': attempt.updated_at.isoformat()}}
                record(db, run, 'execution.real_submission_reserved', {'command_id': command_id,
                    'engine_job_id': attempt.engine_job_id, 'request_sha256': attempt.request_sha256})
            with sessions.begin() as db:
                command, run, prep, payload, _, _ = _authorized(db, command_id, policy, secrets)
                attempt = db.get(EngineAttempt, command_id)
                _, request = _original(command, run, attempt)
                if _stopped(db, command_id):
                    attempt.phase, run.status = 'denied', 'cancelled'
                    return 'cancelled'
                version = get_version(db, run.tenant_id, run.project_id, prep.version_id)
                _external_authorization(db, policy, secrets, version, run, prep)
                asset = payload['resources']['source']
                source = policy.get(asset['resource_id'])
                _require(source is not None and fingerprint(source) == asset['resource_fingerprint'],
                         'real_source_resource_changed')
                reference = db.get(SecretReference, asset['secret_reference_id'], populate_existing=True)
                job, primary = ingress_job_request(command_id, prep.frozen['execution_facts'], source,
                    secrets.open(reference), request['ingress_url'], _token(command_id))
                _require(list(primary) == payload['real_execution_contract']['source_primary_key'], 'real_source_primary_key_changed')
                try:
                    _gate(db, payload, db.get(RuntimeSupervision, command_id), deployment, network_verifier)
                    identifier = client_factory(request['engine_url']).submit(job, attempt.engine_job_id)
                    _require(str(identifier) == attempt.engine_job_id, 'real_submit_identity_mismatch')
                    attempt.phase, run.status, attempt.reason = 'submitted', 'submitted', None
                    run.engine_job_id = attempt.engine_job_id
                except Exception:
                    attempt.phase, run.status = 'submission_unknown', 'submission_unknown'
                    attempt.reason = 'single_submit_response_unknown'
                attempt.updated_at = datetime.now(timezone.utc)
                return run.status
        except Exception:
            # Never retain arbitrary exception strings: drivers may embed secrets.
            with sessions.begin() as db:
                command = db.get(ManagedCommand, command_id)
                lock_tenant(db, command.tenant_id)
                run, attempt = db.get(ExecutionRun, command.run_id), db.get(EngineAttempt, command_id)
                run.status = 'submission_unknown'
                if attempt is not None and attempt.phase != 'prepared':
                    attempt.phase, attempt.reason = 'submission_unknown', 'preparation_or_submission_interrupted'
                return run.status
