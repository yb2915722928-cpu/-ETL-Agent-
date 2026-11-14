"""Single-attempt cleanup of original, stopped, unpublished real-run shadows.

Caller owns the command advisory lock. Current signed control authority, original
frozen references, complete load journals, a live exclusive route and original
engine termination are independently required. A lease or cancellation ACK is
never a fence. DROP reservations commit before I/O; recovery only reads absence.
"""
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import json
import re

from sqlalchemy import DateTime, ForeignKey, JSON, String, select
from sqlalchemy.orm import Mapped, mapped_column

from .connector_policy import ResourcePolicy
from .connector_probe import identifier
from .db import Base, Session
from .doris_objects import DorisObjects
from .engine_adapter import SeaTunnelClient, job_name
from .evidence import lock_tenant
from .execution import ExecutionRun, ManagedCommand
from .execution_runtime import EngineAttempt, TargetOwnership, record
from .managed_load import LoadBatch, LoadRun
from .managed_secrets import SecretProvider
from .preparations import require_current_role
from .publication import PublicationIntent, PublishedSnapshot
from .real_ingress import IngressBinding
from .real_resources import RealResourceResolver
from .real_runtime import _gate, _load_bindings, _manifest, _original, _require, _sha, ingress_service
from .run_control import (ManagedControlEffect, RunControl, check_bound_secrets,
                          effects_for, verify_effect)
from .runtime_supervision import COUNTERS
from .supervision_runtime import RuntimeSupervision


class CleanupIntent(Base):
    __tablename__ = 'real_cleanup_intents'
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_commands.id'), primary_key=True)
    route: Mapped[str] = mapped_column(String(16), primary_key=True)
    effect_id: Mapped[str] = mapped_column(ForeignKey('managed_control_effects.id'))
    binding: Mapped[dict] = mapped_column(JSON)
    binding_sha256: Mapped[str] = mapped_column(String(64))
    phase: Mapped[str] = mapped_column(String(24))
    reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


def check_cleanup_grants(rows, database, lifecycle_privileges=()):
    """Only an explicitly configured same-database Drop_priv permits deletion."""
    configured = set(lifecycle_privileges)
    _require(configured <= {'Alter_priv', 'Drop_priv'} and 'Drop_priv' in configured,
             'cleanup_drop_not_deployment_approved')
    allowed = {'Select_priv', 'Load_priv', 'Create_priv'} | configured
    found = False
    for row in rows:
        _require(row.get('Roles') in (None, '', 'NULL'), 'cleanup_roles_unqualified')
        for key, value in row.items():
            if not key.endswith('Privs') or value in (None, '', 'NULL'):
                continue
            if key == 'WorkloadGroupPrivs' and value == 'normal: Usage_priv':
                continue
            _require(key == 'DatabasePrivs', 'cleanup_global_or_table_grants_unqualified')
            for grant in str(value).split(';'):
                scope, separator, rights = grant.strip().partition(':')
                privileges = set(re.findall(r'\b\w+_priv\b', rights))
                _require(bool(separator) and bool(privileges), 'cleanup_grant_shape_invalid')
                if scope in ('internal.information_schema', 'internal.mysql'):
                    _require(privileges == {'Select_priv'}, 'cleanup_catalog_grants_unqualified')
                else:
                    _require(scope == 'internal.' + database and privileges <= allowed,
                             'cleanup_cross_database_grants_unqualified')
                    found = found or 'Drop_priv' in privileges
    _require(found, 'cleanup_drop_privilege_missing')


class DorisCleanupAdapter:
    """Bounded same-database reads and one non-retrying DROP per reserved route."""
    def __init__(self, resource, credential):
        self.resource, self.objects = resource, DorisObjects(resource, credential)

    def privileges(self):
        return self.objects.publication_privileges()

    def inspect(self, route):
        expected = route['object_identity']
        _require(expected['database'] == self.resource['database']
                 and expected['physical_cluster_id'] == self.resource['physical_cluster_id']
                 and expected['table'] == self.objects._scope(route['target_table'], route['object_run_id'])
                 and expected['table'] not in self.resource['tables'], 'cleanup_shadow_scope_invalid')
        # An empty metadata result proves absence; an error does not.
        with self.objects._cursor() as cursor:
            cursor.execute('SELECT TABLE_NAME FROM information_schema.TABLES '
                           'WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',
                           (expected['database'], expected['table']))
            rows = cursor.fetchall()
        if not rows:
            return None
        _require(len(rows) == 1, 'cleanup_object_ambiguous')
        return self.objects.inspect_shadow(route['target_table'], route['object_run_id'], route['object_command_id'])

    def drop(self, route, *, before_mutation=None):
        check_cleanup_grants(self.privileges(), self.resource['database'],
                             self.resource.get('lifecycle_privileges', ()))
        _require(self.inspect(route) == route['object_identity'], 'cleanup_object_replaced')
        with self.objects._cursor() as cursor:
            # Grants, identity reads, connection setup and SET query_timeout may
            # consume the live-network witness. Check it inside the opened cursor,
            # immediately before the one destructive statement, under caller lock.
            statement = 'DROP TABLE ' + identifier(self.resource['database']) + '.' + identifier(route['object_identity']['table'])
            if before_mutation is not None:
                before_mutation()
            cursor.execute(statement)


def real_cleanup_binding(db, command, run, control):
    """Local public-route preflight; this binding is signed into the control effect.

    Live network, original engine GET and actual object checks occur again in the
    worker. This function never opens network connections or authorizes a DROP.
    """
    attempt = db.get(EngineAttempt, command.id)
    _require(attempt is not None, 'cleanup_original_attempt_missing')
    _, request = _original(command, run, attempt)
    termination, final = control.facts.get('termination', {}), control.facts.get('final_metrics', {})
    _require(termination.get('confirmed') is True and termination.get('basis') == 'engine_terminal'
             and termination.get('engine_job_id') == attempt.engine_job_id
             and termination.get('job_name') == job_name(command.id)
             and termination.get('engine_state') in ('FINISHED', 'CANCELED', 'FAILED'),
             'cleanup_original_terminal_missing')
    _require(final.get('status') == 'known' and isinstance(final.get('sample'), dict)
             and isinstance(final.get('metrics'), dict)
             and all(type(final['sample'].get(key)) in (int, float) and final['sample'][key] >= 0
                     for key in COUNTERS), 'cleanup_final_metrics_missing')
    owner = db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == command.id))
    _require(owner is not None, 'cleanup_owner_missing')
    expected_target = {'physical_cluster_id': request['real_routes']['valid']['object_identity']['physical_cluster_id'],
                       'database': request['real_routes']['valid']['object_identity']['database'],
                       'table': request['real_routes']['valid']['target_table']}
    _require(owner.identity == expected_target and owner.target_key == _sha(expected_target), 'cleanup_owner_changed')
    _require(db.scalar(select(PublicationIntent).where(PublicationIntent.run_id == run.id)) is None,
             'cleanup_publication_intent_exists')
    _require(db.scalar(select(PublishedSnapshot).where(PublishedSnapshot.run_id == run.id)) is None,
             'cleanup_published_snapshot_exists')
    routes = request.get('real_routes')
    _require(isinstance(routes, dict) and set(routes) == {'valid', 'rejected'}, 'cleanup_routes_missing')
    for route in routes.values():
        identity = route['object_identity']
        _require(identity['table'] != expected_target['table']
                 and re.fullmatch(r'shadow_[0-9a-f]{32}', identity['table']) is not None,
                 'cleanup_formal_table_forbidden')
        # Snapshots from any run remain protected, including retained old tables
        # after a swap. Partition identity survives renames.
        for snapshot in db.scalars(select(PublishedSnapshot)):
            if _contains_object(snapshot.identities, identity):
                raise ValueError('cleanup_retained_snapshot_forbidden')
    check_bound_secrets(db, command)
    return {'target_key': owner.target_key, 'target': owner.identity, 'engine_job_id': attempt.engine_job_id,
            'correlation_id': attempt.correlation_id, 'submission_sha256': attempt.request_sha256,
            'real_unpublished_objects': {name: route['object_identity'] for name, route in routes.items()}}


def _contains_object(value, expected):
    if isinstance(value, dict):
        if (value.get('physical_cluster_id') == expected['physical_cluster_id']
                and value.get('database') == expected['database']
                and (value.get('table') == expected['table'] or
                     set(value.get('partition_ids', ())) & set(expected['partition_ids']))):
            return True
        return any(_contains_object(child, expected) for child in value.values())
    return isinstance(value, list) and any(_contains_object(child, expected) for child in value)


def _current(db, command_id, effect_id, resolver, deployment, network_verifier):
    command, run, payload, request, routes, resource, reference, _ = resolver._authorized(db, command_id)
    control, supervision = db.get(RunControl, command_id), db.get(RuntimeSupervision, command_id)
    _require(control is not None and supervision is not None, 'cleanup_control_missing')
    binding = real_cleanup_binding(db, command, run, control)
    effect = db.get(ManagedControlEffect, effect_id)
    _require(effect is not None and effect.kind == 'cleanup' and effect.phase != 'denied', 'cleanup_effect_missing')
    verify_effect(effect, command, run)
    require_current_role(db, run.tenant_id, run.project_id, effect.actor_id, 'operator')
    signed = effect.envelope['payload']
    _require(effect.request_sha256 == _sha({'actor_id': effect.actor_id, 'kind': 'cleanup', 'reason': signed['reason']})
             and all(signed.get(key) == value for key, value in binding.items()), 'cleanup_effect_binding_changed')
    _gate(db, payload, supervision, deployment, network_verifier)
    final = control.facts['final_metrics']
    _require(final['sample'] == supervision.last_trusted and final['metrics'] == supervision.current_metrics
             and final['sample'].get('counter_epoch', '').startswith(request['engine_job_id'] + ':')
             and final['sample'].get('source') == 'seatunnel-2.3.12-source+managed-doris-journal-v1',
             'cleanup_final_metrics_binding_invalid')
    ingress = db.get(IngressBinding, command_id)
    _require(ingress is not None and ingress.closed, 'cleanup_ingress_open')
    measured = 0
    for frozen in _load_bindings(request, payload, command.envelope):
        load = db.get(LoadRun, frozen.command_id)
        expected = json.loads(json.dumps(asdict(frozen)))
        _require(load is not None and load.closed and load.binding == expected
                 and load.binding_sha256 == _sha(expected), 'cleanup_load_binding_invalid')
        for batch in db.scalars(select(LoadBatch).where(LoadBatch.command_id == frozen.command_id)):
            receipt = batch.receipt or {}
            _require(batch.phase == 'receipt' and batch.receipt_at is not None
                     and isinstance(batch.response_sha256, str) and re.fullmatch(r'[0-9a-f]{64}', batch.response_sha256)
                     and batch.backend_id in frozen.backend_ids and receipt.get('Label') == batch.label
                     and receipt.get('Status') == 'Success' and type(receipt.get('LoadBytes')) is int
                     and receipt['LoadBytes'] >= 0 and type(receipt.get('TxnId')) is int and receipt['TxnId'] > 0
                     and receipt.get('TwoPhaseCommit') == 'false'
                     and all(type(receipt.get(key)) is int for key in
                             ('NumberLoadedRows', 'NumberFilteredRows', 'NumberUnselectedRows'))
                     and receipt['NumberLoadedRows'] == 1 and receipt['NumberFilteredRows'] == 0
                     and receipt['NumberUnselectedRows'] == 0, 'cleanup_journal_unknown')
            measured += receipt['LoadBytes']
    _require(measured == final['sample']['sink_bytes'], 'cleanup_original_bytes_mismatch')
    return command, run, effect, control, deepcopy(request), resource, resolver.secrets.open(reference), binding


def _result(sessions, command_id, effect_id, code, *, unknown=False):
    with sessions.begin() as db:
        command = db.get(ManagedCommand, command_id)
        if command is None:
            return 'missing'
        lock_tenant(db, command.tenant_id)
        effect, control = db.get(ManagedControlEffect, effect_id), db.get(RunControl, command_id)
        # A reservation in either route permanently consumes DROP permission.
        sent = db.scalar(select(CleanupIntent).where(CleanupIntent.command_id == command_id)) is not None
        if effect is not None:
            effect.phase = 'unknown' if unknown or sent else 'denied'
            effect.result = {'reason': code, 'ownership_retained': True}
            effect.updated_at = datetime.now(timezone.utc)
        if control:
            control.facts = {**control.facts, 'real_cleanup': {'status': 'unknown' if unknown or sent else 'denied',
                             'reason': code, 'ownership_retained': True}}
    return 'cleanup_pending' if unknown or sent else 'cleanup_denied'


def advance_real_cleanup(command_id, *, sessions=Session, policy=None, secrets=None, manifest=None,
                         network_verifier=None, ingress_factory=None, client_factory=SeaTunnelClient,
                         adapter_factory=DorisCleanupAdapter):
    """Advance accepted cleanup under caller's command lock; never retry DROP.

    Adapter(resource, original_credential) exposes privileges(), inspect(route)
    (None means proven absence), drop(route, *, before_mutation=None). Dependency injection is a fixture
    seam and never a caller-supplied success flag in the public HTTP route.
    """
    with sessions() as db:
        effects = [row for row in effects_for(db, command_id) if row.kind == 'cleanup' and row.phase != 'denied']
        if not effects:
            return 'cleanup_missing'
        effect_id = effects[0].id
        if effects[0].phase == 'confirmed':
            return 'cleaned'
    try:
        deployment = _manifest(manifest)
        resolver = RealResourceResolver(sessions=sessions, policy=policy or ResourcePolicy(),
                                        secrets=secrets or SecretProvider(), manifest=deployment)
        with sessions.begin() as db:
            _, _, _, _, request, resource, credential, _ = _current(
                db, command_id, effect_id, resolver, deployment, network_verifier)
        adapter = adapter_factory(resource, credential)
        check_cleanup_grants(adapter.privileges(), resource['database'], resource.get('lifecycle_privileges', ()))
        service = ingress_factory(deepcopy(request)) if ingress_factory else ingress_service(
            deployment, sessions, policy=resolver.policy, secrets=resolver.secrets)
        observed = service.observe(command_id)
        _require(observed.get('complete') is True and observed.get('admission_closed') is True
                 and observed.get('load_quiescent') is True and observed.get('byte_kind') == 'measured'
                 and type(observed.get('sink_bytes')) is int, 'cleanup_external_writes_unsettled')
        at = datetime.fromisoformat(observed['observed_at'])
        _require(0 <= (datetime.now(timezone.utc) - at).total_seconds() <= 6, 'cleanup_observation_stale')
        proof = client_factory(request['engine_url']).observe(request['engine_job_id'], job_name(command_id))
        _require(isinstance(proof, dict) and proof.get('job_id') == request['engine_job_id']
                 and proof.get('job_name') == job_name(command_id)
                 and proof.get('engine_state') in ('FINISHED', 'CANCELED', 'FAILED'), 'cleanup_engine_not_terminal')
        # Check both existing originals before reserving the first destructive I/O.
        for name, route in request['real_routes'].items():
            with sessions() as db:
                intent = db.get(CleanupIntent, (command_id, name))
            current = adapter.inspect(route)
            _require((intent is not None and current is None) or current == route['object_identity'],
                     'cleanup_object_missing_or_replaced')
        for name, route in request['real_routes'].items():
            with sessions.begin() as db:
                _, _, _, _, current_request, _, _, binding = _current(
                    db, command_id, effect_id, resolver, deployment, network_verifier)
                _require(current_request == request, 'cleanup_original_changed')
                intent = db.get(CleanupIntent, (command_id, name))
                dispatch = intent is None
                if dispatch:
                    frozen = {'route': route, 'scope': binding}
                    now = datetime.now(timezone.utc)
                    intent = CleanupIntent(command_id=command_id, route=name, effect_id=effect_id,
                        binding=frozen, binding_sha256=_sha(frozen), phase='dispatching', created_at=now, updated_at=now)
                    db.add(intent)
                else:
                    _require(intent.effect_id == effect_id and intent.binding == {'route': route, 'scope': binding}
                             and intent.binding_sha256 == _sha(intent.binding), 'cleanup_intent_changed')
            # Reservation successfully committed before DROP. Recovery only inspects.
            try:
                with sessions.begin() as db:
                    _current(db, command_id, effect_id, resolver, deployment, network_verifier)
                    if dispatch:
                        def before_mutation():
                            current = _current(db, command_id, effect_id, resolver, deployment, network_verifier)
                            _require(current[4] == request and current[7] == binding,
                                     'cleanup_original_changed_before_mutation')
                        adapter.drop(route, before_mutation=before_mutation)
                    absent = adapter.inspect(route) is None
                    row = db.get(CleanupIntent, (command_id, name))
                    row.phase, row.reason = ('confirmed', None) if absent else ('unknown', 'cleanup_absence_unproven')
                    row.updated_at = datetime.now(timezone.utc)
                if not absent:
                    return _result(sessions, command_id, effect_id, 'cleanup_absence_unproven', unknown=True)
            except Exception:
                return _result(sessions, command_id, effect_id, 'cleanup_drop_result_unknown', unknown=True)
        with sessions.begin() as db:
            command, run, effect, control, current_request, _, _, _ = _current(
                db, command_id, effect_id, resolver, deployment, network_verifier)
            _require(current_request == request and all(db.get(CleanupIntent, (command_id, name)).phase == 'confirmed'
                     for name in request['real_routes']), 'cleanup_all_originals_not_confirmed')
            # Re-read both absences immediately before releasing physical target ownership.
            _require(all(adapter.inspect(route) is None for route in request['real_routes'].values()),
                     'cleanup_replacement_after_drop')
            owner = db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == command_id))
            db.delete(owner)
            run.status = 'cleaned'
            effect.phase, effect.updated_at = 'confirmed', datetime.now(timezone.utc)
            effect.result = {'original_objects_absent': True, 'ownership_released': True}
            control.wake_at = None
            control.facts = {**control.facts, 'real_cleanup': {'status': 'confirmed',
                             'original_objects_absent': True, 'ownership_released': True}}
            record(db, run, 'control.real_cleanup_confirmed', {'effect_id': effect_id, 'ownership_released': True})
        return 'cleaned'
    except Exception:
        return _result(sessions, command_id, effect_id, 'cleanup_prerequisites_unverified')
