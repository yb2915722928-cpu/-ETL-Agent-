"""Public P1 full-snapshot publication boundary; no automatic privilege grants.

Preparation reads the existing formal table with the frozen metadata account.
The DDL adapter uses the original writer reference, proves same-database ALTER
privilege separately, and never retries a swap. Missing formal tables deny.
"""
from copy import deepcopy
from datetime import timedelta
import json
import os
import re
from pathlib import Path

from fastapi import BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from .connector_policy import ResourcePolicy, fingerprint
from .db import Session
from .doris_objects import DorisObjects
from .evidence import append_event, lock_tenant
from .execution import ExecutionRun, ManagedCommand, get_run
from .execution_runtime import EngineAttempt
from .managed_secrets import SecretProvider
from .models import SecretReference
from .preparations import require_current_role
from .publication import PublicationIntent, PublicationService, PublishedSnapshot, _sha
from .real_engine import verify_real_runtime, HTTP_CONNECTOR
from .real_network import verify_live_network
from .writer_credentials import resolve_execution_writer

PUBLICATION_VERSION = 'etl-full-snapshot-publication-v1'


def _require(condition, code):
    if not condition:
        raise ValueError(code)


def schema_contract(columns):
    """Ignore integer display width only, never type/order/nullability changes."""
    result = []
    for item in columns:
        datatype = item['type'].lower().strip()
        match = re.fullmatch(r'(tinyint|smallint|int|integer|bigint)(?:\(\d+\))?', datatype)
        if match:
            datatype = 'int' if match[1] == 'integer' else match[1]
        result.append({'name': item['name'], 'type': datatype, 'nullable': item['nullable']})
    return result


def freeze_publication_contract(db, policy, secrets, facts):
    """Explicit Prepare P3 read only; no CREATE/ALTER or mutation permission."""
    asset = facts['target']
    resource = policy.get(asset['resource_id'])
    _require(resource is not None and fingerprint(resource) == asset['resource_fingerprint'],
             'publication_resource_changed')
    reference = db.get(SecretReference, asset['secret_reference_id'], populate_existing=True)
    _require(reference is not None and reference.revoked_at is None and reference.version == asset['secret_version'],
             'publication_probe_reference_unavailable')
    stable = DorisObjects(resource, secrets.open(reference)).inspect_stable(facts['plan']['target']['table'])
    _require(schema_contract(stable['schema']) == schema_contract(asset['schema']['columns']),
             'publication_stable_schema_changed')
    plan = facts.get('plan') or {}
    allow_empty = plan.get('allow_empty') is True
    return {'version': PUBLICATION_VERSION, 'mode': 'bounded_full', 'stable_identity': stable,
            'expected_schema_sha256': stable['schema_sha256'], 'allow_empty': allow_empty, 'retain_days': 7}


def check_publication_contract(db, policy, secrets, preparation):
    contract = preparation.frozen.get('publication_contract')
    if preparation.tool != 'pipeline.publish' or not preparation.frozen['execution_facts'].get('real_execution_contract'):
        return
    _require(contract == freeze_publication_contract(db, policy, secrets, preparation.frozen['execution_facts']),
             'publication_stable_identity_changed')


def check_publication_grants(rows, database, lifecycle_privileges=()):
    """Require database ALTER, reject global/catalog/role/cross-business-db grants.

    SELECT/LOAD/CREATE remain valid execution rights. This is deliberately a
    separate policy from metadata.read; that account must still be SELECT-only.
    """
    configured = set(lifecycle_privileges)
    _require(configured <= {'Alter_priv', 'Drop_priv'}, 'publication_lifecycle_policy_invalid')
    allowed = {'Select_priv', 'Load_priv', 'Create_priv'} | configured
    _require('Alter_priv' in configured, 'publication_alter_not_deployment_approved')
    found = False
    for row in rows:
        _require(row.get('Roles') in (None, '', 'NULL'), 'publication_roles_unqualified')
        for key, value in row.items():
            if not key.endswith('Privs') or value in (None, '', 'NULL'):
                continue
            if key == 'WorkloadGroupPrivs' and value == 'normal: Usage_priv':
                continue
            _require(key == 'DatabasePrivs', 'publication_global_or_table_grants_unqualified')
            for grant in str(value).split(';'):
                scope, separator, rights = grant.strip().partition(':')
                _require(bool(separator), 'publication_grant_shape_invalid')
                privileges = set(re.findall(r'\b\w+_priv\b', rights))
                if scope in ('internal.information_schema', 'internal.mysql'):
                    _require(privileges == {'Select_priv'}, 'publication_catalog_grants_unqualified')
                else:
                    _require(scope == 'internal.' + database and privileges <= allowed,
                             'publication_cross_database_grants_unqualified')
                    found = found or 'Alter_priv' in privileges
    _require(found, 'publication_alter_privilege_missing')


class DorisPublicationAdapter:
    def __init__(self, resource, credential, request, deployment):
        self.objects = DorisObjects(resource, credential)
        self.resource, self.request, self.deployment = resource, request, deployment
        self._before = None

    def _qualify(self, cluster, database):
        _require(cluster == self.resource['physical_cluster_id'] and database == self.resource['database'],
                 'publication_physical_scope_mismatch')
        _require(_sha(self.deployment) == self.request.get('real_manifest_sha256'), 'publication_manifest_changed')
        asset = self.request['resources']['target']
        entry = self.deployment.get('resources', {}).get(asset['resource_id'], {})
        _require(entry.get('resource_fingerprint') == asset['resource_fingerprint']
                 and entry.get('writer_resource_fingerprint') == fingerprint(self.resource)
                 and entry.get('physical_cluster_id') == cluster, 'publication_deployment_scope_mismatch')
        verified, _ = verify_real_runtime(self.deployment.get('runtime_overview'),
            self.deployment.get('connector_digests', {}), self.deployment.get('driver_digest'))
        _require(verified and self.deployment.get('http_connector_sha256') == HTTP_CONNECTOR['sha256'],
                 'publication_runtime_unqualified')
        raw = Path(os.environ['REAL_NETWORK_RECEIPT_FILE']).read_bytes()
        _require(verify_live_network(entry, raw) is True, 'publication_network_unqualified')

    def observe(self, cluster, database, target, shadow):
        self._qualify(cluster, database)
        observed = self.objects.observe_publication(target, shadow)
        self._before = deepcopy(observed)
        return observed

    def swap(self, cluster, database, target, shadow):
        self._qualify(cluster, database)
        check_publication_grants(self.objects.publication_privileges(), database,
                                 self.resource.get('lifecycle_privileges', ()))
        expected = deepcopy(self._before)
        _require(expected is not None, 'publication_before_identity_missing')
        def before_mutation():
            # Recheck after SHOW GRANTS and DDL connection setup, immediately before SQL.
            observed = self.objects.observe_publication(target, shadow)
            _require(observed == expected, 'publication_before_identity_changed')
            self._qualify(cluster, database)
        self.objects.swap_existing(target, shadow, before_mutation=before_mutation)


def publication_view(db, command_id):
    intent = db.scalar(select(PublicationIntent).where(PublicationIntent.command_id == command_id))
    command = db.get(ManagedCommand, command_id)
    contract = command.envelope.get('payload', {}).get('publication_contract') if command else None
    snapshot = db.scalar(select(PublishedSnapshot).where(PublishedSnapshot.intent_id == intent.id)) if intent else None
    return {'approved_contract': contract, 'intent': PublicationService._view(db, intent) if intent else None,
            'published': bool(snapshot), 'snapshot': {'id': snapshot.id, 'confirmed_at': snapshot.confirmed_at,
                'retain_until': snapshot.retain_until, 'identities': snapshot.identities} if snapshot else None,
            'cross_database_atomic': False}


def _service(sessions, command_id):
    """Construct from current deployment and original frozen writer, never caller IDs."""
    from .real_runtime import _manifest
    policy, secrets, deployment = ResourcePolicy(), SecretProvider(), _manifest(None)
    with sessions.begin() as db:
        command = db.get(ManagedCommand, command_id)
        _require(command is not None, 'publication_command_missing')
        lock_tenant(db, command.tenant_id)
        payload = command.envelope['payload']
        resource, reference = resolve_execution_writer(db, policy, command.tenant_id, command.project_id,
                                                       payload['resources']['target'])
        attempt = db.get(EngineAttempt, command_id)
        _require(attempt is not None, 'publication_attempt_missing')
        adapter = DorisPublicationAdapter(resource, secrets.open(reference), attempt.request, deployment)
    return PublicationService(sessions, adapter, policy=policy)


def advance_publication(command_id, intent_id, sessions=Session):
    from .real_runtime import _command_lock
    # Bind the original command before constructing any adapter or taking its lock.
    # Persist the next due time even if the worker is busy or its delivery ACK is lost.
    with sessions.begin() as db:
        intent = PublicationService._intent(db, intent_id)
        _require(intent.command_id == command_id, 'publication_command_mismatch')
        if intent.phase not in ('prepared', 'dispatching', 'unknown'):
            intent.recovery_at = None
            return PublicationService._view(db, intent)
        intent.recovery_at = db.scalar(select(func.clock_timestamp())) + timedelta(seconds=15)
    with _command_lock(sessions, command_id) as acquired:
        if not acquired:
            return 'busy'
        try:
            with sessions() as db:
                intent = db.get(PublicationIntent, intent_id)
                phase = intent.phase
                if phase not in ('prepared', 'dispatching', 'unknown'):
                    return PublicationService._view(db, intent)
            service = _service(sessions, command_id)
            if phase == 'prepared':
                return service.publish(intent_id)
            return service.reconcile(intent_id)
        except Exception:
            # Reserved operations remain dispatching/unknown. No retry of SQL.
            return 'publication_pending'


class PublishInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    idempotency_key: str = Field(min_length=1, max_length=128)


def register_publication_routes(app, Db, Actor, project_scope):
    path = '/api/tenants/{tenant_id}/projects/{project_id}/runs/{run_id}/publish'

    @app.post(path, status_code=202)
    def publish(tenant_id: str, project_id: str, run_id: str, body: PublishInput,
                background: BackgroundTasks, request: Request, db: Db, actor: Actor):
        lock_tenant(db, actor.tenant_id)
        try:
            project_scope(db, actor, tenant_id, project_id)
            require_current_role(db, tenant_id, project_id, actor.id, 'operator')
            run = get_run(db, tenant_id, project_id, run_id)
            command = db.scalar(select(ManagedCommand).where(ManagedCommand.run_id == run.id))
            # Same transaction accepts the unique intent. Bounded external I/O is
            # deferred until the HTTP dependency transaction has committed.
            service = PublicationService(policy=request.app.state.connector_policy)
            identifier = service.prepare_in(db, command.id, body.idempotency_key)
        except (HTTPException, ValueError) as error:
            code = error.detail if isinstance(error, HTTPException) else str(error)
            append_event(db, actor, 'publication.denied', {'reason': code}, project_id, run_id=run_id)
            return JSONResponse(status_code=error.status_code if isinstance(error, HTTPException) else 409,
                                content={'detail': code})
        append_event(db, actor, 'publication.accepted', {'intent_id': identifier}, project_id, run_id=run_id)
        # Derive the same database binding, including an isolated fixture schema.
        sessions = sessionmaker(db.get_bind(), expire_on_commit=False)
        facts = publication_view(db, command.id)
        phase = facts['intent']['phase']
        if phase in ('prepared', 'dispatching', 'unknown'):
            background.add_task(advance_publication, command.id, identifier, sessions)
        return {'intent_id': identifier, 'run_id': run.id,
                'phase': 'accepted' if phase == 'prepared' else phase, 'published': facts['published'],
                'ownership_retained': facts['intent']['ownership_retained'], 'cross_database_atomic': False}
