"""Persistent single-table swap intent; no real-L2 or bootstrap qualification.

The signed P1 publication_contract freezes the existing formal table before
execution. The new shadow is derived from the original EngineAttempt and its
quality/write journals. This module rechecks current independent approvals and durable gates;
there is no caller 'passed' Boolean. Adapter must own the exclusive DDL route,
verify cluster identity and issue exactly one REPLACE WITH TABLE swap=true.
Unknown results retain TargetOwnership permanently; reconciliation only reads.
Rollback requires a new P1 approval and is deliberately not implemented here.
"""
from datetime import datetime, timedelta
import hashlib
import json
import os
from typing import Protocol
from uuid import uuid4

from sqlalchemy import DateTime, JSON, String, UniqueConstraint, func, select
from sqlalchemy.orm import Mapped, mapped_column

from .command_signing import COMMAND_FORMAT, signing_bytes, verify_envelope
from .db import Base, Session
from .execution import ExecutionRun, ManagedCommand, authorization_bindings
from .evidence import lock_tenant
from .preparations import get_preparation, validate_preparation, decisions_for, SLOTS
from .connector_policy import ResourcePolicy
from .execution_runtime import EngineAttempt, TargetOwnership, record
from .managed_load import LoadBatch, LoadRun
from .quality_runtime import QualityRun, QualityRecord
from .real_ingress import IngressBinding
from .run_control import RunControl, check_running_authorization
from .supervision_runtime import RuntimeSupervision


def _sha(value):
    return hashlib.sha256(signing_bytes(value)).hexdigest()


def _object(identity):
    required = ('physical_cluster_id', 'database', 'table', 'marker', 'partition_ids', 'schema', 'schema_sha256')
    if (not isinstance(identity, dict) or any(not identity.get(key) for key in required if key != 'marker')
            or identity.get('marker') is not None and not isinstance(identity['marker'], str)):
        raise ValueError('physical_object_identity_missing')
    if (len(identity['partition_ids']) != len(set(identity['partition_ids']))
            or _sha(identity['schema']) != identity['schema_sha256']):
        raise ValueError('physical_object_identity_invalid')
    return {key: identity[key] for key in required}


def _moved(identity, table):
    return {**identity, 'table': table}


class PublicationAdapter(Protocol):
    def observe(self, physical_cluster_id: str, database: str, target: str, shadow: str) -> dict:
        """Return {'stable': exact identity, 'shadow': exact identity}, read only."""
        ...

    def swap(self, physical_cluster_id: str, database: str, target: str, shadow: str) -> None:
        """Exactly one ALTER TABLE target REPLACE WITH TABLE shadow PROPERTIES ('swap'='true').

        No retries or bootstrap. Current execution writer has no implicit ALTER
        authority; production adapter must separately prove frozen privileges.
        """
        ...


class PublicationIntent(Base):
    __tablename__ = 'publication_intents'
    __table_args__ = (UniqueConstraint('command_id'), UniqueConstraint('run_id'),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    command_id: Mapped[str] = mapped_column(String(36))
    run_id: Mapped[str] = mapped_column(String(36))
    idempotency_key: Mapped[str] = mapped_column(String(128))
    target_key: Mapped[str] = mapped_column(String(64), index=True)
    inputs: Mapped[dict] = mapped_column(JSON)
    input_sha256: Mapped[str] = mapped_column(String(64))
    phase: Mapped[str] = mapped_column(String(24))
    reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    recovery_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class PublishedSnapshot(Base):
    __tablename__ = 'published_snapshots'
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    intent_id: Mapped[str] = mapped_column(String(36), unique=True)
    run_id: Mapped[str] = mapped_column(String(36))
    target_key: Mapped[str] = mapped_column(String(64))
    identities: Mapped[dict] = mapped_column(JSON)
    confirmed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    retain_until: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class PublicationService:
    def __init__(self, sessions=Session, adapter: PublicationAdapter | None = None, *, policy=None):
        self.sessions, self.adapter, self.policy = sessions, adapter, policy

    def prepare(self, command_id, idempotency_key):
        with self.sessions.begin() as db:
            return self.prepare_in(db, command_id, idempotency_key)

    def prepare_in(self, db, command_id, idempotency_key):
        if not isinstance(idempotency_key, str) or not 0 < len(idempotency_key) <= 128:
            raise ValueError('publication_key_invalid')
        command = db.get(ManagedCommand, command_id)
        if command is None:
            raise ValueError('publication_command_missing')
        lock_tenant(db, command.tenant_id)
        # A repeated accepted request must not run pre-swap gates against moved
        # objects or an already-published status. It never grants another swap.
        previous = db.scalar(select(PublicationIntent).where(
            PublicationIntent.command_id == command_id).with_for_update())
        if previous:
            run = db.get(ExecutionRun, command.run_id)
            check_running_authorization(db, command, run)
            if previous.idempotency_key != idempotency_key:
                raise ValueError('publication_intent_conflict')
            return previous.id
        inputs, owner = self._gates(db, command_id)
        pending = db.scalar(select(PublicationIntent).where(
            PublicationIntent.target_key == owner.target_key,
            PublicationIntent.phase != 'confirmed'))
        if pending:
            raise ValueError('publication_target_isolated')
        now = db.scalar(select(func.clock_timestamp()))
        identifier = str(uuid4())
        db.add(PublicationIntent(id=identifier, command_id=command_id, run_id=inputs['run_id'],
            idempotency_key=idempotency_key, target_key=owner.target_key, inputs=inputs,
            input_sha256=_sha(inputs), phase='prepared', created_at=now, updated_at=now, recovery_at=now))
        db.flush()
        return identifier

    def publish(self, intent_id):
        with self.sessions.begin() as db:
            intent = self._intent(db, intent_id)
            if intent.phase != 'prepared':
                return self._view(db, intent)  # Never replay dispatching/unknown.
            current, _ = self._gates(db, intent.command_id)
            if _sha(current) != intent.input_sha256:
                raise ValueError('publication_inputs_changed')
            if self.adapter is None:
                raise ValueError('publication_transport_unqualified')
            intent.phase = 'dispatching'
            intent.updated_at = db.scalar(select(func.clock_timestamp()))
            intent.recovery_at = intent.updated_at + timedelta(seconds=15)
            inputs = intent.inputs
            record(db, db.get(ExecutionRun, intent.run_id), 'publication.swap_reserved',
                   {'intent_id': intent.id, 'input_sha256': intent.input_sha256, 'target_key': intent.target_key})
        # The operation intent is committed before any mutation. Validate both
        # actual objects immediately before swap. Exclusive external DDL access
        # is an Adapter prerequisite; DB rows cannot fence outside actors.
        try:
            with self.sessions.begin() as db:
                # Match role/credential revocation ordering and hold the tenant
                # lock across this bounded external mutation. This still does
                # not fence unrelated Doris administrators or old transports.
                command = db.get(ManagedCommand, inputs['command_id'])
                if command is None:
                    raise ValueError('publication_command_missing')
                lock_tenant(db, command.tenant_id)
                current, _ = self._gates(db, inputs['command_id'])
                if _sha(current) != _sha(inputs):
                    raise ValueError('publication_inputs_changed')
                observed = self._observe(inputs)
                if observed != inputs['before']:
                    raise ValueError('publication_before_identity_changed')
                identity = inputs['before']['stable']
                self.adapter.swap(identity['physical_cluster_id'], identity['database'],
                                  identity['table'], inputs['before']['shadow']['table'])
        except Exception:
            with self.sessions.begin() as db:
                intent = self._intent(db, intent_id)
                intent.phase, intent.reason = 'unknown', 'external_effect_unconfirmed'
                intent.updated_at = db.scalar(select(func.clock_timestamp()))
                intent.recovery_at = intent.updated_at + timedelta(seconds=15)
                return self._view(db, intent)
        # A successful SQL response is not sufficient: confirm the moved physical
        # identities. If this DB commit fails, dispatching remains durable.
        return self.reconcile(intent_id)

    def reconcile(self, intent_id):
        with self.sessions.begin() as db:
            intent = self._intent(db, intent_id)
            if intent.phase not in ('dispatching', 'unknown'):
                return self._view(db, intent)
            inputs = intent.inputs
        try:
            observed = self._observe(inputs)
        except Exception:
            observed = None
        with self.sessions.begin() as db:
            intent = self._intent(db, intent_id)
            if intent.phase not in ('dispatching', 'unknown'):
                return self._view(db, intent)
            now = db.scalar(select(func.clock_timestamp()))
            if observed == inputs['after']:
                # Recheck retained ownership before recording a snapshot. Do not
                # rerun pre-swap object gates against the now moved objects.
                owner = db.scalar(select(TargetOwnership).where(
                    TargetOwnership.target_key == intent.target_key).with_for_update())
                if not owner or owner.command_id != intent.command_id:
                    intent.phase, intent.reason = 'unknown', 'target_ownership_changed'
                else:
                    intent.phase, intent.reason = 'confirmed', None
                    db.add(PublishedSnapshot(id=str(uuid4()), intent_id=intent.id, run_id=intent.run_id,
                        target_key=intent.target_key, identities={'before': inputs['before'], 'after': observed},
                        confirmed_at=now, retain_until=now + timedelta(days=7)))
                    run = db.get(ExecutionRun, intent.run_id)
                    if run is not None:
                        run.status = 'published'
                        record(db, run, 'publication.confirmed', {'intent_id': intent.id,
                            'input_sha256': intent.input_sha256, 'retention_days': 7,
                            'cross_database_atomic': False})
                    control = db.get(RunControl, intent.command_id)
                    if control is not None:
                        control.facts = {**control.facts, 'publication': {'intent_id': intent.id,
                            'status': 'confirmed', 'published': True, 'retention_days': 7}}
                        control.wake_at = None
                    # The exact moved identities, snapshot and published status
                    # commit with release of this command's target serialization.
                    # A failed commit rolls all four back, retaining isolation;
                    # retained snapshots protect old objects independently.
                    db.delete(owner)
            else:
                intent.phase, intent.reason = 'unknown', (
                    'before_state_observed_no_replay' if observed == inputs['before'] else 'object_identity_unconfirmed')
            intent.updated_at = now
            intent.recovery_at = now + timedelta(seconds=15) if intent.phase == 'unknown' else None
            return self._view(db, intent)

    def rollback(self, snapshot_id, approval_command_id):
        # Never infer a new P1 approval from the original execution or a caller
        # Boolean. No rollback mutation Adapter is exposed in this first seam.
        raise ValueError('rollback_requires_new_p1_approval_and_verified_retained_snapshot')

    def _observe(self, inputs):
        if self.adapter is None:
            raise ValueError('publication_transport_unqualified')
        stable, shadow = inputs['before']['stable'], inputs['before']['shadow']
        observed = self.adapter.observe(stable['physical_cluster_id'], stable['database'], stable['table'], shadow['table'])
        return {'stable': _object(observed['stable']), 'shadow': _object(observed['shadow'])}

    @staticmethod
    def _intent(db, identifier):
        row = db.get(PublicationIntent, identifier)
        if row is None:
            raise ValueError('publication_intent_missing')
        command = db.get(ManagedCommand, row.command_id)
        if command is None:
            raise ValueError('publication_command_missing')
        lock_tenant(db, command.tenant_id)
        return db.scalar(select(PublicationIntent).where(PublicationIntent.id == identifier)
                         .with_for_update().execution_options(populate_existing=True))

    @staticmethod
    def _view(db, row):
        retained = db.scalar(select(TargetOwnership.target_key).where(
            TargetOwnership.target_key == row.target_key,
            TargetOwnership.command_id == row.command_id)) is not None
        return {'intent_id': row.id, 'run_id': row.run_id, 'phase': row.phase, 'reason': row.reason,
                'ownership_retained': retained, 'real_l2_qualified': False}

    def _gates(self, db, command_id):
        command = db.get(ManagedCommand, command_id)
        if command is None:
            raise ValueError('publication_command_missing')
        run = db.get(ExecutionRun, command.run_id)
        if run is None or (run.tenant_id, run.project_id) != (command.tenant_id, command.project_id):
            raise ValueError('publication_run_binding_invalid')
        check_running_authorization(db, command, run)
        payload = verify_envelope(command.envelope, os.environ['COMMAND_PUBLIC_KEY'], COMMAND_FORMAT)
        preparation = get_preparation(db, run.tenant_id, run.project_id, run.preparation_id)
        if preparation.tool != 'pipeline.publish' or preparation.risk_level != 'P1':
            raise ValueError('publication_requires_p1_publish_approval')
        policy = self.policy if self.policy is not None else ResourcePolicy()
        if not validate_preparation(db, policy, preparation, check_expiry=False)['valid']:
            raise ValueError('publication_preparation_invalid')
        approvals = decisions_for(db, preparation)
        if ({item.slot for item in approvals if item.decision == 'approve'} != set(SLOTS)
                or len(approvals) != 2 or len({item.checker_id for item in approvals}) != 2
                or any(item.checker_id in (preparation.maker_id, run.subject)
                       or item.frozen_sha256 != preparation.frozen_sha256 for item in approvals)):
            raise ValueError('publication_independent_approvals_missing')
        expected = {'command_id': command_id, 'run_id': run.id,
                    'accepted_at': run.accepted_at.isoformat(), **authorization_bindings(preparation, run.subject)}
        if any(payload.get(key) != value for key, value in expected.items()):
            raise ValueError('publication_signed_binding_invalid')
        owner = db.scalar(select(TargetOwnership).where(TargetOwnership.command_id == command_id).with_for_update())
        attempt = db.get(EngineAttempt, command_id)
        supervision = db.get(RuntimeSupervision, command_id)
        control = db.get(RunControl, command_id)
        quality = db.get(QualityRun, run.id)
        if not owner or not attempt or not supervision or not control or not quality:
            raise ValueError('publication_facts_missing')
        if (_sha(owner.identity) != owner.target_key or attempt.engine_job_id != run.engine_job_id
                or not run.engine_job_id or attempt.request.get('run_id') != run.id
                or attempt.request.get('command_id') != command_id or _sha(attempt.request) != attempt.request_sha256):
            raise ValueError('publication_original_attempt_invalid')
        from .real_publication import PUBLICATION_VERSION, schema_contract
        from .doris_objects import object_marker
        from .real_engine import shadow_table
        facts = payload.get('publication_contract')
        if (not isinstance(facts, dict) or facts != preparation.frozen.get('publication_contract')
                or facts.get('version') != PUBLICATION_VERSION or facts.get('mode') != 'bounded_full'
                or type(facts.get('allow_empty')) is not bool or facts.get('retain_days') != 7):
            raise ValueError('publication_frozen_full_snapshot_missing')
        # The signed contract freezes the existing formal object before approval.
        # The new shadow cannot exist at Prepare; derive it from the original
        # attempt and independently verified journal identity after execution.
        routes = attempt.request.get('real_routes', {})
        route = routes.get('valid', {})
        stable, shadow = _object(facts.get('stable_identity')), _object(route.get('object_identity'))
        if (attempt.request.get('adapter') != 'real' or set(routes) != {'valid', 'rejected'}
                or route.get('object_run_id') != run.id or route.get('object_command_id') != command.id
                or route.get('target_table') != stable['table']
                or shadow['table'] != shadow_table(stable['table'], run.id)
                or shadow['marker'] != object_marker(command.id)
                or stable['table'] == shadow['table'] or stable['marker'] == shadow['marker']
                or set(stable['partition_ids']) & set(shadow['partition_ids'])
                or any(stable[key] != owner.identity.get(key) for key in ('physical_cluster_id', 'database', 'table'))
                or any(stable[key] != shadow[key] for key in ('physical_cluster_id', 'database'))
                or schema_contract(stable['schema']) != schema_contract(shadow['schema'])
                or stable['schema_sha256'] != facts.get('expected_schema_sha256')):
            raise ValueError('publication_schema_or_physical_identity_mismatch')
        termination = control.facts.get('termination', {})
        quiescence = control.facts.get('load_quiescence', {})
        if (supervision.publication_veto or supervision.stop_request or supervision.status != 'finished'
                or run.status != 'supervision_passed' or control.facts.get('inflight')
                or termination.get('confirmed') is not True or termination.get('basis') != 'engine_terminal'
                or termination.get('engine_job_id') != run.engine_job_id
                or termination.get('engine_state') != 'FINISHED'
                or quiescence.get('complete') is not True or quiescence.get('admission_closed') is not True
                or quiescence.get('load_quiescent') is not True
                or quiescence.get('external_route_verified') is not True
                or quiescence.get('object_identity_verified') is not True
                or quiescence.get('engine_job_id') != run.engine_job_id
                or control.facts.get('final_metrics', {}).get('status') != 'known'):
            raise ValueError('publication_supervision_or_termination_unproven')
        report = quality.report or {}
        execution_contract = payload.get('real_execution_contract', {})
        expected_quality = {'plan': preparation.frozen['plan'],
            'primary_key_fields': execution_contract.get('source_primary_key'),
            'route_commands': {name: item['command_id'] for name, item in routes.items()},
            'stage_version': execution_contract.get('quality_stage_version'),
            'cleaning': execution_contract.get('cleaning'), 'filters': execution_contract.get('filters')}
        final = control.facts.get('final_metrics', {})
        if (quality.contract != expected_quality or _sha(quality.contract) != quality.contract_sha256
                or final.get('sample') != supervision.last_trusted
                or final.get('metrics') != supervision.current_metrics
                or any(supervision.current_metrics.get(key) != report.get(key)
                    for key in ('source_read', 'evaluated', 'valid', 'rejected'))):
            raise ValueError('publication_quality_or_final_counter_binding_invalid')
        from .runtime_supervision import complete_empty_snapshot
        allow_empty = facts.get('allow_empty') is True
        if (allow_empty != (preparation.frozen['plan'].get('allow_empty') is True)
                or allow_empty != (supervision.contract.get('empty_policy') == 'allow_complete_empty')):
            raise ValueError('publication_empty_permission_binding_invalid')
        empty_report = (complete_empty_snapshot(supervision.last_trusted)
                        and all(type(report.get(key)) is int and report[key] == 0 for key in
                                ('source_read', 'parsed', 'evaluated', 'valid', 'rejected')))
        if (not quality.finalized or report.get('publication_veto') is not False or quality.failures
                or quality.contract_sha256 != attempt.request.get('quality_contract_sha256')
                or quality.source_facts != {'source_read': report.get('source_read'),
                    'parsed': report.get('evaluated'), 'complete': True}
                or report.get('contract_sha256') != quality.contract_sha256 or report.get('run_id') != run.id
                or ((not report.get('source_read') or not report.get('valid'))
                    and not (allow_empty and empty_report))
                or report.get('parsed') != report.get('evaluated')
                or report.get('evaluated') != report.get('valid', 0) + report.get('rejected', 0)
                or report.get('final_written') != {'valid': report.get('valid'), 'rejected': report.get('rejected')}):
            raise ValueError('publication_quality_unproven_or_empty')
        route_handles = quality.contract['route_commands']
        if (set(route_handles) != {'valid', 'rejected'} or len(set(route_handles.values())) != 2
                or quiescence.get('route_command_ids') != route_handles
                or route_handles != {name: item['command_id'] for name, item in routes.items()}):
            raise ValueError('publication_load_quiescence_binding_invalid')
        ingress = db.get(IngressBinding, command_id)
        records = list(db.scalars(select(QualityRecord).where(QualityRecord.run_id == run.id)))
        if (not ingress or not ingress.closed or ingress.failures
                or len(records) != report.get('evaluated')
                or set(ingress.links) != {item.stable_key for item in records}
                or len(set(ingress.links.values())) != len(records)):
            raise ValueError('publication_quality_receipt_links_invalid')
        record_by_label = {ingress.links[item.stable_key]: item for item in records}
        for route, handle in route_handles.items():
            load = db.get(LoadRun, handle)
            if (not load or not load.closed or load.binding.get('run_id') != run.id
                    or load.binding.get('quality_contract_sha256') != quality.contract_sha256
                    or load.close_id in ('supervision-incomplete', 'supervision-stale')):
                raise ValueError('publication_load_unsettled')
            if route == 'valid' and any(load.binding.get(key) != shadow.get(key) for key in
                                       ('physical_cluster_id', 'database', 'table')):
                raise ValueError('publication_shadow_route_mismatch')
            if (load.binding.get('engine_job_id') != run.engine_job_id
                    or load.binding.get('command_sha256') != _sha(command.envelope)
                    or load.binding.get('object_id') != _sha(routes[route]['object_identity'])):
                raise ValueError('publication_load_original_mismatch')
            batches = list(db.scalars(select(LoadBatch).where(LoadBatch.command_id == handle)))
            if (len(batches) != report['final_written'][route]
                    or any(batch.phase != 'receipt' or (batch.receipt or {}).get('Status') != 'Success'
                        or not batch.response_sha256 or batch.receipt_at is None
                        or batch.receipt.get('Label') != batch.label
                        or batch.receipt.get('NumberLoadedRows') != 1
                        or batch.receipt.get('NumberFilteredRows') != 0
                        or batch.receipt.get('NumberUnselectedRows') != 0
                        or batch.label not in record_by_label
                        or record_by_label[batch.label].route != route
                        or record_by_label[batch.label].route_body_sha256 != batch.body_sha256
                        for batch in batches)):
                raise ValueError('publication_load_unsettled')
        before = {'stable': stable, 'shadow': shadow}
        after = {'stable': _moved(shadow, stable['table']), 'shadow': _moved(stable, shadow['table'])}
        return {'run_id': run.id, 'command_id': command_id, 'command_sha256': _sha(command.envelope),
                'quality_report_sha256': _sha(report), 'before': before, 'after': after}, owner
