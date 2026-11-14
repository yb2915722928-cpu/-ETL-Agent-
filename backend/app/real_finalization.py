"""Reconcile original real-run journals into publication eligibility, never publish.

The caller holds the command advisory lock. External object reads and ingress
barriers run without a tenant transaction; a final transaction rechecks every
local original and current authorization before clearing the supervision veto.
Deployment receipt files alone do not qualify a network route.
"""
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import json

from sqlalchemy import select

from .connector_policy import ResourcePolicy
from .db import Session
from .doris_objects import DorisObjects
from .engine_adapter import job_name
from .evidence import lock_tenant
from .execution import ExecutionRun, ManagedCommand
from .execution_runtime import EngineAttempt, TargetOwnership, record
from .managed_load import LoadBatch, LoadRun
from .managed_secrets import SecretProvider
from .quality_runtime import QualityRecord, QualityRun, QualityRuntime
from .real_ingress import IngressBinding
from .real_resources import RealResourceResolver
from .real_runtime import (_authorized, _gate, _load_bindings, _manifest,
                           _original, _require, _sha, ingress_service)
from .run_control import RunControl
from .runtime_supervision import complete_empty_snapshot, evaluate_observation
from .supervision_runtime import RuntimeSupervision


def _digest(value):
    return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


def _current(db, command_id, policy, secrets, deployment, network_verifier, resolver):
    command, run, prep, payload, resource, reference = _authorized(db, command_id, policy, secrets)
    attempt = db.get(EngineAttempt, command_id)
    _require(attempt is not None, 'finalization_attempt_missing')
    _, request = _original(command, run, attempt)
    supervision, control = db.get(RuntimeSupervision, command_id), db.get(RunControl, command_id)
    _require(run.status == 'engine_finished_awaiting_quality' and run.engine_job_id == attempt.engine_job_id
             and supervision is not None
             and control is not None and not supervision.stop_request and not supervision.reasons
             and supervision.status == 'finished', 'finalization_not_ready')
    entry = _gate(db, payload, supervision, deployment, network_verifier)
    _require(request.get('real_manifest_sha256') == _sha(deployment)
             and entry.get('physical_cluster_id') == resource['physical_cluster_id']
             and entry.get('engine_url') == request.get('engine_url')
             and entry.get('ingress_url') == request.get('ingress_url'), 'finalization_manifest_changed')
    # This local resolver validation checks the exact route schemas, suffixes,
    # writer slot and approved quality contract. It performs no object inspection.
    _, _, _, resolved_request, routes, writer, writer_reference, quality = resolver._authorized(db, command_id)
    _require(resolved_request == request and writer == resource and writer_reference.id == reference.id,
             'finalization_writer_changed')
    termination, final = control.facts.get('termination', {}), control.facts.get('final_metrics', {})
    _require(termination.get('confirmed') is True and termination.get('basis') == 'engine_terminal'
             and termination.get('engine_state') == 'FINISHED'
             and termination.get('engine_job_id') == attempt.engine_job_id
             and termination.get('job_name') == job_name(command_id), 'finalization_original_finish_missing')
    sample = supervision.last_trusted
    _require(final.get('status') == 'known' and isinstance(sample, dict)
             and sample == final.get('sample') and sample.get('state') == 'finished'
             and sample.get('source') == 'seatunnel-2.3.12-source+managed-doris-journal-v1'
             and sample.get('counter_epoch', '').startswith(attempt.engine_job_id + ':'),
             'finalization_final_sample_missing')
    decision = evaluate_observation(supervision.contract, sample, None, datetime.now(timezone.utc))
    _require(decision['status'] == 'finished' and not decision['reasons']
             and decision['metrics'] == supervision.current_metrics == final.get('metrics')
             and sample['source_read'] == sample['evaluated'] == sample['final_output']
             and (sample['evaluated'] > 0 or (prep.frozen['plan'].get('allow_empty') is True
                  and supervision.contract.get('empty_policy') == 'allow_complete_empty'
                  and complete_empty_snapshot(sample))), 'finalization_metrics_unverified')
    _require(quality.contract_sha256 == request.get('quality_contract_sha256')
             and not quality.failures, 'finalization_quality_binding_invalid')
    route_commands = {name: route['command_id'] for name, route in routes.items()}
    ingress = db.get(IngressBinding, command_id)
    facts = {'command_id': command_id, 'run_id': run.id, 'engine_job_id': attempt.engine_job_id,
        'route_commands': route_commands, 'quality_contract_sha256': quality.contract_sha256,
        'source_primary_key': payload['real_execution_contract']['source_primary_key']}
    _require(ingress is not None and ingress.facts == facts and ingress.closed and not ingress.failures,
             'finalization_ingress_not_closed')
    bindings, loads, journals = _load_bindings(request, payload, command.envelope), {}, []
    for binding in bindings:
        row = db.get(LoadRun, binding.command_id)
        expected = json.loads(json.dumps(asdict(binding)))
        _require(row is not None and row.binding == expected and row.binding_sha256 == _sha(expected)
                 and row.closed and row.close_id not in ('supervision-incomplete', 'supervision-stale'),
                 'finalization_load_binding_invalid')
        loads[row.command_id] = {'binding': row.binding, 'binding_sha256': row.binding_sha256,
            'counter_epoch': row.counter_epoch, 'closed': row.closed, 'close_id': row.close_id}
        for batch in db.scalars(select(LoadBatch).where(LoadBatch.command_id == row.command_id)
                                .order_by(LoadBatch.label)):
            receipt = batch.receipt or {}
            _require(batch.phase == 'receipt' and _digest(batch.response_sha256)
                     and batch.receipt_at is not None and batch.backend_id in binding.backend_ids
                     and receipt.get('Label') == batch.label and receipt.get('Status') == 'Success'
                     and type(receipt.get('TxnId')) is int and receipt['TxnId'] > 0
                     and type(receipt.get('LoadBytes')) is int and receipt['LoadBytes'] >= 0
                     and receipt.get('TwoPhaseCommit') == 'false'
                     and all(type(receipt.get(key)) is int for key in
                             ('NumberLoadedRows', 'NumberFilteredRows', 'NumberUnselectedRows'))
                     and receipt.get('NumberLoadedRows') == 1 and receipt.get('NumberFilteredRows') == 0
                     and receipt.get('NumberUnselectedRows') == 0,
                     'finalization_original_receipt_missing')
            journals.append({key: getattr(batch, key) for key in ('label', 'command_id', 'partition',
                'sequence', 'body_sha256', 'phase', 'receipt', 'response_sha256', 'backend_id',
                'receipt_at', 'reason')})
    target = {'physical_cluster_id': resource['physical_cluster_id'], 'database': resource['database'],
              'table': prep.frozen['plan']['target']['table']}
    owner = db.get(TargetOwnership, _sha(target))
    _require(owner is not None and owner.command_id == command_id and owner.identity == target,
             'finalization_owner_missing')
    records = [{key: getattr(row, key) for key in ('stable_key', 'body_sha256', 'stage_sha256',
                'route_body_sha256', 'route', 'errors')} for row in db.scalars(
        select(QualityRecord).where(QualityRecord.run_id == run.id).order_by(QualityRecord.stable_key))]
    frozen = deepcopy({'run_id': run.id, 'envelope': command.envelope, 'request': request,
        'request_sha256': attempt.request_sha256, 'engine_job_id': attempt.engine_job_id,
        'quality_contract': quality.contract, 'quality_sha256': quality.contract_sha256,
        'control_facts': control.facts, 'sample': sample, 'current_metrics': supervision.current_metrics,
        'supervision_contract': supervision.contract, 'ingress': {'facts': ingress.facts,
            'closed': ingress.closed, 'failures': ingress.failures, 'links': ingress.links},
        'loads': loads, 'journals': journals, 'quality_records': records, 'owner': target})
    return command, run, supervision, control, quality, frozen, bindings, resource, secrets.open(reference)


def _pending(sessions, command_id, code):
    # Only finite codes reach persistence, never exception messages or credentials.
    with sessions.begin() as db:
        command = db.get(ManagedCommand, command_id)
        if command is None:
            return 'missing'
        lock_tenant(db, command.tenant_id)
        supervision, control = db.get(RuntimeSupervision, command_id), db.get(RunControl, command_id)
        if supervision is not None:
            supervision.publication_veto = True
        if control is not None:
            control.facts = {**{key: value for key, value in control.facts.items() if key != 'load_quiescence'},
                'real_finalization': {'status': 'pending', 'reason': code,
                    'publication_eligible': False, 'ownership_retained': True}}
        run = db.get(ExecutionRun, command.run_id)
        return run.status if run else 'missing'


def finalize_real(command_id, *, sessions=Session, ingress_factory=None, policy=None,
                  secrets=None, manifest=None, network_verifier=None):
    """Finalize quality eligibility only; caller already owns command advisory lock.

    Explicit network/transport seams are for fixtures. Production uses the actual
    measured deployment gate; its default unknown network verifier denies release.
    """
    with sessions() as db:
        command = db.get(ManagedCommand, command_id)
        run = db.get(ExecutionRun, command.run_id) if command else None
        if run is None or run.status != 'engine_finished_awaiting_quality':
            return run.status if run else 'missing'
    try:
        policy = policy if policy is not None else ResourcePolicy()
        secrets = secrets if secrets is not None else SecretProvider()
        deployment = _manifest(manifest)
        resolver = RealResourceResolver(sessions=sessions, policy=policy, secrets=secrets, manifest=deployment)
        with sessions.begin() as db:
            _, _, supervision, _, _, frozen, bindings, resource, credential = _current(
                db, command_id, policy, secrets, deployment, network_verifier, resolver)
            supervision.publication_veto = True
    except Exception:
        return _pending(sessions, command_id, 'finalization_prerequisites_unverified')
    try:
        # No outer tenant transaction surrounds resolver callbacks, Doris metadata
        # inspection, service construction, or its independent journal barrier.
        resolver.authorize(frozen['ingress']['facts'])
        for route in frozen['request']['real_routes'].values():
            current = DorisObjects(resource, credential).inspect_shadow(route['target_table'],
                route['object_run_id'], route['object_command_id'])
            _require(current == route['object_identity'], 'finalization_object_identity_changed')
        service = ingress_factory(deepcopy(frozen['request'])) if ingress_factory is not None else ingress_service(
            deployment, sessions=sessions, policy=policy, secrets=secrets)
        observed = service.observe(command_id)
        sample = frozen['sample']
        _require(isinstance(observed, dict) and observed.get('complete') is True
                 and observed.get('admission_closed') is True and observed.get('load_quiescent') is True
                 and not observed.get('failures') and observed.get('byte_kind') == 'measured'
                 and sample['counter_epoch'] == frozen['engine_job_id'] + ':' + observed.get('counter_epoch', '')
                 and all(type(observed.get(key)) is int and observed[key] == sample[key]
                         for key in ('evaluated', 'valid', 'rejected', 'final_output', 'sink_bytes')),
                 'finalization_ingress_unverified')
        observed_at = datetime.fromisoformat(observed['observed_at'])
        _require(0 <= (datetime.now(timezone.utc) - observed_at).total_seconds() <= 6,
                 'finalization_ingress_stale')
    except Exception:
        return _pending(sessions, command_id, 'finalization_external_observation_unverified')
    try:
        quality = QualityRuntime(sessions)
        with sessions() as db:
            quality_row = db.get(QualityRun, frozen['run_id'])
            finalized = quality_row.finalized
        if not finalized:
            quality.source_progress(frozen['run_id'], source_read=sample['source_read'],
                parsed=observed['evaluated'], complete=True, parse_failed=False)
        report = quality.report(frozen['run_id'], frozen['ingress']['links'])
        _require(isinstance(report, dict) and report.get('run_id') == frozen['run_id']
                 and report.get('contract_sha256') == frozen['quality_sha256']
                 and report.get('publication_veto') is False
                 and not report.get('reasons') and report.get('evaluated') == sample['evaluated']
                 and report.get('source_read') == sample['source_read'] == report.get('parsed')
                 and report.get('valid') == observed['valid'] and report.get('rejected') == observed['rejected']
                 and report.get('final_written') == {'valid': observed['valid'], 'rejected': observed['rejected']},
                 'finalization_quality_report_veto')
        with sessions.begin() as db:
            command, run, supervision, control, quality_row, current, _, _, _ = _current(
                db, command_id, policy, secrets, deployment, network_verifier, resolver)
            _require(current == frozen and quality_row.finalized and quality_row.report == report
                     and quality_row.source_facts == {'source_read': sample['source_read'],
                         'parsed': sample['evaluated'], 'complete': True}, 'finalization_originals_changed')
            supervision.publication_veto = False
            run.status = 'supervision_passed'
            # Completed source metrics must not accrue artificial elapsed time.
            # Later publication/cleanup/stop entries reauthorize independently.
            control.wake_at = None
            control.facts = {**control.facts, 'real_finalization': {'status': 'quality_finalized',
                'publication_eligible': True, 'published': False, 'ownership_retained': True},
                'quality_finalized': {'run_id': run.id, 'contract_sha256': quality_row.contract_sha256,
                    'evaluated': report['evaluated'], 'valid': report['valid'], 'rejected': report['rejected']},
                'load_quiescence': {'complete': True, 'admission_closed': True, 'load_quiescent': True,
                    'external_route_verified': True, 'object_identity_verified': True,
                    'engine_job_id': frozen['engine_job_id'],
                    'route_command_ids': frozen['ingress']['facts']['route_commands']}}
            record(db, run, 'execution.real_quality_finalized', {'command_id': command.id,
                'quality_contract_sha256': quality_row.contract_sha256, 'evaluated': report['evaluated'],
                'publication_eligible': True, 'published': False, 'ownership_retained': True})
            return run.status
    except Exception:
        return _pending(sessions, command_id, 'finalization_quality_or_originals_unverified')


def finalize_real_quality_diagnostics(command_id, *, sessions=Session):
    """Persist terminal quality diagnostics, never grant publication eligibility.

    The caller owns the command advisory lock. The ingress barrier prevents a
    previously admitted POST from judging another record after this final report.
    Only original PostgreSQL facts are read; revoked roles/keys do not block this
    observation, and no current credential or external system is consulted.
    """
    import hashlib
    from .execution import authorization_bindings
    from .preparations import Preparation
    from .real_contract import real_execution_contract
    from .real_ingress import IngressDenied, IngressService

    service = IngressService(None, None, sessions)
    try:
        with service._serial(command_id):
            with sessions.begin() as db:
                command = db.get(ManagedCommand, command_id)
                if command is None:
                    return None
                lock_tenant(db, command.tenant_id)
                run, attempt = db.get(ExecutionRun, command.run_id), db.get(EngineAttempt, command_id)
                if run is None or attempt is None:
                    return None
                payload, request = _original(command, run, attempt)
                prep = db.get(Preparation, run.preparation_id)
                _require(prep is not None and (prep.tenant_id, prep.project_id) ==
                         (run.tenant_id, run.project_id) == (command.tenant_id, command.project_id),
                         'quality_diagnostic_scope_mismatch')
                expected = {'command_id': command.id, 'run_id': run.id,
                    'accepted_at': run.accepted_at.isoformat(), **authorization_bindings(prep, run.subject)}
                _require(all(payload.get(key) == value for key, value in expected.items())
                         and _sha(prep.frozen) == prep.frozen_sha256,
                         'quality_diagnostic_approved_binding_mismatch')
                contract, facts = payload['real_execution_contract'], prep.frozen['execution_facts']
                _require(contract == real_execution_contract(prep.frozen['plan'], facts['source']['schema'],
                         facts['target']['schema']) == facts['real_execution_contract'],
                         'quality_diagnostic_contract_mismatch')
                routes = request['real_routes']
                _require(set(routes) == {'valid', 'rejected'}
                         and len({route['command_id'] for route in routes.values()}) == 2,
                         'quality_diagnostic_routes_mismatch')
                quality = db.get(QualityRun, run.id)
                expected_quality = {'plan': prep.frozen['plan'],
                    'primary_key_fields': contract['source_primary_key'],
                    'route_commands': {name: route['command_id'] for name, route in routes.items()},
                    'stage_version': contract['quality_stage_version'],
                    'cleaning': contract['cleaning'], 'filters': contract['filters']}
                _require(quality is not None and quality.contract == expected_quality
                         and quality.contract_sha256 == _sha(expected_quality) == request['quality_contract_sha256'],
                         'quality_diagnostic_quality_binding_mismatch')
                if quality.finalized:
                    return quality.report
                control, supervision = db.get(RunControl, command_id), db.get(RuntimeSupervision, command_id)
                ingress = db.get(IngressBinding, command_id)
                expected_ingress = {'command_id': command_id, 'run_id': run.id,
                    'engine_job_id': attempt.engine_job_id, 'route_commands': expected_quality['route_commands'],
                    'quality_contract_sha256': quality.contract_sha256,
                    'source_primary_key': contract['source_primary_key']}
                if (control is None or supervision is None or ingress is None or not ingress.closed
                        or ingress.facts != expected_ingress or run.engine_job_id != attempt.engine_job_id):
                    return None
                termination = control.facts.get('termination', {})
                if not (termination.get('confirmed') is True and termination.get('basis') == 'engine_terminal'
                        and termination.get('engine_state') in ('FINISHED', 'CANCELED', 'FAILED')
                        and termination.get('engine_job_id') == attempt.engine_job_id
                        and termination.get('job_name') == job_name(command_id)):
                    return None
                loads, epochs = {}, []
                for binding in _load_bindings(request, payload, command.envelope):
                    row = db.get(LoadRun, binding.command_id)
                    expected_binding = json.loads(json.dumps(asdict(binding)))
                    _require(row is not None and row.binding == expected_binding
                             and row.binding_sha256 == _sha(expected_binding),
                             'quality_diagnostic_load_binding_mismatch')
                    if not row.closed:
                        return None
                    loads[row.command_id] = row
                    epochs.append(row.counter_epoch)
                epoch = attempt.engine_job_id + ':' + hashlib.sha256(
                    json.dumps(epochs, separators=(',', ':')).encode()).hexdigest()
                final, sample, source_read = control.facts.get('final_metrics', {}), supervision.last_trusted, None
                if (final.get('status') == 'known' and isinstance(sample, dict) and final.get('sample') == sample
                        and sample.get('state') == 'finished' and sample.get('counter_epoch') == epoch
                        and sample.get('source') == 'seatunnel-2.3.12-source+managed-doris-journal-v1'
                        and supervision.contract == payload['runtime_supervision_contract']):
                    # Evaluate at the persisted observation time: age now cannot
                    # erase a trusted final count or make an unknown count known.
                    try:
                        decision = evaluate_observation(supervision.contract, sample, None,
                            datetime.fromisoformat(sample['observed_at']))
                        if (decision['metrics'] == supervision.current_metrics == final.get('metrics')
                                and type(decision['metrics'].get('source_read')) is int):
                            source_read = decision['metrics']['source_read']
                    except (KeyError, TypeError, ValueError):
                        # A damaged final sample makes source usage unknown; it
                        # must not suppress the actual quality-side diagnostics.
                        pass
                records = list(db.scalars(select(QualityRecord).where(QualityRecord.run_id == run.id)))
                batches = list(db.scalars(select(LoadBatch).where(LoadBatch.command_id.in_(loads))))
                by_label = {batch.label: batch for batch in batches}
                links = {}
                for record_row in records:
                    label = ingress.links.get(record_row.stable_key)
                    batch = by_label.get(label)
                    owner = loads.get(batch.command_id) if batch else None
                    receipt = (batch.receipt or {}) if batch else {}
                    if (owner is not None and batch.command_id == expected_quality['route_commands'][record_row.route]
                            and batch.body_sha256 == record_row.route_body_sha256 and batch.phase == 'receipt'
                            and _digest(batch.response_sha256) and batch.receipt_at is not None
                            and batch.backend_id in owner.binding['backend_ids']
                            and receipt.get('Label') == label and receipt.get('Status') == 'Success'
                            and type(receipt.get('TxnId')) is int and receipt['TxnId'] > 0
                            and type(receipt.get('LoadBytes')) is int and receipt['LoadBytes'] >= 0
                            and receipt.get('TwoPhaseCommit') == 'false'
                            and all(type(receipt.get(key)) is int for key in
                                    ('NumberLoadedRows', 'NumberFilteredRows', 'NumberUnselectedRows'))
                            and receipt['NumberLoadedRows'] == 1 and receipt['NumberFilteredRows'] == 0
                            and receipt['NumberUnselectedRows'] == 0):
                        links[record_row.stable_key] = label
                receipts_complete = (len(links) == len(records) == len(batches)
                    and len(set(links.values())) == len(links) and links == ingress.links)
                failures = list(quality.failures)
                if supervision.publication_veto or supervision.stop_request or supervision.reasons:
                    failures.append('real_supervision_veto')
                if ingress.failures:
                    failures.append('real_ingress_veto')
                if not receipts_complete:
                    failures.append('real_original_receipt_unverified')
                complete = (termination['engine_state'] == 'FINISHED' and source_read == len(records) == len(links)
                    and source_read is not None and receipts_complete and 'source_parse_failure' not in failures)
                quality.source_facts = {'source_read': source_read, 'parsed': len(records), 'complete': complete}
                quality.failures = list(dict.fromkeys(failures))
                run_id = run.id
            # report() uses its own transaction. Never hold the tenant/quality
            # row lock across it; the ingress barrier remains held until commit.
            return QualityRuntime(sessions).report(run_id, links)
    except (IngressDenied, KeyError, TypeError, ValueError):
        return None
