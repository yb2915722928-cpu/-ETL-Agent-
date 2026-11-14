"""Frozen, durable quality decisions; no engine/HTTP or real-L2 qualification.

Callers authorize/freeze the entire EtlPlan before bind. Cleaning and filters
come from that plan, independently of the mandatory target-field rules.
Only digests, field names and finite codes persist; raw/cleaned records stay in
caller memory. Source progress must come from an independently trusted source
reader, never from adding sink counts. Receipt links must be supplied by trusted
routing code after ManagedLoad, not exposed as user-supplied evidence.
"""
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import math

from sqlalchemy import Boolean, DateTime, ForeignKey, JSON, String, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base, Session
from .managed_load import LoadBatch, LoadRun


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def _sha(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def route_body(result, route):
    """Exact pure Doris JSON-line bytes; no metadata wrapper for valid rows."""
    if result.get('route') != route or route not in ('valid', 'rejected'):
        raise ValueError('quality_route_mismatch')
    payload = result['payload'] if route == 'valid' else result['error_record']
    return (_json(payload) + '\n').encode()


class QualityRun(Base):
    __tablename__ = 'quality_runs'
    run_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    contract: Mapped[dict] = mapped_column(JSON)
    contract_sha256: Mapped[str] = mapped_column(String(64))
    source_facts: Mapped[dict] = mapped_column(JSON, default=dict)
    failures: Mapped[list] = mapped_column(JSON, default=list)
    finalized: Mapped[bool] = mapped_column(Boolean, default=False)
    report: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class QualityRecord(Base):
    __tablename__ = 'quality_records'
    run_id: Mapped[str] = mapped_column(ForeignKey('quality_runs.run_id'), primary_key=True)
    stable_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    body_sha256: Mapped[str] = mapped_column(String(64))
    stage_sha256: Mapped[str] = mapped_column(String(64))
    route_body_sha256: Mapped[str] = mapped_column(String(64))
    route: Mapped[str] = mapped_column(String(16))
    errors: Mapped[list] = mapped_column(JSON)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class QualityRuntime:
    def __init__(self, sessions=Session):
        self.sessions = sessions

    def bind(self, run_id, plan, primary_key_fields, route_commands, *,
             stage_version='quality-v1', cleaning=None, filters=None):
        """route_commands must bind valid/rejected to distinct managed-load handles.

        Cleaning uses mapped target fields: {field, kind:'trim'}. Filtering uses
        original source fields: {field, equals: scalar}; matches get FILTERED,
        never a silent drop. Mandatory plan.quality rules check cleaned targets.
        """
        if hasattr(plan, 'model_dump'):
            plan = plan.model_dump()
        approved_cleaning, approved_filters = plan.get('cleaning', []), plan.get('filters', [])
        if ((cleaning is not None and list(cleaning) != approved_cleaning)
                or (filters is not None and list(filters) != approved_filters)):
            raise ValueError('quality_configuration_not_approved')
        cleaning, filters = approved_cleaning, approved_filters
        mappings, rules = plan['mappings'], plan.get('quality', [])
        targets = [item['target'] for item in mappings]
        if (not primary_key_fields or len(set(primary_key_fields)) != len(primary_key_fields)
                or not all(isinstance(x, str) and x for x in primary_key_fields)
                or not mappings or len(set(targets)) != len(targets)
                or any(item.get('operation', 'direct') != 'direct' for item in mappings)
                or set(route_commands) != {'valid', 'rejected'}
                or len(set(route_commands.values())) != 2):
            raise ValueError('unsupported_quality_contract')
        for rule in rules:
            if rule['field'] not in targets or rule['kind'] not in ('not_null', 'numeric_range'):
                raise ValueError('unsupported_quality_rule')
            if rule['kind'] == 'numeric_range':
                for key in ('minimum', 'maximum'):
                    value = rule.get(key)
                    if value is not None and (type(value) not in (int, float) or not math.isfinite(value)):
                        raise ValueError('invalid_quality_range')
                if (rule.get('minimum') is not None and rule.get('maximum') is not None
                        and rule['minimum'] > rule['maximum']):
                    raise ValueError('invalid_quality_range')
        mapped_sources = {item['target']: item['source'] for item in mappings}
        if any(set(item) != {'field', 'kind'} or item['kind'] != 'trim' or item['field'] not in targets
               or mapped_sources[item['field']] in primary_key_fields for item in cleaning):
            raise ValueError('unsupported_cleaning')
        if any(set(item) != {'field', 'equals'} or not isinstance(item['field'], str) or not item['field']
               or type(item['equals']) not in (str, int, type(None)) for item in filters):
            raise ValueError('unsupported_filter')
        contract = {'plan': plan, 'primary_key_fields': list(primary_key_fields),
                    'route_commands': dict(route_commands), 'stage_version': stage_version,
                    'cleaning': list(cleaning), 'filters': list(filters)}
        digest = _sha(contract)
        with self.sessions.begin() as db:
            db.execute(insert(QualityRun).values(run_id=run_id, contract=contract,
                contract_sha256=digest, source_facts={}, failures=[], finalized=False)
                .on_conflict_do_nothing(index_elements=['run_id']))
            row = self._run(db, run_id)
            if row.contract_sha256 != digest:
                raise ValueError('quality_contract_conflict')
        return digest

    def judge(self, run_id, record):
        with self.sessions.begin() as db:
            run = self._run(db, run_id)
            if run.finalized or run.failures:
                raise ValueError('quality_run_closed')
            contract = run.contract
            if not isinstance(record, dict) or any(
                    type(record.get(key)) not in (str, int, float, bool) for key in contract['primary_key_fields']):
                run.failures = [*run.failures, 'source_parse_failure']
                return {'route': None, 'errors': [{'code': 'SOURCE_PARSE_FAILURE'}], 'publication_veto': True}
            try:
                identity = [[key, type(record[key]).__name__, record[key]] for key in contract['primary_key_fields']]
                stable_key, body_digest = _sha(identity), _sha(record)
                mapped = {item['target']: record[item['source']] for item in contract['plan']['mappings']}
                filter_values = {condition['field']: record[condition['field']] for condition in contract['filters']}
                if any(type(value) not in (str, int, type(None)) for value in filter_values.values()):
                    raise ValueError('source_filter_value_unparseable')
                # Validate the complete record before deciding or persisting hashes.
                _json(mapped)
            except (KeyError, ValueError, TypeError):
                run.failures = [*run.failures, 'source_parse_failure']
                return {'route': None, 'errors': [{'code': 'SOURCE_PARSE_FAILURE'}], 'publication_veto': True}
            for operation in contract['cleaning']:
                value = mapped[operation['field']]
                if isinstance(value, str):
                    mapped[operation['field']] = value.strip()
            errors = []
            for rule in contract['plan'].get('quality', []):
                value, field = mapped[rule['field']], rule['field']
                if rule['kind'] == 'not_null' and value is None:
                    errors.append({'field': field, 'code': 'NOT_NULL'})
                if rule['kind'] == 'numeric_range':
                    code = None
                    if value is None:
                        code = 'RANGE_NULL'
                    elif type(value) not in (int, float):
                        code = 'RANGE_TYPE'
                    else:
                        number = Decimal(str(value))
                        if ((rule.get('minimum') is not None and number < Decimal(str(rule['minimum'])))
                                or (rule.get('maximum') is not None and number > Decimal(str(rule['maximum'])))):
                            code = 'OUT_OF_RANGE'
                    if code:
                        errors.append({'field': field, 'code': code})
            for condition in contract['filters']:
                # Preserve type semantics: True is not the numeric value 1.
                if _json(filter_values[condition['field']]) == _json(condition['equals']):
                    errors.append({'field': condition['field'], 'code': 'FILTERED'})
            errors = [json.loads(value) for value in sorted({_json(error) for error in errors})]
            route = 'rejected' if errors else 'valid'
            stage_digest = _sha({'contract_sha256': run.contract_sha256, 'mapped': mapped})
            error_record = {'record_key': stable_key, 'error_codes': _json(sorted({e['code'] for e in errors})),
                            'field_names': _json(sorted({e['field'] for e in errors})),
                            'record_sha256': body_digest, 'run_id': run_id,
                            'contract_sha256': run.contract_sha256}
            route_payload = mapped if route == 'valid' else error_record
            route_digest = hashlib.sha256((_json(route_payload) + '\n').encode()).hexdigest()
            previous = db.get(QualityRecord, (run_id, stable_key))
            if previous and (previous.body_sha256 != body_digest or previous.stage_sha256 != stage_digest):
                run.failures = [*run.failures, 'record_payload_conflict']
                return {'stable_key': stable_key, 'route': None, 'errors': [{'code': 'PAYLOAD_CONFLICT'}],
                        'publication_veto': True}
            if previous is None:
                db.add(QualityRecord(run_id=run_id, stable_key=stable_key, body_sha256=body_digest,
                    stage_sha256=stage_digest, route_body_sha256=route_digest, route=route, errors=errors,
                    decided_at=db.scalar(select(func.clock_timestamp()))))
            return {'run_id': run_id, 'contract_sha256': run.contract_sha256, 'stable_key': stable_key,
                    'stage_sha256': stage_digest, 'route': route, 'errors': errors,
                    'payload': mapped if route == 'valid' else None,
                    'error_record': error_record if errors else None}

    def source_progress(self, run_id, *, source_read, parsed, complete, parse_failed=False):
        if (type(source_read) is not int or type(parsed) is not int or not 0 <= parsed <= source_read
                or type(complete) is not bool or type(parse_failed) is not bool):
            raise ValueError('invalid_source_progress')
        with self.sessions.begin() as db:
            run = self._run(db, run_id)
            if run.finalized:
                raise ValueError('quality_run_closed')
            previous = run.source_facts
            if previous and (source_read < previous['source_read'] or parsed < previous['parsed']
                             or previous['complete'] and (not complete or source_read != previous['source_read']
                                                          or parsed != previous['parsed'])):
                run.failures = [*run.failures, 'source_counter_reset']
            if parse_failed:
                run.failures = list(dict.fromkeys([*run.failures, 'source_parse_failure']))
            run.source_facts = {'source_read': source_read, 'parsed': parsed, 'complete': complete}

    def report(self, run_id, receipt_links):
        """Finalize once. links: {stable_key: actual ManagedLoad batch label}.

        Each one-record route must have exactly one loaded Doris row. Neither
        supplied counts nor engine FINISHED authorize reconciliation. Missing,
        duplicate, cross-run/route/contract or unknown receipts permanently veto.
        """
        with self.sessions.begin() as db:
            run = self._run(db, run_id)
            if run.finalized:
                return run.report
            records = list(db.scalars(select(QualityRecord).where(QualityRecord.run_id == run_id)))
            reasons = list(run.failures)
            source = run.source_facts
            if not source or not source['complete']:
                reasons.append('source_report_incomplete')
            if source and (source['parsed'] != len(records) or source['source_read'] != source['parsed']):
                reasons.append('source_parse_or_evaluation_incomplete')
            if set(receipt_links) != {record.stable_key for record in records}:
                reasons.append('route_receipts_incomplete')
            if len(set(receipt_links.values())) != len(receipt_links):
                reasons.append('route_receipt_reused')
            written = {'valid': 0, 'rejected': 0}
            for record in records:
                label = receipt_links.get(record.stable_key)
                batch = db.get(LoadBatch, label) if isinstance(label, str) else None
                owner = db.get(LoadRun, batch.command_id) if batch else None
                expected = run.contract['route_commands'][record.route]
                if (not batch or not owner or batch.command_id != expected or owner.binding['run_id'] != run_id
                        or owner.binding.get('quality_contract_sha256') != run.contract_sha256
                        or batch.body_sha256 != record.route_body_sha256
                        or not owner.closed or batch.phase != 'receipt'
                        or batch.receipt.get('Status') != 'Success'
                        or batch.receipt.get('NumberLoadedRows') != 1
                        or batch.receipt.get('NumberFilteredRows') != 0
                        or batch.receipt.get('NumberUnselectedRows') != 0):
                    reasons.append('route_receipt_unverified')
                else:
                    written[record.route] += batch.receipt['NumberLoadedRows']
            # Any extra or unresolved batch on either route invalidates the report.
            for command_id in run.contract['route_commands'].values():
                owner = db.get(LoadRun, command_id)
                if (not owner or not owner.closed or owner.binding.get('run_id') != run_id
                        or owner.binding.get('quality_contract_sha256') != run.contract_sha256):
                    reasons.append('route_not_closed_or_bound')
                if owner and owner.close_id in ('supervision-incomplete', 'supervision-stale'):
                    reasons.append('route_supervision_veto')
                batches = list(db.scalars(select(LoadBatch).where(LoadBatch.command_id == command_id)))
                if any(batch.label not in receipt_links.values() for batch in batches):
                    reasons.append('route_extra_or_failed_batch')
            counts = {route: sum(record.route == route for record in records) for route in written}
            hits = {}
            for record in records:
                for error in record.errors:
                    key = error['field'] + ':' + error['code']
                    hits[key] = hits.get(key, 0) + 1
            run.finalized = True
            run.report = {'run_id': run_id, 'contract_sha256': run.contract_sha256,
                'source_read': source.get('source_read'), 'parsed': source.get('parsed'),
                'evaluated': len(records), 'valid': counts['valid'], 'rejected': counts['rejected'],
                'final_written': written, 'rule_hits': hits,
                'publication_veto': bool(reasons), 'reasons': sorted(set(reasons)),
                'real_l2_qualified': False}
            return run.report

    @staticmethod
    def _run(db, run_id):
        run = db.scalar(select(QualityRun).where(QualityRun.run_id == run_id).with_for_update())
        if run is None:
            raise ValueError('quality_run_missing')
        return run
