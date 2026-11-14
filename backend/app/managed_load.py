"""Durable controlled-load journal, NOT a qualified real-L2 transport.

The caller verifies the signed ManagedCommand before bind. The injected Adapter
must own the only Doris write route, verify physical identities, follow FE
redirects internally, and keep credentials and request bodies in memory. This
module cannot establish those deployment facts. A DB lock/lease is not a fence.

At most one unresolved dispatch per run is allowed. Recovery NEVER resends a
DISPATCHING/UNKNOWN batch, even after a process or connection dies. close closes
admission, not an already reserved external request; quiescence requires its
original successful response. Unknown batches remain isolated permanently.
"""
from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
from typing import Protocol
from uuid import uuid4

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, JSON, String, UniqueConstraint, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base, Session


@dataclass(frozen=True)
class FrozenLoadBinding:
    command_id: str
    run_id: str
    engine_job_id: str
    command_sha256: str
    contract_sha256: str
    physical_cluster_id: str
    database: str
    table: str
    object_id: str
    writer_reference_id: str
    writer_version: int
    backend_ids: tuple[str, ...]
    quality_contract_sha256: str | None = None


@dataclass(frozen=True)
class BatchKey:
    partition: int
    sequence: int


@dataclass(frozen=True)
class TransportReceipt:
    raw: bytes
    physical_cluster_id: str
    database: str
    table: str
    object_id: str
    backend_id: str


class LoadAdapter(Protocol):
    def send(self, binding: FrozenLoadBinding, label: str, body: bytes) -> TransportReceipt:
        """Send ONCE, without HTTP retries; return the original Doris response.

        Identity fields attest the actual upstream connection and frozen object,
        not caller-provided headers. A fixture is not real identity evidence.
        """
        ...


class LoadRun(Base):
    __tablename__ = 'managed_load_runs'
    command_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    binding: Mapped[dict] = mapped_column(JSON)
    binding_sha256: Mapped[str] = mapped_column(String(64))
    counter_epoch: Mapped[str] = mapped_column(String(36))
    closed: Mapped[bool] = mapped_column(Boolean, default=False)
    close_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class LoadBatch(Base):
    __tablename__ = 'managed_load_batches'
    __table_args__ = (UniqueConstraint('command_id', 'partition', 'sequence'),)
    label: Mapped[str] = mapped_column(String(80), primary_key=True)
    command_id: Mapped[str] = mapped_column(ForeignKey('managed_load_runs.command_id'), index=True)
    partition: Mapped[int] = mapped_column(BigInteger)
    sequence: Mapped[int] = mapped_column(BigInteger)
    body_sha256: Mapped[str] = mapped_column(String(64))
    phase: Mapped[str] = mapped_column(String(24))
    intent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    receipt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    receipt: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    response_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    backend_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # Controlled reason codes only: exception messages may contain credentials.
    reason: Mapped[str | None] = mapped_column(String(64), nullable=True)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _digest(value):
    return hashlib.sha256(value).hexdigest()


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate_receipt_key')
        result[key] = value
    return result


def _parse_receipt(raw, label):
    if not isinstance(raw, bytes) or len(raw) > 65536:
        raise ValueError('receipt_size_or_type')
    value = json.loads(raw, object_pairs_hook=_pairs)
    if not isinstance(value, dict) or value.get('Label') != label:
        raise ValueError('receipt_label_mismatch')
    if value.get('Status') not in ('Success', 'Publish Timeout', 'Fail', 'Label Already Exists'):
        raise ValueError('receipt_status_invalid')
    for key in ('TxnId', 'LoadBytes'):
        if type(value.get(key)) is not int or value[key] < 0 or value[key] > 2 ** 63 - 1:
            raise ValueError('receipt_counter_invalid')
    if value.get('TwoPhaseCommit') != 'false' or value.get('GroupCommit') is True:
        raise ValueError('unsupported_transaction_mode')
    if value['Status'] in ('Success', 'Publish Timeout') and value['TxnId'] == 0:
        raise ValueError('receipt_transaction_invalid')
    result = {key: value[key] for key in ('Label', 'TxnId', 'Status', 'LoadBytes', 'TwoPhaseCommit')}
    for key in ('NumberTotalRows', 'NumberLoadedRows', 'NumberFilteredRows', 'NumberUnselectedRows'):
        if key in value:
            if type(value[key]) is not int or not 0 <= value[key] <= 2 ** 63 - 1:
                raise ValueError('receipt_rows_invalid')
            result[key] = value[key]
    # Do not persist Message, ErrorURL, Comment, auth headers, or arbitrary JSON.
    return result


class ManagedLoad:
    """Four-method Interface; sessions must target PostgreSQL.

    The bound epoch is durable. observe timestamps a new consistent journal
    read, not old receipts. It proves bytes only when no unresolved/failed load
    exists. It does NOT provide source/quality metrics or periodic scheduling.
    Runtime wiring must observe every 2s, veto incomplete samples immediately,
    enforce 6s freshness, and close admission when supervision is unavailable.
    """
    def __init__(self, adapter: LoadAdapter, sessions=Session):
        self.adapter, self.sessions = adapter, sessions

    def bind(self, binding: FrozenLoadBinding) -> str:
        facts = json.loads(_canonical(asdict(binding)))
        if (not all(isinstance(value, str) and value for key, value in facts.items()
                    if key not in ('writer_version', 'backend_ids', 'quality_contract_sha256'))
                or type(binding.writer_version) is not int or binding.writer_version < 1
                or not binding.backend_ids or not all(isinstance(x, str) and x for x in binding.backend_ids)):
            raise ValueError('invalid_frozen_binding')
        for key in ('command_sha256', 'contract_sha256', *(['quality_contract_sha256'] if binding.quality_contract_sha256 is not None else [])):
            value = facts[key]
            if len(value) != 64 or any(x not in '0123456789abcdef' for x in value):
                raise ValueError('invalid_binding_digest')
        digest = _digest(_canonical(facts))
        with self.sessions.begin() as db:
            now = db.scalar(select(func.clock_timestamp()))
            db.execute(insert(LoadRun).values(command_id=binding.command_id, binding=facts,
                binding_sha256=digest, counter_epoch=str(uuid4()), closed=False,
                created_at=now, last_observed_at=now)
                .on_conflict_do_nothing(index_elements=['command_id']))
            row = db.scalar(select(LoadRun).where(LoadRun.command_id == binding.command_id).with_for_update())
            if row.binding_sha256 != digest or row.binding != facts:
                raise ValueError('frozen_binding_conflict')
        return binding.command_id

    def send(self, handle: str, batch: BatchKey, body: bytes) -> dict:
        if (type(batch.partition) is not int or type(batch.sequence) is not int
                or not 0 <= batch.partition < 2 ** 63 or not 0 <= batch.sequence < 2 ** 63
                or not isinstance(body, bytes)):
            raise ValueError('invalid_batch')
        label = 'etl_' + _digest(_canonical([handle, batch.partition, batch.sequence]))
        body_digest = _digest(body)
        with self.sessions.begin() as db:
            run = self._run(db, handle)
            previous = db.get(LoadBatch, label)
            if previous:
                if previous.body_sha256 != body_digest:
                    raise ValueError('batch_payload_conflict')
                return self._outcome(previous)
            if run.closed:
                raise ValueError('admission_closed')
            now = db.scalar(select(func.clock_timestamp()))
            age = (now - run.last_observed_at).total_seconds()
            if age < 0 or age > 6:
                run.closed, run.close_id = True, 'supervision-stale'
                # Return through the transaction context so the closure commits.
                # Raising here would roll it back and accidentally reopen admission.
                return {'label': label, 'phase': 'denied', 'receipt': None,
                        'reason': 'supervision_stale'}
            existing = list(db.scalars(select(LoadBatch).where(LoadBatch.command_id == handle)))
            if any(row.phase != 'receipt' or row.receipt['Status'] != 'Success' for row in existing):
                raise ValueError('unsettled_batch_isolated')
            binding = FrozenLoadBinding(**{**run.binding, 'backend_ids': tuple(run.binding['backend_ids'])})
            db.add(LoadBatch(label=label, command_id=handle, partition=batch.partition, sequence=batch.sequence,
                body_sha256=body_digest, phase='dispatching', intent_at=db.scalar(select(func.clock_timestamp()))))
        # Commit happened before Adapter I/O. There is no takeover/replay path.
        # close can win now: this reserved request remains visible as unresolved.
        try:
            result = self.adapter.send(binding, label, body)
            if (result.physical_cluster_id, result.database, result.table, result.object_id) != (
                    binding.physical_cluster_id, binding.database, binding.table, binding.object_id
                    ) or result.backend_id not in binding.backend_ids:
                raise ValueError('transport_identity_mismatch')
            receipt = _parse_receipt(result.raw, label)
        except Exception:
            # Catch Adapter protocol/transport errors without persisting their text.
            # BaseException/process death leaves dispatching, also permanently isolated.
            with self.sessions.begin() as db:
                self._run(db, handle)
                row = db.get(LoadBatch, label)
                row.phase, row.reason = 'unknown', 'transport_or_receipt_unknown'
                return self._outcome(row)
        with self.sessions.begin() as db:
            self._run(db, handle)
            row = db.get(LoadBatch, label)
            row.phase, row.receipt = 'receipt', receipt
            row.receipt_at = db.scalar(select(func.clock_timestamp()))
            row.response_sha256, row.backend_id = _digest(result.raw), result.backend_id
            if receipt['Status'] != 'Success':
                row.reason = 'transaction_not_successfully_settled'
            return self._outcome(row)

    def observe(self, handle: str) -> dict:
        with self.sessions.begin() as db:
            run = self._run(db, handle)
            snapshot = self._snapshot(db, run)
            age = (datetime.fromisoformat(snapshot['observed_at']) - run.last_observed_at).total_seconds()
            if (age < 0 or age > 6) and not run.closed:
                run.closed, run.close_id = True, 'supervision-stale'
                snapshot.update(admission_closed=True, close_id=run.close_id,
                                load_quiescent=snapshot['complete'])
            if not snapshot['complete'] and not run.closed:
                run.closed, run.close_id = True, 'supervision-incomplete'
                snapshot.update(admission_closed=True, close_id=run.close_id)
            run.last_observed_at = datetime.fromisoformat(snapshot['observed_at'])
            snapshot['last_observed_at'] = run.last_observed_at.isoformat()
            return snapshot

    def close(self, handle: str, stop_id: str) -> dict:
        if not isinstance(stop_id, str) or not 0 < len(stop_id) <= 128:
            raise ValueError('invalid_stop_id')
        with self.sessions.begin() as db:
            run = self._run(db, handle)
            run.closed = True
            if run.close_id is None:
                run.close_id = stop_id
            return self._snapshot(db, run)

    @staticmethod
    def _run(db, handle):
        row = db.scalar(select(LoadRun).where(LoadRun.command_id == handle).with_for_update())
        if row is None:
            raise ValueError('load_run_missing')
        return row

    @staticmethod
    def _outcome(row):
        return {'label': row.label, 'phase': row.phase, 'receipt': row.receipt, 'reason': row.reason}

    @staticmethod
    def _snapshot(db, run):
        rows = list(db.scalars(select(LoadBatch).where(LoadBatch.command_id == run.command_id)))
        active = sum(row.phase == 'dispatching' for row in rows)
        unknown = sum(row.phase == 'unknown' for row in rows)
        unsettled = sum(row.phase == 'receipt' and row.receipt['Status'] != 'Success' for row in rows)
        known = sum(row.receipt['LoadBytes'] for row in rows
                    if row.phase == 'receipt' and row.receipt['Status'] != 'Label Already Exists')
        complete = not (active or unknown or unsettled)
        return {'source': 'managed_load_journal_v1', 'counter_epoch': run.counter_epoch,
            'observed_at': db.scalar(select(func.clock_timestamp())).isoformat(),
            'last_observed_at': run.last_observed_at.isoformat(),
            'complete': complete, 'byte_kind': 'measured' if complete else 'unavailable',
            'sink_bytes': known if complete else None, 'receipt_bytes': known,
            'active_count': active, 'unknown_count': unknown, 'unsettled_count': unsettled,
            'batch_count': len(rows), 'admission_closed': run.closed, 'close_id': run.close_id,
            # Load-route quiescence only. Caller must independently prove engine
            # termination, exclusive routing and current physical object identity.
            'load_quiescent': run.closed and complete,
            'real_l2_qualified': False}
