"""Internal-only JDBC -> HTTP quality/load ingress. NOT real-L2 qualification.

Deployment must isolate this app and prove the sole Doris write route. Caller
validates signed commands/roles/secrets and registers these models before DB
creation. Tokens and record bodies never persist here. No public router is added.
"""
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import hmac
import json
from time import monotonic, sleep

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import Boolean, DateTime, JSON, String, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base, Session
from .managed_load import BatchKey, LoadBatch, LoadRun, ManagedLoad
from .quality_runtime import QualityRecord, QualityRun, QualityRuntime, route_body
from .real_contract import REAL_ADMISSION
from .supervision_runtime import RuntimeSupervision


@dataclass(frozen=True)
class IngressBindingFacts:
    command_id: str
    run_id: str
    engine_job_id: str
    route_commands: dict
    quality_contract_sha256: str
    source_primary_key: tuple[str, ...]


class IngressBinding(Base):
    __tablename__ = 'real_ingress_bindings'
    command_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    facts: Mapped[dict] = mapped_column(JSON)
    token_sha256: Mapped[str] = mapped_column(String(64))
    closed: Mapped[bool] = mapped_column(Boolean, default=False)
    failures: Mapped[list] = mapped_column(JSON, default=list)
    links: Mapped[dict] = mapped_column(JSON, default=dict)
    # NULL until an actual complete observer barrier commits; bind is not one.
    last_complete_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class IngressDenied(Exception):
    def __init__(self, code, status=409):
        self.code, self.status = code, status
        super().__init__(code)


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate_json_key')
        result[key] = value
    return result


def _row(raw):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= 65536:
        raise ValueError('invalid_row_size')
    value = json.loads(raw, object_pairs_hook=_pairs,
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite_json')))
    if not isinstance(value, dict) or not value:
        raise ValueError('single_object_required')
    return value


class IngressService:
    def __init__(self, load_factory, authorize, sessions=Session):
        self.sessions, self.load_factory, self.authorize = sessions, load_factory, authorize
        self.quality = QualityRuntime(sessions)

    def bind(self, binding: IngressBindingFacts, token_sha256: str):
        facts = json.loads(json.dumps(asdict(binding)))
        if (set(binding.route_commands) != {'valid', 'rejected'}
                or len(set(binding.route_commands.values())) != 2
                or not binding.source_primary_key
                or not isinstance(token_sha256, str) or len(token_sha256) != 64
                or any(x not in '0123456789abcdef' for x in token_sha256)):
            raise ValueError('invalid_ingress_binding')
        with self.sessions.begin() as db:
            quality = db.get(QualityRun, binding.run_id)
            if (not quality or quality.contract_sha256 != binding.quality_contract_sha256
                    or quality.contract['route_commands'] != binding.route_commands
                    or quality.contract['primary_key_fields'] != list(binding.source_primary_key)):
                raise ValueError('ingress_quality_binding_mismatch')
            db.execute(insert(IngressBinding).values(command_id=binding.command_id, facts=facts,
                token_sha256=token_sha256, closed=False, failures=[], links={})
                .on_conflict_do_nothing(index_elements=['command_id']))
            row = db.get(IngressBinding, binding.command_id)
            if row.facts != facts or row.token_sha256 != token_sha256:
                raise ValueError('ingress_binding_conflict')
        return binding.command_id

    @contextmanager
    def _serial(self, handle):
        # Session-level advisory lock spans independent quality/journal commits.
        # It serializes post and observe, not already reserved external writes.
        with self.sessions() as session, session.get_bind().connect() as connection:
            key = int.from_bytes(hashlib.sha256(('ingress:' + handle).encode()).digest()[:8], 'big', signed=True)
            acquired = connection.scalar(text('SELECT pg_try_advisory_lock(:key)'), {'key': key})
            connection.commit()
            if not acquired:
                raise IngressDenied('ingress_busy', 503)
            try:
                yield
            finally:
                connection.execute(text('SELECT pg_advisory_unlock(:key)'), {'key': key})
                connection.commit()

    def _binding(self, handle, token=None):
        with self.sessions() as db:
            row = db.get(IngressBinding, handle)
            if row is None:
                raise IngressDenied('ingress_not_found', 404)
            if token is not None:
                if not isinstance(token, str) or not hmac.compare_digest(
                        hashlib.sha256(token.encode()).hexdigest(), row.token_sha256):
                    raise IngressDenied('ingress_unauthorized', 403)
            return row

    def _trusted_ready(self, db, binding, now):
        # A journal barrier alone is not source supervision. Only the controller's
        # committed original-command sample opens the same short admission window.
        supervision = db.get(RuntimeSupervision, binding.command_id)
        if (supervision is None or supervision.stop_request or supervision.status != 'observing'
                or not isinstance(supervision.last_trusted, dict)):
            return False
        sample = supervision.last_trusted
        try:
            observed = datetime.fromisoformat(sample['observed_at'])
            return (sample['state'] == 'running' and sample['byte_kind'] == 'measured'
                    and sample['counter_epoch'].startswith(binding.facts['engine_job_id'] + ':')
                    and 0 <= (now - observed).total_seconds() <= REAL_ADMISSION['window_ms'] / 1000)
        except (KeyError, TypeError, ValueError, AttributeError):
            return False

    def _admission_window(self, binding):
        # Aggregate without joining runs to batches: a join would multiply counts
        # and could hide an extra/unknown batch. Keep original binding identity.
        routes = tuple(binding.facts['route_commands'].values())
        runs = (select(func.count(), func.min(LoadRun.last_observed_at),
                       func.max(LoadRun.last_observed_at),
                       func.count().filter(or_(LoadRun.closed.is_(True), LoadRun.last_observed_at.is_(None))))
                .where(LoadRun.command_id.in_(routes)).subquery())
        bad_batches = select(func.count()).where(LoadBatch.command_id.in_(routes), or_(
            func.coalesce(LoadBatch.phase, '') != 'receipt',
            func.coalesce(LoadBatch.receipt['Status'].as_string(), '') != 'Success')).scalar_subquery()
        with self.sessions() as db:
            result = db.execute(select(IngressBinding, RuntimeSupervision, *runs.c, bad_batches)
                .outerjoin(RuntimeSupervision, RuntimeSupervision.command_id == IngressBinding.command_id)
                .join(runs, text('true'))
                .where(IngressBinding.command_id == binding.command_id)).first()
            if result is None:
                return False
            current, supervision, count, earliest, latest, bad_runs, bad = result
            if (current.closed or current.facts != binding.facts
                    or current.token_sha256 != binding.token_sha256
                    or current.last_complete_observed_at is None or count != 2 or bad_runs or bad):
                return False
            # Sample after the aggregate finishes, never before its scans. The
            # selected supervision is already in this session's identity map.
            now = db.scalar(select(func.clock_timestamp()))
            return (self._trusted_ready(db, binding, now) and
                    all(stamp is not None and 0 <= (now - stamp).total_seconds() <= REAL_ADMISSION['window_ms'] / 1000
                        for stamp in (current.last_complete_observed_at, earliest, latest)))

    def post(self, handle, token, raw):
        # Authenticate before waiting/parsing/closing so an invalid token cannot
        # deliberately veto someone else's run or consume its observation slot.
        if token is None:
            raise IngressDenied('ingress_unauthorized', 403)
        original = self._binding(handle, token)
        if original.closed:
            raise IngressDenied('ingress_closed')
        deadline = monotonic() + REAL_ADMISSION['max_wait_ms'] / 1000
        # Full preparation/schema/writer preflight belongs before awaiting the
        # short window, outside its barrier. It grants no later write authority:
        # _post and the transport still resolve current authorization again.
        try:
            self.authorize(original.facts)
        except Exception:
            self.close(handle, 'ingress_record_or_load_failed')
            raise IngressDenied('ingress_record_or_load_failed') from None
        while True:
            binding = self._binding(handle, token)
            if binding.closed:
                raise IngressDenied('ingress_closed')
            if binding.facts != original.facts or binding.token_sha256 != original.token_sha256:
                self.close(handle, 'ingress_record_or_load_failed')
                raise IngressDenied('ingress_record_or_load_failed')
            if monotonic() < deadline and self._admission_window(binding):
                try:
                    with self._serial(handle):
                        # One final aggregate + fresh PG time under the barrier,
                        # immediately before judge, without heavyweight preflight.
                        if monotonic() < deadline and self._admission_window(binding):
                            return self._post(binding, raw)
                except IngressDenied as error:
                    if error.code != 'ingress_busy':
                        raise
            remaining = deadline - monotonic()
            if remaining <= 0:
                self._binding(handle, token)
                self.close(handle, 'ingress_admission_timeout')
                raise IngressDenied('ingress_admission_timeout', 503)
            sleep(min(REAL_ADMISSION['poll_ms'] / 1000, remaining))

    def _post(self, binding, raw):
        # Called only under the barrier, after current authorization and the
        # final admission-window check. From judge through links there is no wait
        # or retry: an observer collision still permanently closes this attempt.
        handle = binding.command_id
        try:
            record = _row(raw)
            decision = self.quality.judge(binding.facts['run_id'], record)
            route = decision.get('route')
            if route not in ('valid', 'rejected'):
                raise ValueError('quality_failed')
            stable_key = decision['stable_key']
            body = route_body(decision, route)
            sequence = int(stable_key[:16], 16) & ((1 << 63) - 1)
            with self.sessions() as db:
                keys = db.scalars(select(QualityRecord.stable_key).where(
                    QualityRecord.run_id == binding.facts['run_id']))
                if any(key != stable_key and (int(key[:16], 16) & ((1 << 63) - 1)) == sequence for key in keys):
                    raise ValueError('stable_key_collision')
            # Reauthorize synchronously immediately before the journal send.
            # Transport must also independently enforce frozen authorization.
            self.authorize(binding.facts)
            current = self._binding(handle)
            if (current.closed or current.facts != binding.facts
                    or current.token_sha256 != binding.token_sha256):
                raise ValueError('admission_changed_before_send')
            loads = self.load_factory(binding.facts)
            sequence = int(stable_key[:16], 16) & ((1 << 63) - 1)
            result = loads.send(binding.facts['route_commands'][route], BatchKey(0, sequence), body)
            receipt = result.get('receipt') or {}
            if (result['phase'] != 'receipt' or receipt.get('Status') != 'Success'
                    or receipt.get('NumberLoadedRows') != 1
                    or receipt.get('NumberFilteredRows') != 0
                    or receipt.get('NumberUnselectedRows') != 0):
                raise ValueError('load_unconfirmed')
            with self.sessions.begin() as db:
                row = db.scalar(select(IngressBinding).where(IngressBinding.command_id == handle).with_for_update())
                links = dict(row.links)
                if stable_key in links and links[stable_key] != result['label']:
                    raise ValueError('route_link_conflict')
                links[stable_key] = result['label']
                row.links = links
                closed_during_send = row.closed
            if closed_during_send:
                raise ValueError('admission_closed_during_send')
            return {'accepted': True, 'stable_key': stable_key, 'route': route,
                    'real_l2_qualified': False}
        except Exception:
            self._close_locked(handle, 'ingress_record_or_load_failed')
            raise IngressDenied('ingress_record_or_load_failed') from None

    def _close_locked(self, handle, reason='ingress_closed'):
        # Close ingress and both routes atomically. A send reservation takes the
        # same LoadRun row lock: close-first prevents new transport I/O; a prior
        # committed reservation remains visible as unresolved, never quiescent.
        with self.sessions.begin() as db:
            row = db.scalar(select(IngressBinding).where(IngressBinding.command_id == handle).with_for_update())
            if row is None:
                raise IngressDenied('ingress_not_found', 404)
            facts = row.facts
            runs = list(db.scalars(select(LoadRun).where(
                LoadRun.command_id.in_(facts['route_commands'].values())
            ).order_by(LoadRun.command_id).with_for_update()))
            if len(runs) != 2:
                raise IngressDenied('ingress_load_binding_missing')
            row.closed = True
            # Finite reason codes only, never caller exception text.
            code = reason if reason in ('ingress_closed', 'ingress_record_or_load_failed',
                                        'ingress_observation_incomplete', 'ingress_admission_timeout',
                                        'engine_finished') else 'ingress_closed'
            if code != 'engine_finished':
                row.failures = list(dict.fromkeys([*row.failures, code]))
            for run in runs:
                run.closed = True
                if run.close_id is None:
                    run.close_id = code
            snapshots = [ManagedLoad._snapshot(db, run) for run in runs]
        return {'admission_closed': True,
                'load_quiescent': all(sample['load_quiescent'] for sample in snapshots),
                'real_l2_qualified': False}

    def close(self, handle, reason='ingress_closed'):
        return self._close_locked(handle, reason)

    def observe(self, handle):
        try:
            with self._serial(handle):
                sample = self._observe(handle)
                if sample['complete']:
                    with self.sessions.begin() as db:
                        row = db.scalar(select(IngressBinding).where(
                            IngressBinding.command_id == handle).with_for_update())
                        if not row.closed:
                            row.last_complete_observed_at = db.scalar(select(func.clock_timestamp()))
                return sample
        except IngressDenied as error:
            if error.code != 'ingress_busy':
                raise
            # A POST spans this observation barrier. Stop admission immediately
            # rather than wait for an unbounded active load or fabricate bytes.
            self._close_locked(handle, 'ingress_observation_incomplete')
            sample = self._observe(handle)
            sample.update(complete=False, sink_bytes=None, byte_kind='unavailable')
            return sample

    def _observe(self, handle):
        binding = self._binding(handle)
        loads = self.load_factory(binding.facts)
        samples = [loads.observe(binding.facts['route_commands'][route]) for route in ('valid', 'rejected')]
        complete = all(sample['complete'] for sample in samples)
        if not complete or any(sample['close_id'] in ('supervision-incomplete', 'supervision-stale') for sample in samples):
            self._close_locked(handle, 'ingress_observation_incomplete')
            binding = self._binding(handle)
        written = 0
        receipts_verified = True
        with self.sessions() as db:
            records = list(db.scalars(select(QualityRecord).where(QualityRecord.run_id == binding.facts['run_id'])))
            by_key = {record.stable_key: record for record in records}
            for key, label in binding.links.items():
                record = by_key.get(key)
                batch = db.get(LoadBatch, label)
                owner = db.get(LoadRun, batch.command_id) if batch else None
                receipt = batch.receipt if batch and batch.receipt else {}
                if (not record or not batch or not owner
                        or batch.command_id != binding.facts['route_commands'][record.route]
                        or owner.binding.get('run_id') != binding.facts['run_id']
                        or owner.binding.get('quality_contract_sha256') != binding.facts['quality_contract_sha256']
                        or batch.body_sha256 != record.route_body_sha256
                        or batch.phase != 'receipt' or receipt.get('Status') != 'Success'
                        or receipt.get('NumberLoadedRows') != 1 or receipt.get('NumberFilteredRows') != 0
                        or receipt.get('NumberUnselectedRows') != 0):
                    receipts_verified = False
                else:
                    written += receipt['NumberLoadedRows']
            if (set(binding.links) != set(by_key)
                    or len(set(binding.links.values())) != len(binding.links)):
                receipts_verified = False
            for handle_id in binding.facts['route_commands'].values():
                batches = list(db.scalars(select(LoadBatch).where(LoadBatch.command_id == handle_id)))
                if any(batch.label not in binding.links.values() for batch in batches):
                    receipts_verified = False
        if not receipts_verified:
            complete = False
            self._close_locked(handle, 'ingress_observation_incomplete')
            binding = self._binding(handle)
        epochs = [sample['counter_epoch'] for sample in samples]
        counter_epoch = hashlib.sha256(json.dumps(epochs, separators=(',', ':')).encode()).hexdigest()
        return {'observed_at': min(sample['observed_at'] for sample in samples),
                'counter_epoch': counter_epoch, 'complete': complete,
                'byte_kind': 'measured' if complete else 'unavailable',
                'sink_bytes': sum(sample['sink_bytes'] for sample in samples) if complete else None,
                'source_read': None, 'parsed': None, 'evaluated': len(records),
                'valid': sum(row.route == 'valid' for row in records),
                'rejected': sum(row.route == 'rejected' for row in records), 'final_output': written,
                'admission_closed': binding.closed, 'failures': binding.failures,
                'load_quiescent': binding.closed and complete and all(sample['load_quiescent'] for sample in samples),
                'route_samples': samples, 'real_l2_qualified': False}


def create_ingress_app(service: IngressService):
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.post('/ingress/{handle}')
    async def receive(handle: str, request: Request):
        # Stream cap avoids allocating an unbounded request before validation.
        raw = bytearray()
        try:
            async for chunk in request.stream():
                raw.extend(chunk)
                if len(raw) > 65536:
                    # Authenticated post closes on oversize using the same path.
                    break
            token = request.headers.get('x-etl-ingress-token')
            # Run synchronous PostgreSQL/HTTP outside the ASGI event loop.
            from starlette.concurrency import run_in_threadpool
            result = await run_in_threadpool(service.post, handle, token, bytes(raw))
            return JSONResponse(result)
        except IngressDenied as error:
            return JSONResponse({'accepted': False, 'code': error.code}, status_code=error.status)
        except Exception:
            return JSONResponse({'accepted': False, 'code': 'ingress_unavailable'}, status_code=503)

    return app
