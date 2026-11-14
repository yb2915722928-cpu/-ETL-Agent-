"""Journal-only PostgreSQL fixtures. No Doris/SeaTunnel calls; NOT real L2.

Run with backend on PYTHONPATH and an authorized DATABASE_URL. Creates and drops
only a randomly named schema; never prints the URL, credentials or raw response.
"""
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
import json
from pathlib import Path
import sys
from uuid import uuid4

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from app.db import engine
from app.managed_load import BatchKey, FrozenLoadBinding, LoadAdapter, LoadBatch, LoadRun, ManagedLoad, TransportReceipt


@contextmanager
def journal_database():
    schema = 'managed_load_fixture_' + uuid4().hex
    isolated = create_engine(engine.url, pool_pre_ping=True,
                             connect_args={'options': f'-csearch_path={schema}'})
    try:
        with engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        for table in (LoadRun.__table__, LoadBatch.__table__):
            table.create(isolated)
        yield sessionmaker(isolated, expire_on_commit=False)
    finally:
        isolated.dispose()
        # schema is a locally generated fixed prefix plus UUID hex, not user SQL.
        with engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


class FixtureAdapter(LoadAdapter):
    def __init__(self, status='Success', failure=None, during_send=None):
        self.calls = 0
        self.status, self.failure, self.during_send = status, failure, during_send

    def send(self, binding, label, body):
        self.calls += 1
        if self.during_send:
            self.during_send()
        if self.failure:
            raise self.failure
        # Distinct from len(body): these bytes stand for an injected Doris
        # response, never request-length measurement.
        raw = json.dumps({'Label': label, 'TxnId': 41, 'Status': self.status,
                          'LoadBytes': 115, 'TwoPhaseCommit': 'false',
                          'Message': 'DO_NOT_PERSIST', 'ErrorURL': 'DO_NOT_PERSIST'}).encode()
        return TransportReceipt(raw, binding.physical_cluster_id, binding.database,
                                binding.table, binding.object_id, binding.backend_ids[0])


def binding():
    return FrozenLoadBinding(str(uuid4()), str(uuid4()), '123', 'a' * 64, 'b' * 64,
                             'fixture-cluster', 'fixture_db', 'shadow_fixture', 'table-identity-1',
                             'reference-only', 1, ('be-1',))


def rejected(call, reason):
    try:
        call()
    except ValueError as error:
        assert str(error) == reason, (str(error), reason)
    else:
        raise AssertionError('expected controlled rejection: ' + reason)


def verify(sessions):
    outcomes = []
    adapter = FixtureAdapter()
    module = ManagedLoad(adapter, sessions)
    frozen = binding()
    handle = module.bind(frozen)
    assert module.observe(handle)['sink_bytes'] == 0
    rejected(lambda: module.bind(replace(frozen, object_id='other-table')), 'frozen_binding_conflict')
    first = module.send(handle, BatchKey(0, 0), b'x')
    assert first['receipt']['LoadBytes'] == 115
    # Restarting the caller preserves both epoch and receipt deduplication.
    epoch = module.observe(handle)['counter_epoch']
    restarted = ManagedLoad(adapter, sessions)
    assert restarted.bind(frozen) == handle
    assert restarted.send(handle, BatchKey(0, 0), b'x') == first
    assert adapter.calls == 1
    rejected(lambda: restarted.send(handle, BatchKey(0, 0), b'different'), 'batch_payload_conflict')
    restarted.send(handle, BatchKey(1, 0), b'y')
    snapshot = restarted.observe(handle)
    assert snapshot['sink_bytes'] == 230 and snapshot['batch_count'] == 2
    assert snapshot['counter_epoch'] == epoch and not snapshot['load_quiescent']
    with sessions() as db:
        rows = list(db.scalars(select(LoadBatch).where(LoadBatch.command_id == handle)))
        assert all('DO_NOT_PERSIST' not in json.dumps(row.receipt) for row in rows)
    closed = restarted.close(handle, 'stop-1')
    assert closed['load_quiescent'] and restarted.close(handle, 'stop-2')['close_id'] == 'stop-1'
    rejected(lambda: restarted.send(handle, BatchKey(0, 2), b'z'), 'admission_closed')
    outcomes.append('multiple_batches_durable_dedup_binding_and_close')

    # observe/close can see intent from a separate transaction BEFORE Adapter I/O
    # completes; closing does not erase the reserved external request.
    active_module = ManagedLoad(FixtureAdapter(), sessions)
    active_handle = active_module.bind(binding())
    def observe_during_send():
        observed = active_module.observe(active_handle)
        assert observed['active_count'] == 1 and observed['sink_bytes'] is None
        closing = active_module.close(active_handle, 'stop-inflight')
        assert closing['admission_closed'] and not closing['load_quiescent']
    active_module.adapter.during_send = observe_during_send
    active_module.send(active_handle, BatchKey(0, 0), b'x')
    assert active_module.observe(active_handle)['load_quiescent']
    assert active_module.observe(active_handle)['close_id'] == 'supervision-incomplete'
    rejected(lambda: active_module.send(active_handle, BatchKey(0, 1), b'y'), 'admission_closed')
    outcomes.append('intent_committed_before_io_and_incomplete_observation_permanently_closes')

    # Move only isolated fixture timestamps; no sleeps or production settings.
    stale_adapter = FixtureAdapter()
    stale_module = ManagedLoad(stale_adapter, sessions)
    stale_binding = binding()
    stale_handle = stale_module.bind(stale_binding)
    with sessions.begin() as db:
        row = db.get(LoadRun, stale_handle)
        row.last_observed_at = row.last_observed_at - timedelta(seconds=7)
        old_observed = row.last_observed_at
    stale_module.bind(stale_binding)
    with sessions() as db:
        assert db.get(LoadRun, stale_handle).last_observed_at == old_observed
    denied = stale_module.send(stale_handle, BatchKey(0, 0), b'x')
    assert denied['phase'] == 'denied' and denied['reason'] == 'supervision_stale'
    assert stale_adapter.calls == 0
    stale_module.bind(stale_binding)
    with sessions() as db:
        row = db.get(LoadRun, stale_handle)
        assert row.closed and row.close_id == 'supervision-stale'
        assert row.last_observed_at == old_observed
    rejected(lambda: stale_module.send(stale_handle, BatchKey(0, 1), b'y'), 'admission_closed')
    assert stale_module.observe(stale_handle)['admission_closed']

    # A late observe cannot erase an already missed freshness deadline either.
    late_handle = stale_module.bind(binding())
    with sessions.begin() as db:
        row = db.get(LoadRun, late_handle)
        row.last_observed_at = row.last_observed_at - timedelta(seconds=7)
    assert stale_module.observe(late_handle)['close_id'] == 'supervision-stale'
    outcomes.append('stale_send_closure_commits_and_bind_or_late_observe_cannot_reopen')

    for failure in (OSError('sensitive transport text'), KeyboardInterrupt()):
        failed_adapter = FixtureAdapter(failure=failure)
        failed = ManagedLoad(failed_adapter, sessions)
        unknown_handle = failed.bind(binding())
        try:
            failed.send(unknown_handle, BatchKey(0, 0), b'x')
        except KeyboardInterrupt:
            pass  # Simulated death leaves durable dispatching.
        recovered = ManagedLoad(failed_adapter, sessions)
        recovered.send(unknown_handle, BatchKey(0, 0), b'x')
        assert failed_adapter.calls == 1
        snapshot = recovered.close(unknown_handle, 'stop-unknown')
        assert not snapshot['complete'] and snapshot['sink_bytes'] is None
        assert not snapshot['load_quiescent']
        # A different key cannot sidestep an unresolved prior request.
        blocked = ManagedLoad(failed_adapter, sessions)
        other_handle = blocked.bind(binding())
        try:
            blocked.send(other_handle, BatchKey(0, 0), b'x')
        except KeyboardInterrupt:
            pass
        rejected(lambda: blocked.send(other_handle, BatchKey(0, 1), b'y'), 'unsettled_batch_isolated')
    outcomes.append('transport_loss_and_process_death_never_replay')

    for status in ('Publish Timeout', 'Fail', 'Label Already Exists'):
        exceptional = ManagedLoad(FixtureAdapter(status=status), sessions)
        exceptional_handle = exceptional.bind(binding())
        exceptional.send(exceptional_handle, BatchKey(0, 0), b'x')
        snapshot = exceptional.close(exceptional_handle, 'stop-status')
        assert snapshot['unsettled_count'] == 1 and snapshot['sink_bytes'] is None
        assert not snapshot['load_quiescent']
        if status == 'Label Already Exists':
            assert snapshot['receipt_bytes'] == 0  # Never recover original bytes from duplicate response.
    outcomes.append('non_success_isolation_and_duplicate_label_not_original_measurement')

    class InvalidReceipt(FixtureAdapter):
        def __init__(self, mode):
            super().__init__()
            self.mode = mode

        def send(self, frozen, label, body):
            original = super().send(frozen, label, body)
            if self.mode == 'identity':
                return replace(original, physical_cluster_id='another-cluster')
            if self.mode == 'duplicate':
                raw = original.raw[:-1] + b', "LoadBytes": 999}'
            else:
                fields = json.loads(original.raw)
                fields['LoadBytes'] = True
                raw = json.dumps(fields).encode()
            return replace(original, raw=raw)

    for mode in ('identity', 'duplicate', 'boolean_counter'):
        invalid = ManagedLoad(InvalidReceipt(mode), sessions)
        invalid_handle = invalid.bind(binding())
        assert invalid.send(invalid_handle, BatchKey(0, 0), b'x')['phase'] == 'unknown'
        assert invalid.observe(invalid_handle)['sink_bytes'] is None
    outcomes.append('physical_identity_and_strict_raw_receipt_validation')
    return outcomes


if __name__ == '__main__':
    with journal_database() as fixture_sessions:
        results = verify(fixture_sessions)
    print(json.dumps({'level': 'postgresql_journal_fixture', 'real_l2_qualified': False,
                      'passed': results}, ensure_ascii=False))
