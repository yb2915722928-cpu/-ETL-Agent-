"""PostgreSQL internal-ingress fixture with injected Doris adapter, NOT real L2."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import hashlib
import json
from threading import Event
from time import monotonic

from sqlalchemy import event, func, select
from sqlalchemy.schema import CreateTable

from verify_managed_load_fixtures import journal_database
from verify_quality_fixtures import setup
from app.managed_load import LoadRun
from app.quality_runtime import QualityRecord, QualityRun
from app.real_ingress import IngressBinding, IngressBindingFacts, IngressDenied, IngressService
from app.supervision_runtime import RuntimeSupervision
from app.execution import ManagedCommand  # Resolve the supervision command-id column type for isolated DDL.


TOKEN = 'fixture-owned-token-not-a-deployment-secret'
SENTINEL = 'PRIVATE_RECORD_SENTINEL_771'


def rejected(call, code):
    try:
        call()
    except IngressDenied as error:
        assert error.code == code, error.code
    else:
        raise AssertionError('expected non-success ingress outcome')


class JournalFixtureIngress(IngressService):
    # This lower-layer fixture isolates journal/admission races from controller
    # readiness. Product readiness is exercised without this seam by
    # verify_real_startup_fixtures through actual submit/control/PG journals.
    def _trusted_ready(self, db, binding, now):
        return True


def new_ingress(sessions, *, observed=True):
    run, commands, quality, loads = setup(sessions)
    checked = []
    def authorize(facts):
        checked.append(facts['command_id'])
    with sessions() as db:
        contract = db.get(QualityRun, run).contract_sha256
    binding = IngressBindingFacts(commands['valid'], run, '123', commands, contract, ('id',))
    ingress = JournalFixtureIngress(lambda _: loads, authorize, sessions)
    handle = ingress.bind(binding, hashlib.sha256(TOKEN.encode()).hexdigest())
    if observed:
        assert ingress.observe(handle)['complete']
    return ingress, handle, checked, loads


def verify_concurrency(sessions, raw):
    ingress, handle, checked, loads = new_ingress(sessions)
    # An older route sample vetoes admission even while the successful ingress
    # barrier and the other route are recent. Do not wait out the real 2s tick.
    with sessions.begin() as db:
        previous = db.scalar(select(func.clock_timestamp())) - timedelta(milliseconds=400)
        binding = db.get(IngressBinding, handle)
        db.get(LoadRun, binding.facts['route_commands']['rejected']).last_observed_at = previous
    observing, release, waiting = Event(), Event(), Event()
    original_observe = loads.observe
    def held_observe(command):
        observing.set()
        assert release.wait(5), 'observer fixture was not released'
        return original_observe(command)
    window_reads = 0
    def before_window_read(connection, cursor, statement, parameters, context, executemany):
        nonlocal window_reads
        if statement.startswith('SELECT') and 'managed_load_runs' in statement:
            window_reads += 1
            if window_reads >= 2:
                waiting.set()
    loads.observe = held_observe
    engine = sessions.kw['bind']
    event.listen(engine, 'before_cursor_execute', before_window_read)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            posting = pool.submit(ingress.post, handle, TOKEN, raw)
            try:
                assert waiting.wait(2), 'late POST did not wait for a new observation window'
                assert not posting.done() and loads.adapter.calls == 0 and checked == [handle]
                observation = pool.submit(ingress.observe, handle)
                assert observing.wait(2), 'waiting POST held the observation barrier'
                assert not posting.done() and loads.adapter.calls == 0 and checked == [handle]
            finally:
                release.set()
            assert observation.result(timeout=3)['complete']
            assert posting.result(timeout=3)['accepted']
    finally:
        event.remove(engine, 'before_cursor_execute', before_window_read)
    assert loads.adapter.calls == 1 and len(checked) == 2
    sample = ingress.observe(handle)
    assert sample['complete'] and sample['final_output'] == 1 and not sample['admission_closed']
    passed = ['late_post_waits_outside_barrier_for_new_complete_window_then_single_send']

    active, active_handle, _, active_loads = new_ingress(sessions)
    dispatched, finish_send = Event(), Event()
    def held_send():
        dispatched.set()
        assert finish_send.wait(5), 'reserved send fixture was not released'
    active_loads.adapter.during_send = held_send
    with ThreadPoolExecutor(max_workers=1) as pool:
        posting = pool.submit(active.post, active_handle, TOKEN, raw)
        try:
            assert dispatched.wait(2), 'POST did not dispatch'
            started = monotonic()
            sample = active.observe(active_handle)
            assert monotonic() - started < 0.5, 'observer waited for the reserved POST'
            assert sample['admission_closed'] and not sample['complete']
            assert sample['sink_bytes'] is None and sample['byte_kind'] == 'unavailable'
            assert not sample['load_quiescent'] and active_loads.adapter.calls == 1
        finally:
            finish_send.set()
        rejected(lambda: posting.result(timeout=3), 'ingress_record_or_load_failed')
    passed.append('observer_collision_with_dispatched_post_immediately_closes_and_reports_unknown_bytes')

    timed, timed_handle, timed_checked, timed_loads = new_ingress(sessions, observed=False)
    assert timed._binding(timed_handle).last_complete_observed_at is None
    observing, release = Event(), Event()
    original_timed_observe = timed_loads.observe
    def held_timed_observe(command):
        observing.set()
        assert release.wait(5), 'timeout observer fixture was not released'
        return original_timed_observe(command)
    timed_loads.observe = held_timed_observe
    with ThreadPoolExecutor(max_workers=1) as pool:
        observation = pool.submit(timed.observe, timed_handle)
        try:
            assert observing.wait(2), 'timeout observer did not acquire its barrier'
            started = monotonic()
            rejected(lambda: timed.post(timed_handle, TOKEN, raw), 'ingress_admission_timeout')
            elapsed = monotonic() - started
            assert 2.4 <= elapsed < 4, ('POST observation wait was not bounded', elapsed)
            binding = timed._binding(timed_handle)
            assert binding.closed and binding.failures == ['ingress_admission_timeout']
            assert timed_loads.adapter.calls == 0 and timed_checked == [timed_handle]
            assert all(original_timed_observe(command)['admission_closed']
                       for command in binding.facts['route_commands'].values())
        finally:
            release.set()
        observation.result(timeout=3)
    assert timed.observe(timed_handle)['evaluated'] == 0
    rejected(lambda: timed.post(timed_handle, TOKEN, raw), 'ingress_closed')
    passed.append('no_complete_observer_times_out_persistently_without_judge_or_send')

    victim, victim_handle, victim_checked, victim_loads = new_ingress(sessions)
    observing, release = Event(), Event()
    original_victim_observe = victim_loads.observe
    def held_victim_observe(command):
        observing.set()
        assert release.wait(5), 'unauthorized observer fixture was not released'
        return original_victim_observe(command)
    victim_loads.observe = held_victim_observe
    with ThreadPoolExecutor(max_workers=1) as pool:
        observation = pool.submit(victim.observe, victim_handle)
        try:
            assert observing.wait(2), 'victim observer did not acquire its barrier'
            started = monotonic()
            for token in ('wrong', None):
                rejected(lambda: victim.post(victim_handle, token, b'{broken'), 'ingress_unauthorized')
            assert monotonic() - started < 0.5, 'unauthorized POST entered the observation wait'
            binding = victim._binding(victim_handle)
            assert not binding.closed and binding.failures == []
            assert victim_loads.adapter.calls == 0 and victim_checked == []
        finally:
            release.set()
        assert observation.result(timeout=3)['complete']
    assert victim.post(victim_handle, TOKEN, raw)['accepted'] and victim_loads.adapter.calls == 1
    passed.append('unauthorized_post_during_observer_barrier_never_waits_or_closes_victim')
    return passed


def verify_close_race(sessions, raw):
    # Close at either authorization boundary, then at the last gap before the
    # journal reserves dispatch. Every close commits all admission flags.
    for boundary in (1, 2, 'before_reservation'):
        ingress, handle, checked, loads = new_ingress(sessions)
        original_authorize, original_send = ingress.authorize, loads.send
        def close_and_check():
            assert ingress.close(handle)['load_quiescent']
            with sessions() as db:
                binding = db.get(IngressBinding, handle)
                assert binding.closed
                assert all(db.get(LoadRun, command).closed
                           for command in binding.facts['route_commands'].values())
        def close_on_authorize(facts):
            original_authorize(facts)
            if len(checked) == boundary:
                close_and_check()
        def close_before_reservation(*args):
            close_and_check()
            return original_send(*args)
        ingress.authorize = close_on_authorize
        if boundary == 'before_reservation':
            loads.send = close_before_reservation
        code = 'ingress_closed' if boundary == 1 else 'ingress_record_or_load_failed'
        rejected(lambda: ingress.post(handle, TOKEN, raw), code)
        assert loads.adapter.calls == 0
        with sessions() as db:
            binding = db.get(IngressBinding, handle)
            records = list(db.scalars(select(QualityRecord).where(
                QualityRecord.run_id == binding.facts['run_id'])))
            assert len(records) == (0 if boundary == 1 else 1)
        # Idempotent close never reopens either route.
        assert ingress.close(handle)['admission_closed']

    # The reverse ordering is already an irreversible journal reservation.
    # Closing must return while external I/O is paused and show no quiescence.
    ingress, handle, _, loads = new_ingress(sessions)
    reserved, release = Event(), Event()
    def pause_reserved_send():
        reserved.set()
        assert release.wait(5), 'reserved request was not released'
    loads.adapter.during_send = pause_reserved_send
    with ThreadPoolExecutor(max_workers=1) as pool:
        posting = pool.submit(ingress.post, handle, TOKEN, raw)
        try:
            assert reserved.wait(2)
            closed = ingress.close(handle)
            assert closed['admission_closed'] and not closed['load_quiescent']
            assert loads.adapter.calls == 1
        finally:
            release.set()
        rejected(lambda: posting.result(timeout=3), 'ingress_record_or_load_failed')
    return ['atomic_close_before_journal_reservation_has_zero_io_and_reserved_first_is_unsettled']


def verify(sessions):
    ingress, handle, checked, loads = new_ingress(sessions)
    raw = json.dumps({'id': 1, 'name': SENTINEL, 'score': 4, 'source_status': 'keep'}).encode()
    rejected(lambda: ingress.post(handle, 'wrong', b'{broken'), 'ingress_unauthorized')
    assert not ingress._binding(handle).closed
    first = ingress.post(handle, TOKEN, raw)
    assert first['accepted'] and len(checked) == 2 and loads.adapter.calls == 1
    assert ingress.post(handle, TOKEN, raw) == first
    assert loads.adapter.calls == 1
    sample = ingress.observe(handle)
    assert sample['complete'] and sample['evaluated'] == 1 and sample['sink_bytes'] == 115
    assert sample['source_read'] is None and sample['parsed'] is None
    assert sample['final_output'] == 1 and sample['byte_kind'] == 'measured'
    assert sample['counter_epoch'] and sample['observed_at']
    changed = json.dumps({'id': 1, 'name': SENTINEL, 'score': 9, 'source_status': 'keep'}).encode()
    rejected(lambda: ingress.post(handle, TOKEN, changed), 'ingress_record_or_load_failed')
    assert ingress._binding(handle).closed and loads.adapter.calls == 1
    rejected(lambda: ingress.post(handle, TOKEN, raw), 'ingress_closed')
    with sessions() as db:
        binding = db.get(IngressBinding, handle)
        records = list(db.scalars(select(QualityRecord).where(QualityRecord.run_id == binding.facts['run_id'])))
        persisted = {'facts': binding.facts, 'links': binding.links, 'failures': binding.failures,
                     'records': [{'body': row.body_sha256, 'errors': row.errors} for row in records]}
        assert TOKEN not in json.dumps(persisted) and SENTINEL not in json.dumps(persisted)
    passed = ['stable_retry_single_doris_send_and_changed_payload_permanent_veto',
              'invalid_token_cannot_close_victim_and_sensitive_values_do_not_persist']

    for bad in (b'{"id":1,"id":2}', b'[]', b'{"id":NaN}', b'x' * 65537):
        broken, broken_handle, _, broken_loads = new_ingress(sessions)
        rejected(lambda: broken.post(broken_handle, TOKEN, bad), 'ingress_record_or_load_failed')
        assert broken._binding(broken_handle).closed and broken_loads.adapter.calls == 0
    passed.append('authenticated_malformed_duplicate_nonfinite_or_oversize_body_closes_before_doris')

    active, active_handle, _, active_loads = new_ingress(sessions)
    def observe_inflight():
        observed = active.observe(active_handle)
        assert not observed['complete'] and observed['sink_bytes'] is None
        assert observed['admission_closed']
    active_loads.adapter.during_send = observe_inflight
    rejected(lambda: active.post(active_handle, TOKEN, raw), 'ingress_record_or_load_failed')
    assert active._binding(active_handle).closed
    rejected(lambda: active.post(active_handle, TOKEN, raw), 'ingress_closed')
    passed.append('observation_overlapping_reserved_post_closes_without_claiming_complete_bytes')

    failed, failed_handle, _, failed_loads = new_ingress(sessions)
    failed_loads.adapter.failure = OSError('SECRET_MUST_NOT_LEAK')
    rejected(lambda: failed.post(failed_handle, TOKEN, raw), 'ingress_record_or_load_failed')
    assert failed._binding(failed_handle).closed
    assert failed.observe(failed_handle)['sink_bytes'] is None
    assert failed_loads.adapter.calls == 1
    passed.append('unknown_doris_receipt_never_returns_success_and_stays_isolated')
    normal, normal_handle, _, _ = new_ingress(sessions)
    normal.post(normal_handle, TOKEN, raw)
    normal.close(normal_handle, 'engine_finished')
    normal_sample = normal.observe(normal_handle)
    assert normal_sample['failures'] == [] and normal_sample['admission_closed']
    assert normal_sample['load_quiescent'] and normal_sample['final_output'] == 1
    passed.append('trusted_engine_finished_closure_does_not_invent_failure')

    zero, zero_handle, _, zero_loads = new_ingress(sessions)
    original_send = zero_loads.adapter.send
    def zero_loaded(frozen, label, body):
        from dataclasses import replace
        result = original_send(frozen, label, body)
        fields = json.loads(result.raw)
        fields['NumberLoadedRows'] = 0
        return replace(result, raw=json.dumps(fields).encode())
    zero_loads.adapter.send = zero_loaded
    rejected(lambda: zero.post(zero_handle, TOKEN, raw), 'ingress_record_or_load_failed')
    zero_sample = zero.observe(zero_handle)
    assert zero_sample['admission_closed'] and not zero_sample['complete']
    assert zero_sample['final_output'] == 0 and zero_sample['sink_bytes'] is None
    passed.append('success_status_with_zero_loaded_rows_never_proves_final_output')
    passed.extend(verify_concurrency(sessions, raw))
    passed.extend(verify_close_race(sessions, raw))
    return passed


if __name__ == '__main__':
    with journal_database() as sessions:
        # The journal-only readiness seam needs the selected empty table, but
        # intentionally has no execution-domain command rows or foreign keys.
        with sessions.kw['bind'].begin() as db:
            db.execute(CreateTable(RuntimeSupervision.__table__, include_foreign_key_constraints=[]))
        for table in (QualityRun.__table__, QualityRecord.__table__, IngressBinding.__table__):
            table.create(sessions.kw['bind'])
        results = verify(sessions)
    print(json.dumps({'level': 'postgresql_ingress_fixture', 'real_l2_qualified': False,
                      'passed': results}, ensure_ascii=False))
