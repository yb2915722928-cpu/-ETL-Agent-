"""Boundary checks for independently sourced real counters. NOT engine/L2 evidence."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from app.real_observation import compose_observation, derive_terminal_metrics
from app.runtime_supervision import RuntimeSupervisionContract, evaluate_observation


def main():
    now = datetime.now(timezone.utc)
    contract = RuntimeSupervisionContract().model_dump()
    proof = {'job_id': '123', 'job_name': 'original', 'engine_state': 'FINISHED',
             'metrics': {'SourceReceivedCount': '3', 'SinkWriteCount': '999'}}
    ingress = {'complete': True, 'byte_kind': 'measured', 'sink_bytes': 115,
               'evaluated': 3, 'valid': 2, 'rejected': 1, 'final_output': 3,
               'admission_closed': True, 'load_quiescent': True, 'failures': [],
               'counter_epoch': 'journal-epoch', 'observed_at': now.isoformat()}
    def compose(p=proof, g=ingress):
        return compose_observation(contract, p, g, engine_job_id='123', expected_name='original',
                                   engine_observed_at=now.isoformat(), elapsed_seconds=5, now=now)
    sample, reasons = compose()
    assert not reasons and sample['source_read'] == 3 and sample['sink_bytes'] == 115
    assert sample['final_output'] == 3  # Not Http sink's untrusted acceptance count.
    decision = evaluate_observation(contract, sample, None, now)
    assert decision['status'] == 'unsafe' and decision['reasons'] == ['reject_ratio_exceeded']
    for p, g in (({**proof, 'metrics': {}}, ingress),
                 ({**proof, 'metrics': {'SourceReceivedCount': True}}, ingress),
                 ({**proof, 'engine_state': 'SCHEDULED', 'metrics': {}}, ingress),
                 ({**proof, 'job_name': 'other'}, ingress),
                 (proof, {**ingress, 'complete': False, 'sink_bytes': None}),
                 (proof, {**ingress, 'admission_closed': False}),
                 (proof, {**ingress, 'failures': ['supervision-incomplete']}),
                 (proof, {**ingress, 'observed_at': (now - timedelta(seconds=7)).isoformat()})):
        result, reasons = compose(p, g)
        assert result is None and reasons
    pending, reasons = compose({**proof, 'engine_state': 'PENDING',
        'metrics': {'SourceReceivedCount': '0'}}, {**ingress, 'evaluated': 0,
        'valid': 0, 'rejected': 0, 'final_output': 0, 'sink_bytes': 0, 'admission_closed': False})
    assert pending['source_read'] == 0 and not reasons  # Actual engine zero, no invented grace.
    running, _ = compose({**proof, 'engine_state': 'RUNNING'}, ingress)
    prior = {**running, 'source_read': 4}
    assert evaluate_observation(contract, running, prior, now)['reasons'] == ['metrics_reset']
    def terminal(p=proof, g=ingress, previous=None):
        return derive_terminal_metrics(contract, p, g, previous, engine_job_id='123', expected_name='original',
            engine_observed_at=now.isoformat(), elapsed_seconds=5, now=now)
    cancelled = {**proof, 'engine_state': 'CANCELED', 'metrics': {'SourceReceivedCount': '5'}}
    partial = {**ingress, 'final_output': 2, 'failures': ['ingress_closed']}
    sample, decision, reasons = terminal(cancelled, partial)
    assert not reasons and sample['source_read'] == 5 and sample['evaluated'] == 3 and sample['final_output'] == 2
    assert sample['terminal_purpose'] == 'cleanup_only' and sample['engine_state'] == 'CANCELED'
    assert decision['status'] == 'unsafe' and decision['metrics']['sink_bytes'] == 115
    assert 'terminal_cleanup_only' in decision['reasons'] and 'reject_ratio_exceeded' in decision['reasons']
    zero = {**ingress, 'evaluated': 0, 'valid': 0, 'rejected': 0, 'final_output': 0, 'sink_bytes': 0}
    sample, decision, reasons = terminal({**cancelled, 'metrics': {'SourceReceivedCount': '0'}}, zero)
    assert sample and not reasons and decision['metrics']['source_read'] == 0
    assert 'empty_result' in decision['reasons'] and decision['status'] == 'unsafe'
    for p, g, previous in (
            ({**cancelled, 'metrics': {}}, partial, None),
            ({**cancelled, 'metrics': {'SourceReceivedCount': True}}, partial, None),
            ({**cancelled, 'job_id': 'other'}, partial, None),
            ({**cancelled, 'engine_state': 'RUNNING'}, partial, None),
            (cancelled, {**partial, 'complete': False, 'sink_bytes': None}, None),
            (cancelled, {**partial, 'admission_closed': False}, None),
            (cancelled, {**partial, 'load_quiescent': False}, None),
            (cancelled, {**partial, 'counter_epoch': ''}, None),
            (cancelled, {**partial, 'observed_at': (now - timedelta(seconds=7)).isoformat()}, None),
            (cancelled, {**partial, 'observed_at': (now + timedelta(seconds=1)).isoformat()}, None),
            (cancelled, partial, {**running, 'source_read': 6}),
            (cancelled, partial, {**running, 'counter_epoch': 'different'})):
        sample, decision, reasons = terminal(p, g, previous)
        assert sample is None and reasons and all(decision['metrics'][key] is None for key in ('source_read', 'sink_bytes'))
    print(json.dumps({'passed': ['independent source/receipt counters; final small-sample reject veto',
        'missing or in-flight/stale/closed observations never synthesize zero or startup grace',
        'counter regression retains supervisor veto',
        'cancelled partial and genuine zero counts known only for cleanup; publication always vetoed',
        'terminal missing source, unknown bytes, open admission, stale or reset epoch remain pending'],
        'level': 'observation_boundary_fixture', 'real_l2_qualified': False}))


if __name__ == '__main__':
    main()
