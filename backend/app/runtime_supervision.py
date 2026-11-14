"""Frozen, reviewable metric semantics and the supervisor's decision boundary."""
from datetime import datetime
import math
from pydantic import BaseModel, ConfigDict


class RuntimeSupervisionContract(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    version: str = 'runtime-supervision-v1'
    max_rows: int = 100000
    max_bytes: int = 268435456
    max_seconds: float = 600
    max_amplification: float = 2
    max_reject_ratio: float = .2
    observation_seconds: int = 2
    stale_after_seconds: int = 6
    running_reject_min_evaluated: int = 100
    row_boundary: str = 'source_read: connector emitted records before mapping'
    quality_boundary: str = 'evaluated=valid+rejected: mapped records, stable-key deduplicated; reject once per record'
    output_boundary: str = 'final_output: sink received records after quality and expansion'
    byte_boundary: str = 'sink_bytes: cumulative measured sink accepted-write bytes; byte, not MiB or estimates'
    retry_boundary: str = 'all counters stable-key deduplicated, same counter_epoch; reset is unsafe'
    ratio_boundary: str = 'amplification=final_output/source_read; reject_ratio=rejected/evaluated; zero denominator=null'
    empty_policy: str = 'veto_publication'
    violation_action: str = 'persist_stop_request_and_veto_publication'


def supervision_contract(budget, *, allow_empty=False):
    contract = RuntimeSupervisionContract(**budget).model_dump()
    if allow_empty is True:
        contract['empty_policy'] = 'allow_complete_empty'
    return contract


def complete_empty_snapshot(sample):
    """Only a finished, measured, internally complete zero snapshot is empty proof."""
    return (isinstance(sample, dict) and sample.get('state') == 'finished'
            and sample.get('byte_kind') == 'measured'
            and all(type(sample.get(key)) is int and sample[key] == 0 for key in
                    ('source_read', 'evaluated', 'valid', 'rejected', 'final_output', 'sink_bytes')))


COUNTERS = ('source_read', 'evaluated', 'valid', 'rejected', 'final_output', 'sink_bytes', 'elapsed_seconds')


def evaluate_observation(contract, sample, previous, now: datetime):
    """Untrusted/missing observations expose no current values and always latch a veto."""
    unavailable = dict.fromkeys((*COUNTERS, 'throughput_rows_per_second', 'amplification', 'reject_ratio'))
    def unsafe(reason):
        return {'status': 'unsafe', 'reasons': [reason], 'metrics': unavailable}
    if sample is None:
        return unsafe('metrics_missing')
    if sample.get('contract_version') != contract['version'] or not sample.get('source') or not sample.get('counter_epoch'):
        return unsafe('metric_contract_mismatch')
    if sample.get('byte_kind') != 'measured':
        return unsafe('bytes_not_measured')
    try:
        observed = datetime.fromisoformat(sample['observed_at'])
        age = (now - observed).total_seconds()
        if age < -contract['observation_seconds'] or age > contract['stale_after_seconds']:
            return unsafe('metrics_stale')
    except (KeyError, ValueError, TypeError):
        return unsafe('metrics_timestamp_invalid')
    for key in COUNTERS:
        value = sample.get(key)
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or (key != 'elapsed_seconds' and type(value) is not int):
            return unsafe('metrics_missing_or_invalid')
    if sample.get('state') not in ('running', 'finished') or sample['evaluated'] != sample['valid'] + sample['rejected']:
        return unsafe('metrics_inconsistent')
    if previous and (sample['counter_epoch'] != previous['counter_epoch'] or sample['source'] != previous['source'] or
                     any(sample[key] < previous[key] for key in COUNTERS) or observed < datetime.fromisoformat(previous['observed_at'])):
        return unsafe('metrics_reset')
    metrics = {key: sample[key] for key in COUNTERS}
    metrics.update(throughput_rows_per_second=sample['final_output'] / sample['elapsed_seconds'] if sample['elapsed_seconds'] else None,
                   amplification=sample['final_output'] / sample['source_read'] if sample['source_read'] else None,
                   reject_ratio=sample['rejected'] / sample['evaluated'] if sample['evaluated'] else None)
    reasons = []
    for metric, limit, reason in (('source_read', 'max_rows', 'rows_exceeded'), ('sink_bytes', 'max_bytes', 'bytes_exceeded'),
                                  ('elapsed_seconds', 'max_seconds', 'duration_exceeded'), ('amplification', 'max_amplification', 'amplification_exceeded')):
        if metrics[metric] is not None and metrics[metric] > contract[limit]:
            reasons.append(reason)
    if (sample['state'] == 'finished' or sample['evaluated'] >= contract['running_reject_min_evaluated']) and metrics['reject_ratio'] is not None and metrics['reject_ratio'] > contract['max_reject_ratio']:
        reasons.append('reject_ratio_exceeded')
    if (sample['state'] == 'finished' and (sample['source_read'] == 0 or sample['final_output'] == 0)
            and not (contract.get('empty_policy') == 'allow_complete_empty' and complete_empty_snapshot(sample))):
        reasons.append('empty_result')
    return {'status': 'unsafe' if reasons else 'finished' if sample['state'] == 'finished' else 'observing',
            'reasons': reasons, 'metrics': metrics}
