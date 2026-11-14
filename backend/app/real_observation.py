"""Compose actual SeaTunnel source counters and complete ingress load observations.

Neither HTTP-sink counters nor received records are a substitute for independent
source emissions. This module produces no values for missing measurements and
never grants L2 qualification, sink quiescence, or publication permission.
"""
from datetime import datetime
import math


def _integer(value):
    # SeaTunnel 2.3.12 emits decimal strings. Do not coerce booleans/floats.
    if type(value) is int and value >= 0:
        return value
    if type(value) is str and value.isascii() and value.isdecimal():
        return int(value)
    return None


def compose_observation(contract, proof, ingress, *, engine_job_id, expected_name,
                        engine_observed_at, elapsed_seconds, now: datetime):
    """Return (sample, reasons). proof must be associated_job's validated result.

    The caller takes the ingress barrier BEFORE its original-ID engine read, so
    engine source emissions cannot legitimately precede the earlier evaluation
    count. No metrics are backfilled across an engine read or receipt gap.
    """
    if (not isinstance(proof, dict) or proof.get('job_id') != str(engine_job_id)
            or proof.get('job_name') != expected_name):
        return None, ['original_job_unconfirmed']
    if proof.get('engine_state') not in ('INITIALIZING', 'CREATED', 'PENDING', 'SCHEDULED', 'RUNNING', 'FINISHED'):
        return None, ['source_observation_unavailable']
    if not isinstance(ingress, dict) or not ingress.get('complete'):
        return None, ['load_observation_incomplete']
    if ingress.get('failures'):
        return None, ['ingress_permanently_closed']
    if ingress.get('byte_kind') != 'measured':
        return None, ['bytes_not_measured']
    if (type(elapsed_seconds) not in (int, float) or not math.isfinite(elapsed_seconds)
            or elapsed_seconds < 0):
        return None, ['elapsed_observation_invalid']
    metrics = proof.get('metrics')
    source_read = _integer(metrics.get('SourceReceivedCount')) if isinstance(metrics, dict) else None
    if source_read is None:
        return None, ['source_counter_missing']
    counts = {key: ingress.get(key) for key in
              ('evaluated', 'valid', 'rejected', 'final_output', 'sink_bytes')}
    if any(type(value) is not int or value < 0 for value in counts.values()):
        return None, ['ingress_counter_missing']
    if (counts['evaluated'] != counts['valid'] + counts['rejected']
            or counts['evaluated'] > source_read
            or counts['final_output'] != counts['evaluated']):
        return None, ['source_quality_write_counts_inconsistent']
    finished = proof['engine_state'] == 'FINISHED'
    if finished and (source_read != counts['evaluated'] or not ingress.get('admission_closed')
                     or not ingress.get('load_quiescent')):
        return None, ['final_source_or_load_incomplete']
    try:
        ingress_time = datetime.fromisoformat(ingress['observed_at'])
        engine_time = datetime.fromisoformat(engine_observed_at)
        observed = min(ingress_time, engine_time)
        if observed.tzinfo is None or max((now - ingress_time).total_seconds(),
                                          (now - engine_time).total_seconds()) > contract['stale_after_seconds']:
            return None, ['observation_stale']
        if max(ingress_time, engine_time) > now:
            return None, ['observation_from_future']
    except (KeyError, TypeError, ValueError):
        return None, ['observation_timestamp_invalid']
    epoch = ingress.get('counter_epoch')
    if not isinstance(epoch, str) or not epoch:
        return None, ['counter_epoch_missing']
    return {'contract_version': contract['version'],
            'source': 'seatunnel-2.3.12-source+managed-doris-journal-v1',
            'counter_epoch': str(engine_job_id) + ':' + epoch,
            'observed_at': observed.isoformat(), 'byte_kind': 'measured',
            'state': 'finished' if finished else 'running', 'source_read': source_read,
            **counts, 'elapsed_seconds': elapsed_seconds}, []


def derive_terminal_metrics(contract, proof, ingress, previous, *, engine_job_id,
                            expected_name, engine_observed_at, elapsed_seconds, now: datetime):
    """Derive sealed cleanup-only counters, never full-source/publication eligibility.

    Cancellation may leave emitted records unevaluated or judged records unsent.
    Every dispatched load still needs a complete original receipt. The caller
    must retain publication veto and must not send this sample to finalization.
    An unsafe *budget/quality decision* can have known counters; invalid/reset or
    missing measurements never do. Returned decision always records that veto.
    """
    from .runtime_supervision import evaluate_observation

    def unavailable(reason):
        return None, evaluate_observation(contract, None, previous, now), [reason]

    if (not isinstance(proof, dict) or proof.get('job_id') != str(engine_job_id)
            or proof.get('job_name') != expected_name
            or proof.get('engine_state') not in ('FINISHED', 'CANCELED', 'FAILED')):
        return unavailable('terminal_original_job_unconfirmed')
    if (not isinstance(ingress, dict) or ingress.get('complete') is not True
            or ingress.get('admission_closed') is not True or ingress.get('load_quiescent') is not True
            or ingress.get('byte_kind') != 'measured'):
        return unavailable('terminal_load_observation_incomplete')
    metrics = proof.get('metrics')
    source_read = _integer(metrics.get('SourceReceivedCount')) if isinstance(metrics, dict) else None
    if source_read is None:
        return unavailable('terminal_source_counter_missing')
    counts = {key: ingress.get(key) for key in ('evaluated', 'valid', 'rejected', 'final_output', 'sink_bytes')}
    if (any(type(value) is not int or value < 0 for value in counts.values())
            or type(elapsed_seconds) not in (int, float) or not math.isfinite(elapsed_seconds)
            or elapsed_seconds < 0):
        return unavailable('terminal_counter_missing_or_invalid')
    if (counts['evaluated'] != counts['valid'] + counts['rejected']
            or not counts['final_output'] <= counts['evaluated'] <= source_read):
        return unavailable('terminal_source_quality_write_counts_inconsistent')
    # Closing on operator stop/supervision safety is expected. Failures are not
    # erased; they veto publication without manufacturing a byte gap or success.
    epoch = ingress.get('counter_epoch')
    if not isinstance(epoch, str) or not epoch:
        return unavailable('terminal_counter_epoch_missing')
    try:
        ingress_time = datetime.fromisoformat(ingress['observed_at'])
        engine_time = datetime.fromisoformat(engine_observed_at)
        observed = min(ingress_time, engine_time)
        if (ingress_time.tzinfo is None or engine_time.tzinfo is None
                or max((now - ingress_time).total_seconds(), (now - engine_time).total_seconds()) > 6
                or max(ingress_time, engine_time) > now):
            return unavailable('terminal_observation_stale_or_future')
    except (KeyError, TypeError, ValueError):
        return unavailable('terminal_observation_timestamp_invalid')
    sample = {'contract_version': contract['version'],
        'source': 'seatunnel-2.3.12-source+managed-doris-journal-v1',
        'counter_epoch': str(engine_job_id) + ':' + epoch,
        'observed_at': observed.isoformat(), 'byte_kind': 'measured',
        'state': 'finished', 'source_read': source_read, **counts, 'elapsed_seconds': elapsed_seconds,
        'terminal_purpose': 'cleanup_only', 'engine_state': proof['engine_state']}
    decision = evaluate_observation(contract, sample, previous, now)
    # Counter validation/epoch regression exposes all-null metrics. Budget,
    # quality and empty-result reasons preserve the actual measured counters.
    if any(decision['metrics'][key] is None for key in ('source_read', 'evaluated', 'valid',
                                                       'rejected', 'final_output', 'sink_bytes', 'elapsed_seconds')):
        return None, decision, list(decision['reasons'])
    return sample, {**decision, 'status': 'unsafe',
                    'reasons': list(dict.fromkeys([*decision['reasons'], 'terminal_cleanup_only']))}, []
