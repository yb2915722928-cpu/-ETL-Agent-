"""Bounded, credential-free SeaTunnel protocol probe, never a managed L2 run."""
import json
from pathlib import Path
import sys
import time
import urllib.error
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from app.engine_adapter import SeaTunnelClient, associated_job, job_name


def probe(output):
    client = SeaTunnelClient('http://127.0.0.1:18080', timeout=5)
    identifier = str((uuid.uuid4().int % (2 ** 62)) + 1)
    name = job_name(str(uuid.uuid4()))
    report = {'level': 'real_protocol_probe', 'managed_l2_accepted': False,
              'job_id': identifier, 'job_name': name, 'submit_attempted': False, 'checks': {}}
    def persist():
        Path(output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    job = {'env': {'job.mode': 'BATCH', 'parallelism': 1, 'job.name': name},
           'source': [{'plugin_name': 'FakeSource', 'row.num': 10, 'split.num': 10,
                       'split.read-interval': 2000, 'schema': {'fields': {'id': 'int'}}}],
           'sink': [{'plugin_name': 'Console'}]}
    report['submit_attempted'] = True
    persist()  # This probe ID and intent survive even a lost response. Never submit again.
    try:
        returned = client.submit(job, identifier)
        report['checks']['preselected_id_response_matches'] = returned == identifier
    except (OSError, ValueError):
        report['checks']['submit_response'] = 'unknown; GET original ID only'
    deadline = time.monotonic() + 45
    observation = None
    failures = 0
    while time.monotonic() < deadline:
        try:
            observation = client.observe(identifier, name)
        except (OSError, ValueError):
            failures += 1
        if observation:
            break
        time.sleep(.5)
    report['checks']['associated_original_job'] = bool(observation)
    report['checks']['observation_transient_errors'] = failures
    report['checks']['initial_state'] = observation['engine_state'] if observation else None
    unknown = str((int(identifier) + 1) % (2 ** 63 - 1))
    raw = client._request('GET', '/job-info/' + unknown)
    report['checks']['unknown_id_is_not_job_proof'] = associated_job(raw, unknown, name) is None
    report['checks']['unknown_response_fields'] = sorted(raw) if isinstance(raw, dict) else []
    # A Console-only duplicate diagnostic is deliberately outside managed execution.
    # It tests the server contract; production callers never repeat this POST.
    try:
        duplicate = client._request('POST', '/submit-job?jobId=' + identifier, job)
        report['checks']['duplicate_diagnostic'] = {'response_fields': sorted(duplicate),
            'same_id': str(duplicate.get('jobId')) == identifier}
    except urllib.error.HTTPError as error:
        report['checks']['duplicate_diagnostic'] = {'http_status': error.code}
    except OSError:
        report['checks']['duplicate_diagnostic'] = {'outcome': 'unknown'}
    try:
        ack = client._request('POST', '/stop-job/' + identifier,
                             {'jobId': identifier, 'isStopWithSavePoint': False})
        report['checks']['stop_ack'] = ack
    except (OSError, ValueError):
        report['checks']['stop_ack'] = {'outcome': 'unknown'}
    deadline = time.monotonic() + 45
    terminal = None
    while time.monotonic() < deadline:
        try:
            candidate = client.observe(identifier, name)
            if candidate and candidate['engine_terminal']:
                terminal = candidate
                break
        except (OSError, ValueError):
            pass
        time.sleep(.5)
    report['checks']['terminal_state'] = terminal['engine_state'] if terminal else None
    report['checks']['sink_quiescence_proven'] = False
    persist()
    print(json.dumps(report, ensure_ascii=False))
    assert observation and terminal and report['checks']['preselected_id_response_matches']
    assert report['checks']['unknown_id_is_not_job_proof']


if __name__ == '__main__':
    probe(sys.argv[1])
