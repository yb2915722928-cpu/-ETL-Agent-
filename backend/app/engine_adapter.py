"""SeaTunnel 2.3.12 REST v2 protocol, without invented L1 receipts.

The caller must persist the chosen job ID and submission intent before submit.
A lost response is reconciled by GET of that ID; the POST is never replayed. A transport failure never authorizes resubmission.
A terminal engine job does not prove that external Doris writes have settled.
"""
import json
import urllib.request


TERMINAL_STATES = frozenset(('FINISHED', 'CANCELED', 'FAILED'))
JOB_STATES = TERMINAL_STATES | frozenset((
    'INITIALIZING', 'CREATED', 'PENDING', 'SCHEDULED', 'RUNNING',
    'FAILING', 'CANCELING', 'DOING_SAVEPOINT', 'SAVEPOINT_DONE', 'UNKNOWABLE'))


def job_name(command_id):
    return 'etl_agent_real_' + command_id.replace('-', '')


def associated_job(info, expected_id, expected_name):
    """Unknown IDs echo jobId without jobStatus; never synthesize missing proof."""
    if (not isinstance(info, dict) or str(info.get('jobId')) != str(expected_id)
            or info.get('jobName') != expected_name or info.get('jobStatus') not in JOB_STATES):
        return None
    return {'job_id': str(expected_id), 'job_name': expected_name,
            'engine_state': info['jobStatus'],
            'engine_terminal': info['jobStatus'] in TERMINAL_STATES,
            'sink_quiescent': None, 'metrics': info.get('metrics'),
            'submit_time': info.get('submitTime'), 'finish_time': info.get('finishedTime')}


class SeaTunnelClient:
    def __init__(self, base_url, timeout=5):
        self.base_url, self.timeout = base_url.rstrip('/'), timeout

    def _request(self, method, path, body=None):
        request = urllib.request.Request(self.base_url + path,
            data=json.dumps(body, ensure_ascii=False, allow_nan=False).encode('utf-8') if body is not None else None,
            method=method, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return json.load(response)

    def overview(self):
        return self._request('GET', '/overview')

    @staticmethod
    def validate_job_id(value):
        identifier = str(value)
        if not identifier.isdecimal() or not 0 < int(identifier) < 2 ** 63:
            raise ValueError('真实作业 ID 必须是已持久化的正 Long')
        return identifier

    def submit(self, job, engine_job_id):
        # Caller commits this ID before this single external attempt. A rejected,
        # mismatched or lost response never leads to another POST.
        expected = self.validate_job_id(engine_job_id)
        result = self._request('POST', '/submit-job?jobId=' + expected, job)
        if not isinstance(result, dict) or str(result.get('jobId')) != expected:
            raise ValueError('真实提交响应与原作业 ID 不一致；保持待核对，禁止重送')
        return expected

    def observe(self, engine_job_id, expected_name):
        info = self._request('GET', '/job-info/' + str(engine_job_id))
        return associated_job(info, engine_job_id, expected_name)

    def stop(self, engine_job_id):
        result = self._request('POST', '/stop-job/' + str(engine_job_id),
                               {'jobId': str(engine_job_id), 'isStopWithSavePoint': False})
        return {'accepted': str(result.get('jobId')) == str(engine_job_id),
                'termination_confirmed': False}

    def cleanup(self, engine_job_id):
        raise ValueError('SeaTunnel 无影子表清理接口；必须独立核对 Doris 对象后执行受管清理')
