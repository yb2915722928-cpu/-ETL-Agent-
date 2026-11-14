"""Pinned real-runtime facts and diagnostic job construction.

These helpers do not authorize a managed transfer. The public real execution
path remains denied until frozen writer credentials, current cumulative bytes,
external-write quiescence and scoped cleanup have all been verified.
"""
import hashlib
from datetime import datetime, timezone

REAL_ENGINE = {'name': 'apache-seatunnel', 'version': '2.3.12', 'git_commit_abbrev': 'aa70e2d',
               'rest_api': 'v2', 'java': '17', 'distribution': 'apache-seatunnel-2.3.12-bin.tar.gz',
               'sha512': 'f77f85195429db39367447f2cb1cb90ba5663c268e606c58eb077cbd7d984e2de06638f18e000658f553a68fcbe6435c9020a0820cd137eb7bea65063c2ff1fa'}
REAL_CONNECTORS = (
    {'artifact': 'connector-jdbc-2.3.12.jar', 'sha1': '97aacc31b8353bd21fb914d4df2c9947d64b2e82'},
    {'artifact': 'connector-doris-2.3.12.jar', 'sha1': '380fc6200ee7eb32266f0bb07e8ba2dfb11c8ee5'},
)
# The HTTP sink carries every managed record to the ingress; its digest is
# bound by the network receipt, so it is pinned beside the other artifacts.
HTTP_CONNECTOR = {
    'artifact': 'connector-http-base-2.3.12.jar',
    'sha1': '8107813d6c987de4bd7769713aac44f2815ebf96',
    'sha256': 'a2a8ab002697dea5f070a8c77edd44f5c53da8ac8cb3ff26b71ccefcefac3ca9',
}
REAL_DRIVER = {'artifact': 'mysql-connector-j-8.0.33.jar', 'sha1': '9e64d997873abc4318620264703d3fdb6b02dd5a'}
REAL_MYSQL = {'version': '8.0.46', 'driver': 'com.mysql.cj.jdbc.Driver'}
REAL_DORIS = {'version': '3.0.8-rc01', 'git_commit': '09b0cc49a60ffdd444df3e40e5f3dc180299b561',
              'query_port': 9030, 'http_port': 8030}
MEASURED_BYTE_SOURCE = {
    'name': 'doris_stream_load_record', 'kind': 'measured', 'unit': 'byte',
    'owner': 'execution_writer', 'query': "SHOW STREAM LOAD WHERE Label LIKE '<prefix>'",
    'fields': ('Label', 'Db', 'Table', 'Status', 'TotalRows', 'LoadedRows', 'FilteredRows', 'LoadBytes'),
    'running_channel_verified': False,
}


def real_runtime_pin():
    return {'engine': dict(REAL_ENGINE), 'connectors': [dict(item) for item in REAL_CONNECTORS],
            'driver': dict(REAL_DRIVER), 'source': dict(REAL_MYSQL), 'sink': dict(REAL_DORIS)}


def verify_real_runtime(overview, connector_digests, driver_digest):
    conditions = []
    if not overview or overview.get('projectVersion') != REAL_ENGINE['version']:
        conditions.append({'reason': 'engine_version_unverified', 'expected': REAL_ENGINE['version']})
    elif overview.get('gitCommitAbbrev') != REAL_ENGINE['git_commit_abbrev']:
        conditions.append({'reason': 'engine_commit_unverified', 'expected': REAL_ENGINE['git_commit_abbrev']})
    for expected in REAL_CONNECTORS:
        observed = connector_digests.get(expected['artifact'])
        if observed != expected['sha1']:
            conditions.append({'reason': 'connector_digest_unverified', 'artifact': expected['artifact'],
                                'observed': observed, 'expected': expected['sha1']})
    if driver_digest != REAL_DRIVER['sha1']:
        conditions.append({'reason': 'driver_digest_unverified', 'artifact': REAL_DRIVER['artifact']})
    return not conditions, conditions


def shadow_table(target_table, run_id):
    # Use the whole run identity, including the part after a shared prefix.
    identity = (target_table + '\0' + run_id).encode('utf-8')
    return 'shadow_' + hashlib.sha256(identity).hexdigest()[:32]


def stream_load_label(prefix, table, job_id, subtask_index=0):
    """Label prefix only; non-2PC LabelGenerator appends _<millisecond time>."""
    return prefix + '_' + table.replace('.', '_') + '_' + str(job_id) + '_' + str(subtask_index)


def real_job_request(managed, facts, writer, reader):
    """Diagnostic request only; plaintext credentials stay in caller memory."""
    from .engine_adapter import job_name
    source_table = facts['plan']['source']['table']
    shadow = shadow_table(facts['plan']['target']['table'], managed['run_id'])
    return {'env': {'job.mode': 'BATCH', 'parallelism': 1, 'job.name': job_name(managed['command_id'])},
            'source': [{'plugin_name': 'Jdbc', 'plugin_output': source_table,
                        'url': reader['jdbc_url'], 'driver': reader['driver'], 'user': reader['username'],
                        'password': reader['password'], 'query': reader['query'],
                        'table_paths': [reader['database'] + '.' + source_table],
                        'connection_check_timeout_sec': 10}],
            'sink': [{'plugin_name': 'Doris', 'plugin_input': [source_table],
                      'fenodes': writer['fenodes'], 'query-port': writer['query_port'], 'username': writer['username'],
                      'password': writer['password'], 'database': writer['database'],
                      'table': shadow, 'sink.label-prefix': writer['label_prefix'],
                      'sink.enable-2pc': False, 'schema_save_mode': 'ERROR_WHEN_SCHEMA_NOT_EXIST',
                      'data_save_mode': 'APPEND_DATA',
                      'doris.config': {'format': 'json', 'read_json_by_line': 'true'}}]}


def measured_bytes(receipt, database, table, label_prefix):
    """A scoped successful receipt proves its own bytes, not a live cumulative counter."""
    unavailable = {'reason': 'measured_byte_source_unavailable', 'source': MEASURED_BYTE_SOURCE['name']}
    if not receipt:
        return 'unavailable', None, unavailable
    label = str(receipt.get('Label', ''))
    suffix = label[len(label_prefix) + 1:] if label.startswith(label_prefix + '_') else ''
    if (receipt.get('Db') != database or receipt.get('Table') != table or not suffix.isdecimal()
            or receipt.get('Status') != 'Success'):
        return 'unavailable', None, {'reason': 'measured_byte_receipt_scope_or_status_invalid'}
    try:
        count = int(receipt['LoadBytes'])
    except (KeyError, TypeError, ValueError):
        return 'unavailable', None, unavailable
    if count < 0:
        return 'unavailable', None, unavailable
    return 'measured', count, None


def transfer_gate(supervision_frozen, control_ready, byte_source_available, runtime_verified):
    conditions = []
    if not runtime_verified:
        conditions.append({'reason': 'real_runtime_unverified', 'expected': real_runtime_pin()})
    if not supervision_frozen:
        conditions.append({'reason': 'budget_supervision_not_frozen'})
    if not control_ready:
        conditions.append({'reason': 'managed_stop_and_cleanup_not_ready'})
    if not byte_source_available:
        conditions.append({'reason': 'measured_byte_source_unavailable', 'source': MEASURED_BYTE_SOURCE})
    return not conditions, conditions


def utcnow():
    return datetime.now(timezone.utc)
