"""Pinned JDBC -> internal ingress job, constructed in memory after authorization.

This request contains the frozen source password and an ingress-only token.
Do not store it in EngineAttempt, evidence, API responses, or exception messages.
The engine never receives Doris credentials or a Doris endpoint.
"""
import re
from urllib.parse import urlsplit

from .connector_policy import fingerprint
from .connector_probe import identifier
from .engine_adapter import job_name
from .real_contract import real_execution_contract
from .real_engine import HTTP_CONNECTOR  # noqa: F401  re-exported for callers


def ingress_job_request(command_id, facts, source_resource, source_secret, ingress_base_url, ingress_token):
    """One bounded, non-restarting source scan; caller persists intent before POST.

    LIMIT(max_rows+1) makes a truncated full scan violate the row budget rather
    than appear complete. Stable non-null source primary keys are required for
    durable record/batch deduplication; they need not be mapped to the target.
    """
    if (source_resource.get('kind') != 'mysql'
            or fingerprint(source_resource) != facts['source']['resource_fingerprint']):
        raise ValueError('real_source_resource_mismatch')
    table = facts['plan']['source']['table']
    if table not in source_resource['tables'] or facts['source']['schema']['name'] != table:
        raise ValueError('real_source_table_mismatch')
    contract = real_execution_contract(facts['plan'], facts['source']['schema'], facts['target']['schema'])
    if facts.get('real_execution_contract') != contract:
        raise ValueError('real_execution_contract_mismatch')
    primary = contract['source_primary_key']
    maximum = facts['runtime_supervision_contract']['max_rows']
    if type(maximum) is not int or maximum <= 0:
        raise ValueError('real_source_row_budget_invalid')
    parsed = urlsplit(ingress_base_url)
    if (parsed.scheme != 'http' or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.query or parsed.fragment or parsed.path not in ('', '/')
            or not re.fullmatch(r'[a-zA-Z0-9_-]{32,128}', ingress_token)
            or not re.fullmatch(r'[a-zA-Z0-9-]{1,64}', command_id)):
        raise ValueError('real_ingress_route_invalid')
    host = source_resource['host']
    if not re.fullmatch(r'[a-zA-Z0-9_.-]+', host) or not 1 <= source_resource['port'] <= 65535:
        raise ValueError('real_source_endpoint_invalid')
    query = ('SELECT ' + ','.join(identifier(name) for name in contract['source_columns']) +
             ' FROM ' + identifier(source_resource['database']) + '.' + identifier(table) +
             ' LIMIT ' + str(maximum + 1))
    request = {'env': {'job.mode': 'BATCH', 'parallelism': 1, 'job.name': job_name(command_id),
                       'job.retry.times': 0},
        'source': [{'plugin_name': 'Jdbc', 'plugin_output': 'raw_records',
                    'url': 'jdbc:mysql://' + host + ':' + str(source_resource['port']) + '/' + source_resource['database'],
                    'driver': 'com.mysql.cj.jdbc.Driver', 'user': source_resource['username'],
                    'password': source_secret, 'query': query,
                    'table_paths': [source_resource['database'] + '.' + table],
                    'connection_check_timeout_sec': 3}],
        'sink': [{'plugin_name': 'Http', 'plugin_input': ['raw_records'],
                  'url': ingress_base_url.rstrip('/') + '/ingress/' + command_id,
                  'headers': {'Content-Type': 'application/json', 'x-etl-ingress-token': ingress_token},
                  'array_mode': False, 'batch_size': 1, 'retry': 0,
                  'connect_timeout_ms': 1500,
                   'socket_timeout_ms': contract['admission']['http_socket_timeout_ms']}]}
    return request, tuple(primary)
