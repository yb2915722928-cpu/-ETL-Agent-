"""Finite approved-wire boundary, not an engine or real-L2 test."""
from copy import deepcopy
import json

from app.connector_policy import fingerprint
from app.real_contract import real_execution_contract
from app.real_job import ingress_job_request


def main():
    source = {'name': 'customers', 'columns': [
        {'name': 'id', 'type': 'int', 'nullable': False, 'key': 'PRI'},
        {'name': 'name', 'type': 'varchar(64)', 'nullable': True, 'key': ''},
        {'name': 'unselected_decimal', 'type': 'decimal(18,8)', 'nullable': True, 'key': ''}]}
    target = {'name': 'customers', 'columns': [
        {'name': 'name', 'type': 'varchar(64)', 'nullable': True}]}
    plan = {'source': {'table': 'customers'}, 'target': {'table': 'customers'},
            'mappings': [{'source': 'name', 'target': 'name', 'operation': 'direct'}]}
    resource = {'kind': 'mysql', 'host': 'source-relay', 'port': 3306, 'database': 'synthetic',
                'username': 'read_only', 'tables': ['customers']}
    contract = real_execution_contract(plan, source, target)
    facts = {'plan': plan, 'source': {'schema': source, 'resource_fingerprint': fingerprint(resource)},
             'target': {'schema': target}, 'real_execution_contract': contract,
             'runtime_supervision_contract': {'max_rows': 3}}
    job, primary = ingress_job_request('command-1', facts, resource, 'source-memory-only',
                                        'http://ingress:8001', 'ingress-token-memory-only-0123456789')
    assert primary == ('id',)
    assert job['source'][0]['query'] == 'SELECT `id`,`name` FROM `synthetic`.`customers` LIMIT 4'
    assert 'unselected_decimal' not in job['source'][0]['query']
    assert job['sink'][0]['retry'] == 0 and job['env']['job.retry.times'] == 0
    assert contract['admission'] == {'window_ms': 200, 'max_wait_ms': 2500, 'poll_ms': 50,
                                     'http_socket_timeout_ms': 5000}
    assert job['sink'][0]['socket_timeout_ms'] == 5000  # Includes unsent admission wait, not a Doris deadline.
    assert facts['runtime_supervision_contract']['max_rows'] == 3
    assert 'doris' not in json.dumps(job['sink']).lower()
    assert contract['rejected_schema'][0] == {'name': 'record_key', 'type': 'varchar(64)', 'nullable': False}
    configured_source = deepcopy(source)
    configured_source['columns'].append({'name': 'status', 'type': 'int', 'nullable': True, 'key': ''})
    configured_plan = {**plan, 'cleaning': [{'field': 'name', 'kind': 'trim'}],
                       'filters': [{'field': 'status', 'equals': 0}]}
    configured = real_execution_contract(configured_plan, configured_source, target)
    assert configured['source_columns'] == ['id', 'name', 'status']
    assert configured['cleaning'] == configured_plan['cleaning'] and configured['filters'] == configured_plan['filters']
    configured_facts = {**facts, 'plan': configured_plan, 'source': {**facts['source'], 'schema': configured_source},
                        'real_execution_contract': configured}
    configured_job, _ = ingress_job_request('command-1', configured_facts, resource, 'source-memory-only',
                                            'http://ingress:8001', 'ingress-token-memory-only-0123456789')
    assert configured_job['source'][0]['query'] == 'SELECT `id`,`name`,`status` FROM `synthetic`.`customers` LIMIT 4'
    for mutation in ('trim_primary', 'trim_integer', 'unknown_cleaner', 'filter_decimal',
                     'filter_float_value', 'filter_bool_value', 'filter_unknown', 'unapproved_filter'):
        changed = deepcopy(configured_facts)
        if mutation == 'trim_primary':
            changed['plan']['mappings'] = [{'source': 'id', 'target': 'name', 'operation': 'direct'}]
        elif mutation == 'trim_integer':
            changed['source']['schema']['columns'][1]['type'] = 'int'
        elif mutation == 'unknown_cleaner':
            changed['plan']['cleaning'][0]['kind'] = 'expression'
        elif mutation == 'filter_decimal':
            changed['plan']['filters'][0]['field'] = 'unselected_decimal'
        elif mutation == 'filter_float_value':
            changed['plan']['filters'][0]['equals'] = 0.5
        elif mutation == 'filter_bool_value':
            changed['plan']['filters'][0]['equals'] = False
        elif mutation == 'filter_unknown':
            changed['plan']['filters'][0]['field'] = 'missing'
        else:
            changed['plan']['filters'][0]['equals'] = 1
        try:
            ingress_job_request('command-1', changed, resource, 'source-memory-only',
                                'http://ingress:8001', 'ingress-token-memory-only-0123456789')
        except ValueError:
            pass
        else:
            raise AssertionError(mutation)
    for mutation in ('no_primary', 'nullable_primary', 'decimal_mapping', 'contract_tampered'):
        changed = deepcopy(facts)
        if mutation == 'no_primary':
            changed['source']['schema']['columns'][0]['key'] = ''
        elif mutation == 'nullable_primary':
            changed['source']['schema']['columns'][0]['nullable'] = True
        elif mutation == 'decimal_mapping':
            changed['plan']['mappings'][0]['source'] = 'unselected_decimal'
        else:
            changed['real_execution_contract']['rejected_schema'][0]['nullable'] = True
        try:
            ingress_job_request('command-1', changed, resource, 'source-memory-only',
                                  'http://ingress:8001', 'ingress-token-memory-only-0123456789')
        except ValueError:
            pass
        else:
            raise AssertionError(mutation)
    print(json.dumps({'passed': ['unmapped primary key retained; unselected unproven wire types omitted',
        'bounded scan uses max_rows+1; engine restart and HTTP retry disabled',
        'missing/nullable identity, lossy wire type and changed approved error schema denied'],
        'level': 'real_contract_boundary_fixture', 'real_l2_qualified': False}))


if __name__ == '__main__':
    main()
