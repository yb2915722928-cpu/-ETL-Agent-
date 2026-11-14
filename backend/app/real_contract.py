"""Versioned, approval-visible contract for the finite real ingress path."""
from .studio_plan import column_type, quality_configuration


REAL_EXECUTION_VERSION = 'etl-real-ingress-v1'
REAL_ADMISSION = {'window_ms': 200, 'max_wait_ms': 2500, 'poll_ms': 50,
                  'http_socket_timeout_ms': 5000}
ERROR_RECORD_COLUMNS = [
    {'name': 'record_key', 'type': 'varchar(64)', 'nullable': False},
    {'name': 'error_codes', 'type': 'varchar(8192)', 'nullable': False},
    {'name': 'field_names', 'type': 'varchar(8192)', 'nullable': False},
    {'name': 'record_sha256', 'type': 'varchar(64)', 'nullable': False},
    {'name': 'run_id', 'type': 'varchar(36)', 'nullable': False},
    {'name': 'contract_sha256', 'type': 'varchar(64)', 'nullable': False},
]


def real_execution_contract(plan, source_schema, target_schema):
    """Freeze routes and source identity before approval, without opening secrets.

    The first transport dialect accepts integer/text projection only. Decimal,
    floating and temporal encodings require their own proven wire contract;
    Python JSON floating conversion cannot claim lossless decimal transport.
    """
    columns = {column['name']: column for column in source_schema['columns']}
    primary = [column['name'] for column in source_schema['columns'] if column.get('key') == 'PRI']
    if not primary or any(columns[name]['nullable'] is not False for name in primary):
        raise ValueError('real_source_stable_primary_key_required')
    cleaning, filters = quality_configuration(plan, source_schema, target_schema)
    selected = sorted(set(primary) | {mapping['source'] for mapping in plan['mappings']}
                      | {condition['field'] for condition in filters})
    for name in selected:
        datatype = column_type(columns[name]['type'])
        if datatype is None or datatype[0] not in ('integer', 'text'):
            raise ValueError('real_source_wire_type_unverified')
    for column in target_schema['columns']:
        datatype = column_type(column['type'])
        if datatype is None or datatype[0] not in ('integer', 'text'):
            raise ValueError('real_target_wire_type_unverified')
    return {'version': REAL_EXECUTION_VERSION, 'source_primary_key': primary,
            'source_columns': selected, 'quality_stage_version': 'quality-v1',
            'cleaning': cleaning, 'filters': filters, 'valid_schema': target_schema['columns'],
            'rejected_schema': [dict(column) for column in ERROR_RECORD_COLUMNS],
            'valid_object_run_suffix': '', 'rejected_object_run_suffix': '-error',
            'sink_protocol': 'single-json-record-original-doris-receipt-v1', 'text_encoding': 'UTF-8',
            'admission': dict(REAL_ADMISSION)}
