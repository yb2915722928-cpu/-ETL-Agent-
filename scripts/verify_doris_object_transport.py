"""One synthetic Doris object/load protocol probe; no managed-L2 claim.

Keeps its uniquely named object for audit. Never publishes, grants, retries a
mutation, or changes an existing table. Credentials are read only in memory.
"""
import json
import os
from pathlib import Path
import sys
import uuid

import pymysql

# Import-time application globals only; this protocol probe never connects to PG/Redis.
os.environ.setdefault('DATABASE_URL', 'postgresql+psycopg://unused:unused@127.0.0.1:1/unused')
os.environ.setdefault('REDIS_URL', 'redis://127.0.0.1:1/0')
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from app.connector_policy import fingerprint
from app.doris_objects import DorisObjects
from app.doris_transport import DorisStreamTransport, object_identity_id
from app.managed_load import FrozenLoadBinding, _parse_receipt


def main(config_path, output):
    if Path(output).exists():
        raise ValueError('探针输出已存在；保留原意图并为新探针使用新路径')
    sink = json.loads(Path(config_path).read_text(encoding='utf-8'))['sink'][0]
    command_id, run_id = str(uuid.uuid4()), str(uuid.uuid4())
    # This is a diagnostic route, NOT proof of exclusive routing or authenticated
    # cluster provenance. Production must freeze those independent deployment facts.
    resource = {'kind': 'doris', 'direction': 'target', 'host': '127.0.0.1', 'port': 19030,
                'database': sink['database'], 'username': sink['username'],
                'physical_cluster_id': 'local-doris-protocol-probe-unqualified', 'tables': ['customers']}
    objects = DorisObjects(resource, sink['password'])
    report = {'level': 'real_object_transport_protocol_probe', 'managed_l2_accepted': False,
              'run_id': run_id, 'command_id': command_id, 'phase': 'create_intent',
              'exclusive_network_verified': False, 'trusted_backend_mapping_verified': False}
    def persist():
        Path(output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    persist()
    columns = [{'name': 'id', 'type': 'int', 'nullable': False},
               {'name': 'name', 'type': 'varchar(64)', 'nullable': True}]
    identity = objects.create_shadow('customers', run_id, command_id, columns)
    report.update(phase='created', object=identity)
    persist()
    binding = FrozenLoadBinding(command_id, run_id, '1', fingerprint({'command': command_id}),
        fingerprint({'protocol_probe': True}), resource['physical_cluster_id'], resource['database'],
        identity['table'], object_identity_id(identity), 'memory-only-protocol-reference', 1,
        ('local-port-mapping-unqualified',))
    route = {'host': '127.0.0.1', 'port': 18040, 'backend_id': binding.backend_ids[0]}
    def resolve(current):
        assert current == binding
        return resource, sink['password'], objects.inspect_shadow('customers', run_id, command_id), route
    body = b'{"id":1,"name":"synthetic-A"}\n{"id":2,"name":null}\n'
    label = 'etl_' + fingerprint({'command': command_id, 'batch': 0})
    report.update(phase='load_intent', label=label, body_sha256=__import__('hashlib').sha256(body).hexdigest())
    persist()  # Never repeat this PUT if the response/next process step is lost.
    result = DorisStreamTransport(resolve).send(binding, label, body)
    receipt = _parse_receipt(result.raw, label)
    report.update(phase='receipt_persisted', receipt=receipt,
                  response_sha256=__import__('hashlib').sha256(result.raw).hexdigest())
    persist()
    assert receipt['Status'] == 'Success' and receipt['NumberLoadedRows'] == 2
    assert receipt['NumberFilteredRows'] == 0 and receipt['LoadBytes'] > 0
    assert objects.matches('customers', run_id, command_id, identity)
    with pymysql.connect(host=resource['host'], port=resource['port'], user=resource['username'],
                         password=sink['password'], database=resource['database'], connect_timeout=3,
                         read_timeout=4) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT id,name FROM `' + identity['table'] + '` ORDER BY id')
            assert cursor.fetchall() == ((1, 'synthetic-A'), (2, None))
    report.update(phase='verified', synthetic_result_matches=True, identity_unchanged=True,
                  measured_bytes=receipt['LoadBytes'], retained_for_audit=True)
    persist()
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
