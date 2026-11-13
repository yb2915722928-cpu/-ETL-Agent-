"""Only deployment roots and server-generated project/storage IDs address files."""
import hashlib
from pathlib import Path

from .connector_probe import ProbeFailure
from .csv_probe import MAX_CSV_BYTES, SourceParseFailure, inspect_csv


def storage_path(resource, tenant_id, project_id, storage_id):
    root = Path(resource['root']).resolve()
    path = (root / tenant_id / project_id / (storage_id + '.csv')).resolve()
    if not path.is_relative_to(root):
        raise ProbeFailure('schema_unavailable')
    return path


def save_csv(resource, connection, data):
    path = storage_path(resource, connection.tenant_id, connection.project_id, connection.storage_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as output:
        output.write(data)


def inspect_stored_csv(resource, connection):
    source_ref = 'csv:' + connection.storage_id
    try:
        path = storage_path(resource, connection.tenant_id, connection.project_id, connection.storage_id)
        with path.open('rb') as source:
            data = source.read(MAX_CSV_BYTES + 1)
    except OSError:
        raise SourceParseFailure(source_ref, 1, 1, '受管文件缺失或无法读取') from None
    if len(data) != connection.source_bytes or hashlib.sha256(data).hexdigest() != connection.source_sha256:
        raise SourceParseFailure(source_ref, 1, 1, '受管文件与登记指纹不符，请重新上传')
    return inspect_csv(data, source_ref)
