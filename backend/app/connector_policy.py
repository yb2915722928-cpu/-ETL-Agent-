"""Deployment-owned resource whitelist. This is not a user-editable SQL policy."""
import hashlib
import json
import os
import re
from pathlib import Path

from .connector_probe import MAX_TABLES, identifier


class ResourcePolicy:
    def __init__(self):
        self.path = Path(os.environ['CONNECTOR_RESOURCES_FILE'])
        self.resources()  # Invalid deployment configuration prevents startup.

    def resources(self):
        resources = json.loads(self.path.read_text(encoding='utf-8'))
        for resource_id, resource in resources.items():
            if resource['kind'] not in ('mysql', 'doris'):
                raise ValueError('连接白名单类型不合法或尚未实现')
            resource['direction'] = 'source' if resource['kind'] == 'mysql' else 'target'
            if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_-]{0,63}', resource_id):
                raise ValueError('连接白名单资源ID不合法')
            identifier(resource['database'])
            if not resource['host'] or not 1 <= resource['port'] <= 65535 or not resource['username']:
                raise ValueError('连接白名单地址或账号不合法')
            if not 1 <= len(resource['tables']) <= MAX_TABLES or len(set(resource['tables'])) != len(resource['tables']):
                raise ValueError('连接白名单表数必须为1至8且不得重复')
            for table in resource['tables']:
                identifier(table)
        return resources

    def get(self, resource_id):
        return self.resources().get(resource_id)


def fingerprint(resource):
    return hashlib.sha256(json.dumps(resource, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def resource_view(resource_id, resource):
    return {'id': resource_id, 'kind': resource['kind'], 'direction': resource['direction'],
            'database': resource['database'], 'tables': resource['tables'], 'fingerprint': fingerprint(resource)}
