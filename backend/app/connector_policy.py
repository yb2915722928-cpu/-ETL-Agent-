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
            if resource['kind'] not in ('mysql', 'doris', 'postgresql', 'csv'):
                raise ValueError('连接白名单类型不合法或尚未实现')
            resource['direction'] = 'target' if resource['kind'] == 'doris' else 'source'
            writer = resource.get('execution_writer')
            if writer is not None:
                if (resource['kind'] != 'doris' or not isinstance(writer, dict)
                        or not {'username'} <= set(writer) <= {'username', 'lifecycle_privileges'}
                        or not isinstance(writer.get('lifecycle_privileges', []), list)
                        or not set(writer.get('lifecycle_privileges', [])) <= {'Alter_priv', 'Drop_priv'}
                        or len(writer.get('lifecycle_privileges', [])) != len(set(writer.get('lifecycle_privileges', [])))
                        or not isinstance(writer['username'], str)
                        or not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]{0,63}', writer['username'])
                        or writer['username'] == resource['username']):
                    raise ValueError('execution_writer仅允许同一Doris目标的独立部署用户名')
            if os.environ.get('EXECUTION_ADAPTER', 'disabled') in ('l1', 'real') and resource['kind'] == 'doris':
                if not isinstance(resource.get('physical_cluster_id'), str) or not resource['physical_cluster_id']:
                    raise ValueError('L1/real Doris 白名单必须声明稳定 physical_cluster_id，别名共享同一标识')
            if resource['kind'] == 'csv':
                root = Path(resource['root'])
                if not root.is_absolute() or not resource['projects'] or not all(isinstance(p, str) and p for p in resource['projects']):
                    raise ValueError('CSV白名单需要绝对受管目录和明确项目ID')
                resource['database'], resource['tables'] = 'managed_csv', ['data']
            elif resource['kind'] == 'postgresql':
                identifier(resource['schema'])
            if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_-]{0,63}', resource_id):
                raise ValueError('连接白名单资源ID不合法')
            identifier(resource['database'])
            if resource['kind'] != 'csv' and (not resource['host'] or not 1 <= resource['port'] <= 65535 or not resource['username']):
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
