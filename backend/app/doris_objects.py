"""Doris object identity and narrowly scoped shadow preparation.

Names and row counts are insufficient identity. A managed object has a unique
creation marker, a frozen schema and the physical partition IDs returned by
Doris. This adapter does not authorize mutations or infer sink quiescence.
The caller persists its operation intent before calling create_shadow once.
"""
import re
from contextlib import contextmanager

import pymysql

from .connector_policy import fingerprint
from .connector_probe import identifier
from .real_engine import shadow_table
from .studio_plan import column_type


class DorisObjectUnavailable(Exception):
    pass


def object_marker(command_id):
    # Only a fixed, safe identifier is inserted into a SQL comment literal.
    if not re.fullmatch(r'[a-zA-Z0-9-]{1,64}', command_id):
        raise ValueError('无效的受管命令身份')
    return 'etl_agent_object:' + command_id


def table_schema(columns):
    return [{'name': item['name'], 'type': item['type'].lower().strip(),
             'nullable': item['nullable']} for item in columns]


class DorisObjects:
    def __init__(self, resource, secret):
        if resource['kind'] != 'doris' or not resource.get('physical_cluster_id'):
            raise ValueError('Doris 物理集群身份尚未冻结')
        self.resource, self._secret = resource, secret

    @contextmanager
    def _cursor(self):
        try:
            with pymysql.connect(host=self.resource['host'], port=self.resource['port'],
                                 user=self.resource['username'], password=self._secret,
                                 database=self.resource['database'], connect_timeout=3,
                                 read_timeout=4, write_timeout=3, charset='utf8mb4') as connection:
                with connection.cursor() as cursor:
                    cursor.execute('SET query_timeout=3')
                    yield cursor
        except pymysql.MySQLError:
            # SQL messages and server details can contain sensitive values.
            raise DorisObjectUnavailable('Doris 对象操作结果不可确认；保留原意图与隔离') from None

    def _scope(self, target, run_id):
        if target not in self.resource['tables']:
            raise ValueError('目标表不在冻结的部署范围内')
        return shadow_table(target, run_id)

    def inspect_shadow(self, target, run_id, command_id):
        table = self._scope(target, run_id)
        identity = self._inspect(table)
        if identity['marker'] != object_marker(command_id):
            raise DorisObjectUnavailable('影子表创建标记不匹配；保持隔离')
        return identity

    def _inspect(self, table):
        database = self.resource['database']
        qualified = identifier(database) + '.' + identifier(table)
        with self._cursor() as cursor:
            cursor.execute('SHOW CREATE TABLE ' + qualified)
            row = cursor.fetchone()
            if not row:
                raise DorisObjectUnavailable('对象身份不可确认')
            ddl = str(row[1])
            marker = re.search(r'COMMENT\s+[\"\'](etl_agent_object:[a-zA-Z0-9-]{1,64})[\"\']', ddl)
            cursor.execute('SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE FROM information_schema.COLUMNS '
                           'WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION',
                           (database, table))
            schema = [{'name': item[0], 'type': str(item[1]).lower().strip(), 'nullable': item[2] == 'YES'}
                      for item in cursor.fetchall()]
            cursor.execute('SHOW PARTITIONS FROM ' + qualified)
            columns = [column[0] for column in cursor.description]
            if 'PartitionId' not in columns:
                raise DorisObjectUnavailable('Doris 未提供物理分区身份')
            partition_ids = sorted(str(item[columns.index('PartitionId')]) for item in cursor.fetchall())
            if not schema or not partition_ids or len(partition_ids) != len(set(partition_ids)):
                raise DorisObjectUnavailable('Doris 对象结构或物理分区身份不完整')
        return {'physical_cluster_id': self.resource['physical_cluster_id'], 'database': database,
                'table': table, 'marker': marker[1] if marker else None,
                'partition_ids': partition_ids, 'schema': schema, 'schema_sha256': fingerprint(schema)}

    def inspect_stable(self, target):
        """Read the existing approved table, including unmanaged physical identity.

        A missing managed marker is represented as None, never invented. Partition
        IDs and the complete schema remain mandatory; names/row counts are not ID.
        """
        if target not in self.resource['tables']:
            raise ValueError('目标表不在冻结的部署范围内')
        return self._inspect(target)

    def observe_publication(self, target, shadow):
        if target not in self.resource['tables'] or not re.fullmatch(r'shadow_[0-9a-f]{32}', shadow):
            raise ValueError('发布对象不在受限目标范围内')
        return {'stable': self._inspect(target), 'shadow': self._inspect(shadow)}

    def swap_existing(self, target, shadow, *, before_mutation=None):
        """One SQL, no retry, no IF EXISTS, no bootstrap or destructive cleanup.

        Caller must first persist dispatch reservation and prove current frozen
        publication authority. Server privilege enforcement remains independent.
        """
        if target not in self.resource['tables'] or not re.fullmatch(r'shadow_[0-9a-f]{32}', shadow):
            raise ValueError('发布对象不在受限目标范围内')
        statement = ('ALTER TABLE ' + identifier(self.resource['database']) + '.' + identifier(target) +
                     ' REPLACE WITH TABLE ' + identifier(shadow) + ' PROPERTIES ("swap"="true")')
        with self._cursor() as cursor:
            # _cursor already opened the connection and set query_timeout. The
            # last bounded authority observation must include that elapsed time.
            if before_mutation is not None:
                before_mutation()
            cursor.execute(statement)

    def publication_privileges(self):
        """Read grants without granting or relaxing the metadata probe policy."""
        with self._cursor() as cursor:
            cursor.execute('SHOW GRANTS')
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def create_shadow(self, target, run_id, command_id, columns, *, before_mutation=None):
        """One external attempt. No IF NOT EXISTS and no retry on unknown outcome."""
        table = self._scope(target, run_id)
        if not columns:
            raise ValueError('影子表缺少冻结结构')
        definitions = []
        for column in columns:
            datatype = column['type'].lower().strip()
            if column_type(datatype) is None or not isinstance(column['nullable'], bool):
                raise ValueError('影子表字段类型尚未支持')
            definitions.append(identifier(column['name']) + ' ' + datatype +
                               (' NULL' if column['nullable'] else ' NOT NULL'))
        key = identifier(columns[0]['name'])
        if column_type(columns[0]['type'])[0] not in ('integer', 'decimal', 'text', 'date', 'datetime'):
            raise ValueError('影子表首字段不支持受管分布键')
        statement = ('CREATE TABLE ' + identifier(self.resource['database']) + '.' + identifier(table) +
                     ' (' + ','.join(definitions) + ') DUPLICATE KEY (' + key +
                     ") COMMENT '" + object_marker(command_id) + "' DISTRIBUTED BY HASH (" + key +
                     ') BUCKETS 1 PROPERTIES ("replication_num"="1")')
        with self._cursor() as cursor:
            if before_mutation is not None:
                before_mutation()
            cursor.execute(statement)
        return self.inspect_shadow(target, run_id, command_id)

    def matches(self, target, run_id, command_id, expected):
        observed = self.inspect_shadow(target, run_id, command_id)
        return observed == expected
