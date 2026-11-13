"""Fixed P3 read-only tool exit. No caller-provided SQL, URLs or sample limits."""
import re
import time

import pymysql

SAMPLE_ROWS = 5
QUERY_TIMEOUT_SECONDS = 3
TOTAL_SECONDS = 20
MAX_TABLES = 8
MAX_COLUMNS = 64
POLICY_VERSION = 'p3-connectors-v1'


class ProbeFailure(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__({'external_unavailable': '外部连接失败，请核对受管账号、网络及服务健康状态',
                          'readonly_required': '探查账号必须仅有只读权限且不得继承可写角色',
                          'budget_exceeded': '探查超过固定预算',
                          'schema_unavailable': '白名单表或字段不可用'}[code])


def identifier(value):
    # Deployment policy is trusted, but identifiers must still be SQL identifiers.
    if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]{0,63}', value):
        raise ProbeFailure('schema_unavailable')
    return '`' + value + '`'


def inspect_resource(resource, password):
    started = time.monotonic()
    queries = 0
    try:
        with pymysql.connect(host=resource['host'], port=resource['port'],
                             user=resource['username'], password=password, database=resource['database'],
                             connect_timeout=3, read_timeout=4, write_timeout=3,
                             charset='utf8mb4', autocommit=False) as connection:
            with connection.cursor() as cursor:
                def query(sql, params=None):
                    nonlocal queries
                    if time.monotonic() - started > TOTAL_SECONDS or queries >= 32:
                        raise ProbeFailure('budget_exceeded')
                    queries += 1
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                    if time.monotonic() - started > TOTAL_SECONDS:
                        raise ProbeFailure('budget_exceeded')
                    return rows

                version = str(query('SELECT VERSION()')[0][0])
                grants = query('SHOW GRANTS')
                grant_columns = [column[0] for column in cursor.description]
                if resource['kind'] == 'mysql':
                    if query('SELECT CURRENT_ROLE()')[0][0] != 'NONE':
                        raise ProbeFailure('readonly_required')
                    for row in grants:
                        grant = str(row[0])
                        match = re.match(r'^GRANT (.+?) ON ', grant)
                        if (not match or 'WITH GRANT OPTION' in grant or
                                not set(match[1].split(', ')) <= {'USAGE', 'SELECT', 'SHOW VIEW'}):
                            raise ProbeFailure('readonly_required')
                    query('SET SESSION MAX_EXECUTION_TIME=3000')
                    query('SET TRANSACTION READ ONLY')
                else:
                    # Doris' SHOW GRANTS format is checked before reading any business rows.
                    verify_doris_readonly(grants, grant_columns)
                    query('SET query_timeout=3')
                tables = []
                for table in resource['tables']:
                    columns = query('SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY '
                                    'FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s '
                                    'ORDER BY ORDINAL_POSITION LIMIT 65', (resource['database'], table))
                    if not columns or len(columns) > MAX_COLUMNS:
                        raise ProbeFailure('schema_unavailable')
                    stats = query('SELECT TABLE_ROWS,DATA_LENGTH FROM information_schema.TABLES '
                                  'WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s', (resource['database'], table))
                    # Mask inside SQL: business values never cross the tool boundary.
                    expressions = ','.join('CASE WHEN ' + identifier(c[0]) + ' IS NULL THEN NULL '
                                            "ELSE '[已脱敏]' END AS " + identifier(c[0]) for c in columns)
                    rows = query('SELECT ' + expressions + ' FROM ' + identifier(resource['database']) + '.'
                                 + identifier(table) + ' LIMIT 5')
                    tables.append({'name': table,
                                   'columns': [{'name': c[0], 'type': c[1], 'nullable': c[2] == 'YES',
                                                'key': c[3]} for c in columns],
                                   'approx_rows': int(stats[0][0]) if stats and stats[0][0] is not None else None,
                                   'approx_bytes': int(stats[0][1]) if stats and stats[0][1] is not None else None,
                                   'statistics_kind': 'server_estimate',
                                   'samples': [dict(zip((c[0] for c in columns), row)) for row in rows]})
                connection.rollback()
                return {'server_version': version,
                        'version_kind': 'mysql_protocol_compatibility' if resource['kind'] == 'doris' else 'server',
                        'database': resource['database'], 'tables': tables,
                        'budget': {'sample_rows_per_table': SAMPLE_ROWS, 'max_tables': MAX_TABLES,
                                   'query_timeout_seconds': QUERY_TIMEOUT_SECONDS, 'max_seconds': TOTAL_SECONDS,
                                   'max_queries': 32, 'queries_used': queries}, 'policy_version': POLICY_VERSION}
    except pymysql.MySQLError:
        raise ProbeFailure('external_unavailable') from None


def verify_doris_readonly(grants, columns):
    # Doris 3.0.8 grants normal-workload Usage_priv to all users. That permits
    # scheduling queries, not writes; no other non-SELECT privileges or roles pass.
    has_select = False
    if not grants or 'Roles' not in columns or 'DatabasePrivs' not in columns:
        raise ProbeFailure('readonly_required')
    for values in grants:
        row = dict(zip(columns, values))
        if row['Roles'] not in (None, '', 'NULL'):
            raise ProbeFailure('readonly_required')
        for field, value in row.items():
            if not field.endswith('Privs') or value in (None, '', 'NULL'):
                continue
            if field == 'WorkloadGroupPrivs' and value == 'normal: Usage_priv':
                continue
            privileges = re.findall(r'\b\w+_priv\b', value)
            if not privileges or set(privileges) != {'Select_priv'}:
                raise ProbeFailure('readonly_required')
            has_select = True
    if not has_select:
        raise ProbeFailure('readonly_required')
