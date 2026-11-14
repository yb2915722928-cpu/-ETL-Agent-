"""Read-only inspection of the real L2 endpoints (MySQL source, Doris target).

Loads credentials only in memory from the private probe configuration and the
readonly connector configuration. Prints no credentials and no business values:
only grant shapes, server versions and column metadata needed to freeze facts.
"""
import argparse
import json
import os
from pathlib import Path

import pymysql

HOST_ALIASES = {
    'connector-mysql': os.environ.get('REAL_L2_MYSQL_HOST', '127.0.0.1'),
    'connector-doris': os.environ.get('REAL_L2_DORIS_HOST', '127.0.0.1'),
}
PORT_OVERRIDES = {
    'connector-mysql': int(os.environ.get('REAL_L2_MYSQL_PORT', '13306')),
    'connector-doris': int(os.environ.get('REAL_L2_DORIS_PORT', '19030')),
}


def endpoint(fenodes):
    host = fenodes.split(':')[0]
    port = int(fenodes.split(':')[1]) if ':' in fenodes else 3306
    return HOST_ALIASES.get(host, host), PORT_OVERRIDES.get(host, port)


def grants(cursor):
    cursor.execute('SHOW GRANTS')
    return [str(row[0]) for row in cursor.fetchall()]


def inspect(config, readonly, source_table, target_table):
    source = config['source'][0]
    sink = config['sink'][0]
    report = {}
    with pymysql.connect(host='127.0.0.1', port=PORT_OVERRIDES['connector-mysql'], user=source['user'],
                          password=source['password'], database=source['url'].rsplit('/', 1)[-1].split('?')[0],
                          connect_timeout=5, read_timeout=8, charset='utf8mb4') as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT VERSION()')
            report['mysql_version'] = str(cursor.fetchone()[0])
            cursor.execute('SELECT CURRENT_ROLE()')
            report['mysql_current_role'] = str(cursor.fetchone()[0])
            report['mysql_grants'] = grants(cursor)
            cursor.execute('SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY '
                           'FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s '
                           'ORDER BY ORDINAL_POSITION', (source['url'].rsplit('/', 1)[-1].split('?')[0], source_table))
            report['source_columns'] = [{'name': row[0], 'type': row[1], 'nullable': row[2] == 'YES', 'key': row[3]}
                                        for row in cursor.fetchall()]
            cursor.execute('SELECT COUNT(*) FROM ' + source_table)
            report['source_rows'] = int(cursor.fetchone()[0])
    for label, user, password, database in (
            ('writer', sink['username'], sink['password'], sink['database']),
            ('readonly', readonly['user'], readonly['password'], sink['database'])):
        entry = {}
        host, port = endpoint(sink['fenodes'])
        try:
            with pymysql.connect(host=host, port=port, user=user,
                                  password=password, database=database,
                                  connect_timeout=5, read_timeout=8, charset='utf8mb4') as connection:
                with connection.cursor() as cursor:
                    cursor.execute('SELECT VERSION()')
                    entry['version'] = str(cursor.fetchone()[0])
                    cursor.execute('SHOW GRANTS')
                    description = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    entry['grant_columns'] = description
                    entry['grants'] = [{key: (None if value is None else str(value))
                                        for key, value in zip(description, row)} for row in rows]
                    cursor.execute('SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY '
                                   'FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s '
                                   'ORDER BY ORDINAL_POSITION', (database, target_table))
                    entry['target_columns'] = [{'name': row[0], 'type': row[1], 'nullable': row[2] == 'YES',
                                                'key': row[3]} for row in cursor.fetchall()]
                    cursor.execute('SELECT COUNT(*) FROM ' + target_table)
                    entry['target_rows'] = int(cursor.fetchone()[0])
        except pymysql.MySQLError as error:
            entry['error'] = type(error).__name__
            entry['error_number'] = getattr(error, 'args', [None])[0] if getattr(error, 'args', None) else None
        report['doris_' + label] = entry
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='.runtime/seatunnel/private-probe/doris-measured-config.json')
    parser.add_argument('--readonly', default='.runtime/connectors/doris-readonly.cnf')
    parser.add_argument('--source-table', default='customers')
    parser.add_argument('--target-table', required=True)
    parser.add_argument('--output', type=Path, required=True)
    arguments = parser.parse_args()
    config = json.loads(Path(arguments.config).read_text(encoding='utf-8'))
    readonly = {}
    for line in Path(arguments.readonly).read_text(encoding='utf-8').splitlines():
        if '=' in line and not line.lstrip().startswith('#'):
            key, value = line.split('=', 1)
            readonly[key.strip()] = value.strip()
    report = inspect(config, readonly, arguments.source_table, arguments.target_table)
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'artifact': str(arguments.output), 'source_rows': report['source_rows'],
                      'doris_writer_error': report['doris_writer'].get('error'),
                      'doris_readonly_error': report['doris_readonly'].get('error'),
                      'doris_readonly_target_rows': report['doris_readonly'].get('target_rows')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
