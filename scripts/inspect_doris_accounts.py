"""Read-only inspection of Doris account catalog for the real L2 probe design.

Connects with the probe writer account that already holds SELECT on
information_schema and the internal mysql catalog. Never selects password
material and never changes grants: it only reports which accounts exist and
which database-level privileges they hold, so the L2 runner can pick a genuine
readonly metadata account instead of granting anything new.
"""
import argparse
import json
from pathlib import Path

import pymysql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='.runtime/seatunnel/private-probe/doris-measured-config.json')
    parser.add_argument('--output', type=Path, required=True)
    arguments = parser.parse_args()
    config = json.loads(Path(arguments.config).read_text(encoding='utf-8'))
    sink = config['sink'][0]
    host, port = '127.0.0.1', 19030
    report = {}
    with pymysql.connect(host=host, port=port, user=sink['username'], password=sink['password'],
                         database=sink['database'], connect_timeout=5, read_timeout=8, charset='utf8mb4') as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT User, Host FROM mysql.user ORDER BY User')
            report['users'] = [{'user': row[0], 'host': row[1]} for row in cursor.fetchall()]
            cursor.execute('SELECT Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, '
                           'Create_priv, Drop_priv, Grant_priv, Alter_priv, Index_priv FROM mysql.tables_priv '
                           'ORDER BY Db, User')
            columns = [column[0] for column in cursor.description]
            report['table_grants'] = [dict(zip(columns, [None if value is None else str(value)
                                                         for value in row])) for row in cursor.fetchall()]
            cursor.execute("SHOW DATABASES")
            report['databases'] = [row[0] for row in cursor.fetchall()]
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'artifact': str(arguments.output), 'users': [entry['user'] for entry in report['users']],
                      'databases': report['databases']}, ensure_ascii=False))

if __name__ == '__main__':
    main()
