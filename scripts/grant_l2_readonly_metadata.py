"""Grant the existing readonly account SELECT on the L2 probe database.

The managed P3 preflight (``check_execution_resources``) inspects the target
asset with its own probe account and only accepts an account whose database
privileges are exactly ``Select_priv``. The probe database ``etl_agent_l2``
previously had no such account, so a real managed submission could never pass
that preflight. This script adds exactly one database-scoped grant to the
already-existing readonly account ``source_ro``:

    GRANT SELECT ON etl_agent_l2.* TO source_ro

It does not touch the execution writer, does not add global privileges, and
does not touch any other database. The before/after grant state of both
accounts is written to an artifact so the change stays auditable.
"""
import argparse
import json
import re
from pathlib import Path

import pymysql

TARGET_DATABASE = 'etl_agent_l2'
READONLY_ACCOUNT = 'source_ro'
# Doris requires the <privilege>_priv spelling and a quoted account literal;
# the MySQL-style "GRANT SELECT ON db.* TO user" is rejected by the FE parser.
GRANT_STATEMENT = f"GRANT SELECT_PRIV ON {TARGET_DATABASE}.* TO '{READONLY_ACCOUNT}'@'%'"


def load_cnf(path):
    values = {}
    for line in Path(path).read_text(encoding='utf-8').splitlines():
        if '=' in line and not line.lstrip().startswith('#'):
            key, value = line.split('=', 1)
            values[key.strip()] = value.strip()
    return values


def grants(cursor, account):
    # Doris does not accept a bound parameter for SHOW GRANTS FOR; the account
    # is a fixed constant validated against a strict character allowlist.
    if not re.fullmatch(r'[a-z_][a-z0-9_]*', account):
        raise ValueError('unexpected_account_identifier')
    cursor.execute(f"SHOW GRANTS FOR '{account}'@'%'")
    columns = [column[0] for column in cursor.description]
    return [{key: (None if value is None else str(value)) for key, value in zip(columns, row)}
            for row in cursor.fetchall()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--admin', default='.runtime/connectors/doris-admin.cnf')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=19030)
    parser.add_argument('--output', type=Path, required=True)
    arguments = parser.parse_args()
    admin = load_cnf(arguments.admin)
    report = {'statement': GRANT_STATEMENT,
              'scope': 'database', 'target_database': TARGET_DATABASE, 'account': READONLY_ACCOUNT}
    with pymysql.connect(host=arguments.host, port=arguments.port, user=admin['user'],
                          password=admin['password'], connect_timeout=5, read_timeout=8,
                          charset='utf8mb4', autocommit=True) as connection:
        with connection.cursor() as cursor:
            report['before'] = {READONLY_ACCOUNT: grants(cursor, READONLY_ACCOUNT),
                                'etl_l2_probe_writer': grants(cursor, 'etl_l2_probe_writer')}
            cursor.execute(GRANT_STATEMENT)
            report['grant_applied'] = True
            report['after'] = {READONLY_ACCOUNT: grants(cursor, READONLY_ACCOUNT),
                               'etl_l2_probe_writer': grants(cursor, 'etl_l2_probe_writer')}
    writer = report['after']['etl_l2_probe_writer']
    report['writer_unchanged'] = writer == report['before']['etl_l2_probe_writer']
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps({'artifact': str(arguments.output), 'grant_applied': report['grant_applied'],
                      'writer_unchanged': report['writer_unchanged'],
                      'readonly_database_privs': [entry['DatabasePrivs'] for entry in report['after'][READONLY_ACCOUNT]]},
                     ensure_ascii=False))


if __name__ == '__main__':
    main()
