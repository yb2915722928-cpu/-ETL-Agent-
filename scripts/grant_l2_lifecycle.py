"""Provision the two lifecycle rights inside the existing synthetic L2 database.

The public managed controls still independently require explicit deployment
lifecycle_privileges, frozen references, current roles, original physical IDs,
complete counters and live exclusive routes. This script does not alter data.
"""
import argparse
import json
from pathlib import Path

import pymysql

from grant_l2_readonly_metadata import grants, load_cnf


DATABASE = 'etl_agent_l2'
WRITER = 'etl_l2_probe_writer'
PROBE = 'source_ro'
RIGHTS = ('Alter_priv', 'Drop_priv')
STATEMENT = "GRANT Alter_priv, Drop_priv ON etl_agent_l2.* TO 'etl_l2_probe_writer'@'%'"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('refuse_to_overwrite_lifecycle_grant_evidence')
    admin = load_cnf('.runtime/connectors/doris-admin.cnf')
    report = {'scope': DATABASE + '.*', 'account': WRITER, 'privileges': list(RIGHTS),
              'statement': STATEMENT, 'phase': 'intent', 'real_l2_qualified': False}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    def persist():
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    with pymysql.connect(host='127.0.0.1', port=19030, user=admin['user'],
                         password=admin['password'], connect_timeout=5,
                         read_timeout=8, charset='utf8mb4', autocommit=True) as connection:
        with connection.cursor() as cursor:
            before = {WRITER: grants(cursor, WRITER), PROBE: grants(cursor, PROBE)}
            report['before'] = before
            persist()
            cursor.execute(STATEMENT)
            after = {WRITER: grants(cursor, WRITER), PROBE: grants(cursor, PROBE)}
            report.update(after=after, phase='granted', probe_unchanged=before[PROBE] == after[PROBE])
            persist()
            if not report['probe_unchanged']:
                raise ValueError('readonly_probe_grants_changed')
    print(json.dumps({'phase': report['phase'], 'scope': report['scope'],
                      'privileges': report['privileges'], 'probe_unchanged': report['probe_unchanged']}))


if __name__ == '__main__':
    main()
