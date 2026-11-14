"""Read-only FE/BE membership evidence; credentials stay in the private client file."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import pymysql


def main(client_file, output):
    if Path(output).exists():
        raise ValueError('identity_probe_output_exists')
    result = {'level': 'doris_membership_protocol_probe', 'managed_l2_accepted': False,
              'observed_at': datetime.now(timezone.utc).isoformat()}
    with pymysql.connect(read_default_file=client_file, host='127.0.0.1', port=19030,
                         connect_timeout=3, read_timeout=4,
                         cursorclass=pymysql.cursors.DictCursor) as connection:
        for name, sql, selected in (
            ('frontends', 'SHOW FRONTENDS', ('Name', 'Host', 'EditLogPort', 'HttpPort', 'QueryPort',
                                            'Role', 'IsMaster', 'ClusterId', 'Alive', 'Version')),
            ('backends', 'SHOW BACKENDS', ('BackendId', 'Host', 'HeartbeatPort', 'BePort',
                                          'HttpPort', 'BrpcPort', 'Alive', 'Version'))):
            with connection.cursor() as cursor:
                cursor.execute(sql)
                result[name] = [{key: row[key] for key in selected if key in row}
                                for row in cursor.fetchall()]
    Path(output).write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
