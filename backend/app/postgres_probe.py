"""PostgreSQL source adapter: effective privilege audit precedes fixed reads."""
import os
import time

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

from .connector_probe import (MAX_COLUMNS, MAX_TABLES, POLICY_VERSION, QUERY_TIMEOUT_SECONDS,
                              SAMPLE_ROWS, TOTAL_SECONDS, ProbeFailure)


def inspect_postgresql(resource, password):
    control = make_url(os.environ['DATABASE_URL']) if os.environ.get('DATABASE_URL') else None
    if control and (resource['username'] == control.username or resource['database'] == control.database):
        raise ProbeFailure('readonly_required')
    started, queries = time.monotonic(), 0
    try:
        with psycopg.connect(host=resource['host'], port=resource['port'], dbname=resource['database'],
                             user=resource['username'], password=password, connect_timeout=3,
                             options='-c statement_timeout=3000 -c lock_timeout=3000 -c default_transaction_read_only=on') as connection:
            def query(statement, params=None):
                nonlocal queries
                if time.monotonic() - started > TOTAL_SECONDS or queries >= 32:
                    raise ProbeFailure('budget_exceeded')
                queries += 1
                rows = connection.execute(statement, params).fetchall()
                if time.monotonic() - started > TOTAL_SECONDS:
                    raise ProbeFailure('budget_exceeded')
                return rows

            version, unsafe = query("""SELECT version(), rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication
                OR rolbypassrls OR EXISTS(SELECT 1 FROM pg_auth_members WHERE member=pg_roles.oid)
                OR has_database_privilege(current_user,current_database(),'CREATE')
                OR has_database_privilege(current_user,current_database(),'TEMP')
                FROM pg_roles WHERE rolname=current_user""")[0]
            if unsafe:
                raise ProbeFailure('readonly_required')
            if query("""SELECT 1 FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname <> 'information_schema'
                        AND has_schema_privilege(oid,'CREATE') LIMIT 1"""):
                raise ProbeFailure('readonly_required')
            if query("""SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND
                ((c.relkind IN ('r','p','v','m','f') AND
                  (has_table_privilege(c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR
                   has_any_column_privilege(c.oid,'INSERT,UPDATE,REFERENCES') OR
                   pg_has_role(c.relowner,'USAGE'))) OR
                 (c.relkind='S' AND has_sequence_privilege(c.oid,'USAGE,UPDATE'))) LIMIT 1"""):
                raise ProbeFailure('readonly_required')
            # Executable application routines can perform writes or elevate privileges.
            if query("""SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
                AND has_function_privilege(p.oid,'EXECUTE') LIMIT 1"""):
                raise ProbeFailure('readonly_required')
            tables = []
            schema = resource['schema']
            for table in resource['tables']:
                columns = query("""SELECT a.attname,format_type(a.atttypid,a.atttypmod),NOT a.attnotnull,
                    CASE WHEN EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conrelid=c.oid
                         AND k.contype='p' AND a.attnum=ANY(k.conkey)) THEN 'PRI' ELSE '' END
                    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    JOIN pg_attribute a ON a.attrelid=c.oid
                    WHERE n.nspname=%s AND c.relname=%s AND a.attnum>0 AND NOT a.attisdropped
                    ORDER BY a.attnum LIMIT 65""", (schema, table))
                if not columns or len(columns) > MAX_COLUMNS:
                    raise ProbeFailure('schema_unavailable')
                stats = query("""SELECT CASE WHEN c.reltuples<0 THEN NULL ELSE c.reltuples::bigint END,
                    CASE WHEN c.relkind IN ('r','p','m') THEN pg_total_relation_size(c.oid) ELSE NULL END
                    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname=%s AND c.relname=%s""", (schema, table))
                expressions = sql.SQL(',').join(sql.SQL("CASE WHEN {} IS NULL THEN NULL ELSE '[已脱敏]' END AS {}")
                                               .format(sql.Identifier(c[0]), sql.Identifier(c[0])) for c in columns)
                rows = query(sql.SQL('SELECT {} FROM {}.{} LIMIT 5').format(
                    expressions, sql.Identifier(schema), sql.Identifier(table)))
                tables.append({'name': table, 'schema': schema,
                               'columns': [{'name': c[0], 'type': c[1], 'nullable': c[2], 'key': c[3]} for c in columns],
                               'approx_rows': stats[0][0], 'approx_bytes': stats[0][1],
                               'statistics_kind': 'server_estimate',
                               'samples': [dict(zip((c[0] for c in columns), row)) for row in rows]})
            connection.rollback()
            return {'server_version': version, 'version_kind': 'server', 'database': resource['database'],
                    'schema': schema, 'tables': tables,
                    'budget': {'sample_rows_per_table': SAMPLE_ROWS, 'max_tables': MAX_TABLES,
                               'query_timeout_seconds': QUERY_TIMEOUT_SECONDS, 'max_seconds': TOTAL_SECONDS,
                               'max_queries': 32, 'queries_used': queries}, 'policy_version': POLICY_VERSION}
    except psycopg.Error:
        raise ProbeFailure('external_unavailable') from None
