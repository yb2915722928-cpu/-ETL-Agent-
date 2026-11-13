"""Prepare independent PG/CSV demo inputs. Runtime secrets never enter tracked files/stdout."""
import argparse
import json
import os
from pathlib import Path
import secrets

import psycopg
from psycopg import sql


def prepare(directory):
    directory.mkdir(parents=True, exist_ok=True)
    private = directory / 'secrets.json'
    config = json.loads(private.read_text(encoding='utf-8')) if private.exists() else {}
    if 'postgresql' not in config:
        config['postgresql'] = {'host': 'connector-postgres', 'port': 5432, 'host_port': 15432,
            'database': 'connector_source', 'schema': 'public', 'username': 'source_ro',
            'password': secrets.token_urlsafe(32), 'admin_username': 'connector_admin',
            'admin_password': secrets.token_urlsafe(32)}
        private.write_text(json.dumps(config, indent=2) + '\n', encoding='utf-8')
        os.chmod(private, 0o600)
    pg = config['postgresql']
    env = directory / 'postgres.env'
    if not env.exists():
        env.write_text(f"POSTGRES_USER={pg['admin_username']}\nPOSTGRES_DB={pg['database']}\n"
                       f"POSTGRES_PASSWORD={pg['admin_password']}\n", encoding='utf-8')
        os.chmod(env, 0o600)
    fixtures = directory / 'csv'
    fixtures.mkdir(exist_ok=True)
    inputs = {
        'customers-utf8.csv': 'id,email,full_name,balance,active,notes\n1,alice@example.test,Alice Example,1234.5678,true,"line one"\n2,bob@example.test,Bob Example,-12.5000,false,\n3,chen@example.test,陈示例,,true,"quoted, comma"\n',
        'quoted-multiline.csv': 'id,notes\n1,"first line\nsecond line"\n2,"quote ""inside"", comma"\n',
        'invalid-unclosed-quote.csv': 'id,notes\n1,"unterminated\n2,value\n',
        'invalid-row-width.csv': 'id,notes\n1,one,extra\n',
    }
    for name, text in inputs.items():
        (fixtures / name).write_bytes(text.encode('utf-8'))
    (fixtures / 'invalid-utf8.csv').write_bytes(b'id,notes\n1,\xff\xfe\n')
    return pg


def initialize(pg):
    # Explicit fixture admin, separate from etl/etl_control; never used by probe.
    with psycopg.connect(host='127.0.0.1', port=pg['host_port'], dbname=pg['database'],
                         user=pg['admin_username'], password=pg['admin_password']) as db:
        if not db.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (pg['username'],)).fetchone():
            db.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT')
                       .format(sql.Identifier(pg['username']), sql.Literal(pg['password'])))
        db.execute('REVOKE CREATE ON SCHEMA public FROM PUBLIC')
        db.execute('''CREATE TABLE IF NOT EXISTS public.customers (
            id BIGINT PRIMARY KEY, email VARCHAR(200) NOT NULL UNIQUE, full_name VARCHAR(80),
            balance NUMERIC(18,4), active BOOLEAN NOT NULL, created_at TIMESTAMPTZ, notes TEXT)''')
        db.execute("COMMENT ON TABLE public.customers IS 'Synthetic connector acceptance customers'")
        db.execute("COMMENT ON COLUMN public.customers.email IS 'Synthetic email; mask in profiles'")
        db.execute('''INSERT INTO public.customers VALUES
            (1,'alice@example.test','Alice Example',1234.5678,true,'2025-01-01T01:02:03.123456Z','line one'),
            (2,'bob@example.test','Bob Example',-12.5000,false,'2025-01-02T03:04:05.000001Z',NULL),
            (3,'chen@example.test','陈示例',NULL,true,'2025-01-03T00:00:00Z','quoted, comma') ON CONFLICT DO NOTHING''')
        db.execute('''CREATE TABLE IF NOT EXISTS public.orders (id BIGINT PRIMARY KEY,
            customer_id BIGINT NOT NULL REFERENCES public.customers(id), amount NUMERIC(20,6), order_date DATE, payload JSONB)''')
        db.execute('''INSERT INTO public.orders VALUES
            (101,1,99.120000,'2025-01-02','{"source":"synthetic","priority":1}'),
            (102,2,0.000001,'2025-01-03',NULL) ON CONFLICT DO NOTHING''')
        db.execute('CREATE OR REPLACE VIEW public.customer_public AS SELECT id,full_name,active FROM public.customers')
        db.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(sql.Identifier(pg['database'])))
        db.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(sql.Identifier(pg['database']), sql.Identifier(pg['username'])))
        db.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(sql.Identifier(pg['username'])))
        db.execute(sql.SQL('GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}').format(sql.Identifier(pg['username'])))
        db.execute(sql.SQL('ALTER ROLE {} IN DATABASE {} SET default_transaction_read_only=on')
                   .format(sql.Identifier(pg['username']), sql.Identifier(pg['database'])))
        db.execute('ANALYZE public.customers'); db.execute('ANALYZE public.orders')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, default=Path('.runtime/connectors'))
    parser.add_argument('--initialize', action='store_true', help='Initialize the already-running independent PG demo')
    args = parser.parse_args()
    pg = prepare(args.directory)
    if args.initialize:
        try:
            initialize(pg)
        except psycopg.Error:
            raise SystemExit('Independent PostgreSQL initialization failed; check source health and private credentials') from None
    print('Independent PostgreSQL configuration and five CSV fixtures prepared; no credentials printed.')
