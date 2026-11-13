#!/bin/sh
set -eu
mkdir -p "$PGDATA" /run/postgresql
chown postgres:postgres "$PGDATA" /run/postgresql
chmod 700 "$PGDATA"
if [ ! -f "$PGDATA/PG_VERSION" ]; then
    printf '%s\n' "$POSTGRES_PASSWORD" > /tmp/pg-password
    chown postgres:postgres /tmp/pg-password
    chmod 600 /tmp/pg-password
    su postgres -s /bin/sh -c 'initdb -D "$PGDATA" -U "$POSTGRES_USER" --pwfile=/tmp/pg-password --auth-host=scram-sha-256 --auth-local=trust' > /dev/null
    rm /tmp/pg-password
    printf '%s\n' 'host all all all scram-sha-256' >> "$PGDATA/pg_hba.conf"
    su postgres -s /bin/sh -c 'pg_ctl -D "$PGDATA" -o "-c listen_addresses=localhost" -w start' > /dev/null
    su postgres -s /bin/sh -c 'createdb -U "$POSTGRES_USER" "$POSTGRES_DB"'
    su postgres -s /bin/sh -c 'pg_ctl -D "$PGDATA" -m fast -w stop' > /dev/null
fi
exec su postgres -s /bin/sh -c 'exec postgres -D "$PGDATA" -c listen_addresses=*'
