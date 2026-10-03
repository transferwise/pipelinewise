#!/usr/bin/env bash
set -euo pipefail

mkdir -p "${PGDATA}"
chmod 700 "${PGDATA}"
chown postgres:postgres "${PGDATA}"

if [ ! -s "${PGDATA}/PG_VERSION" ]; then
    gosu postgres pg_basebackup --host=db_primary --username=repl_user \
        --pgdata="${PGDATA}" --wal-method=stream --write-recovery-conf
fi

exec docker-entrypoint.sh postgres -c config_file=/etc/postgresql/postgresql.conf
