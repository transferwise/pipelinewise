#!/usr/bin/env bash
set -euo pipefail

psql --username postgres --dbname postgres --set ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE repl_user WITH LOGIN SUPERUSER REPLICATION PASSWORD 'repl_password';
CREATE ROLE test_user WITH LOGIN SUPERUSER PASSWORD 'my-secret-passwd';
SQL

printf '\nhost replication repl_user all scram-sha-256\n' >> "${PGDATA}/pg_hba.conf"
touch "${PGDATA}/primary_ready"
