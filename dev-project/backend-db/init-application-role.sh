#!/usr/bin/env bash
#
# Create the data-diff application role, with no rights beyond CONNECT. The
# container superuser is the DDL role that owns the schema; migrations grant this
# role access to what they create. Keeping the two apart means dev and e2e exercise
# the production privilege split rather than assuming it.
#
# Postgres runs docker-entrypoint-initdb.d only on an empty data directory, so an
# existing backend volume must be removed for changes here to take effect.
#
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE "${PIPELINEWISE_BACKEND_APP_USER}"
        LOGIN PASSWORD '${PIPELINEWISE_BACKEND_APP_PASSWORD}';
    GRANT CONNECT ON DATABASE "${POSTGRES_DB}" TO "${PIPELINEWISE_BACKEND_APP_USER}";
EOSQL
