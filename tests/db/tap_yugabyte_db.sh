#!/bin/bash -e
#
# Building a test YugabyteDB database for integration testing of tap-yugabyte
PWD="$(dirname "$0")"

echo "Building test YugabyteDB database..."

# To run this script some environment variables must be set.
# Normally it's defined in dev-project/.env
if [[ -z "${TAP_YUGABYTE_HOST}" || -z "${TAP_YUGABYTE_PORT}" || -z "${TAP_YUGABYTE_USER}" || -z "${TAP_YUGABYTE_PASSWORD}" || -z "${TAP_YUGABYTE_DB}" || -z "${TAP_YUGABYTE_SUPERUSER}" || -z "${TAP_YUGABYTE_SUPERUSER_PASSWORD}" ]]; then
    echo "ERROR: One or more required environment variable is not defined:"
    echo "       - TAP_YUGABYTE_HOST"
    echo "       - TAP_YUGABYTE_PORT"
    echo "       - TAP_YUGABYTE_USER"
    echo "       - TAP_YUGABYTE_PASSWORD"
    echo "       - TAP_YUGABYTE_DB"
    echo "       - TAP_YUGABYTE_SUPERUSER"
    echo "       - TAP_YUGABYTE_SUPERUSER_PASSWORD"
    exit 1
fi

# Create a password file for non-interactive connection. yugabyted ships only the
# bootstrap superuser, so the tap role and database have to be created here.
PGPASSFILE=~/.pgpass
{
  echo "${TAP_YUGABYTE_HOST}:${TAP_YUGABYTE_PORT}:*:${TAP_YUGABYTE_SUPERUSER}:${TAP_YUGABYTE_SUPERUSER_PASSWORD}"
  echo "${TAP_YUGABYTE_HOST}:${TAP_YUGABYTE_PORT}:${TAP_YUGABYTE_DB}:${TAP_YUGABYTE_USER}:${TAP_YUGABYTE_PASSWORD}"
} > ${PGPASSFILE}
chmod 0600 ${PGPASSFILE}

PSQL_SUPER="psql -U ${TAP_YUGABYTE_SUPERUSER} -h ${TAP_YUGABYTE_HOST} -p ${TAP_YUGABYTE_PORT} -d ${TAP_YUGABYTE_SUPERUSER}"

# CREATE ROLE and CREATE DATABASE have no IF NOT EXISTS in YSQL, so both are guarded
# to keep this script idempotent across container restarts.
${PSQL_SUPER} -v ON_ERROR_STOP=1 <<SQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${TAP_YUGABYTE_USER}') THEN
        CREATE ROLE ${TAP_YUGABYTE_USER} WITH LOGIN SUPERUSER PASSWORD '${TAP_YUGABYTE_PASSWORD}';
    ELSE
        ALTER ROLE ${TAP_YUGABYTE_USER} WITH LOGIN SUPERUSER PASSWORD '${TAP_YUGABYTE_PASSWORD}';
    END IF;
END
\$\$;
SQL

if ! ${PSQL_SUPER} -tAc "SELECT 1 FROM pg_database WHERE datname = '${TAP_YUGABYTE_DB}'" | grep -q 1; then
    ${PSQL_SUPER} -v ON_ERROR_STOP=1 -c "CREATE DATABASE ${TAP_YUGABYTE_DB} OWNER ${TAP_YUGABYTE_USER}"
fi

# Build the test Database
PSQL_TAP="psql -U ${TAP_YUGABYTE_USER} -h ${TAP_YUGABYTE_HOST} -p ${TAP_YUGABYTE_PORT} -d ${TAP_YUGABYTE_DB}"

TEST_DB_SQL=${PWD}/tap_yugabyte_data.sql
${PSQL_TAP} -v ON_ERROR_STOP=1 -f ${TEST_DB_SQL}

TEST_DB_SQL=${PWD}/tap_yugabyte_data_logical.sql
${PSQL_TAP} -v ON_ERROR_STOP=1 -f ${TEST_DB_SQL}
