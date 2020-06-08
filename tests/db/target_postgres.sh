#!/bin/bash -e
#
# Building a test PostgreSQL target database for integration testing of tap-postgres
PWD="$(dirname "$0")"

echo "Building test PostgreSQL target database..."

# To run this script some environment variables must be set.
# Normally it's defined in .circleci/config.yml
if [[ -z "${TARGET_POSTGRES_HOST}" || -z "${TARGET_POSTGRES_PORT}" || -z "${TARGET_POSTGRES_USER}" || -z "${TARGET_POSTGRES_PASSWORD}" || -z "${TARGET_POSTGRES_DB}" ]]; then
    echo "ERROR: One or more required environment variable is not defined:"
    echo "       - TARGET_POSTGRES_HOST"
    echo "       - TARGET_POSTGRES_PORT"
    echo "       - TARGET_POSTGRES_USER"
    echo "       - TARGET_POSTGRES_PASSWORD"
    echo "       - TARGET_POSTGRES_DB"
    exit 1
fi

# Create a postgres password file for non-interaction connection
PGPASSFILE=~/.pgpass
echo ${TARGET_POSTGRES_HOST}:${TARGET_POSTGRES_PORT}:${TARGET_POSTGRES_DB}:${TARGET_POSTGRES_USER}:${TARGET_POSTGRES_PASSWORD} > ${PGPASSFILE}
chmod 0600 ${PGPASSFILE}

# Build the test Databases
TEST_DB_SQL=${PWD}/target_postgres_data.sql
psql -U ${TARGET_POSTGRES_USER} -h ${TARGET_POSTGRES_HOST} -f ${TEST_DB_SQL} -d ${TARGET_POSTGRES_DB}
