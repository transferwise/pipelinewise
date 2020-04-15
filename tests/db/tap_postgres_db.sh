#!/bin/bash -e
#
# Building a test PostgreSQL database for integration testing of tap-postgres
PWD="$(dirname "$0")"

echo "Building test PostgreSQL database..."

# To run this script some environment variables must be set.
# Normally it's defined in .circleci/config.yml
if [[ -z "${TAP_POSTGRES_HOST}" || -z "${TAP_POSTGRES_PORT}" || -z "${TAP_POSTGRES_USER}" || -z "${TAP_POSTGRES_PASSWORD}" || -z "${TAP_POSTGRES_DB}" ]]; then
    echo "ERROR: One or more required environment variable is not defined:"
    echo "       - TAP_POSTGRES_HOST"
    echo "       - TAP_POSTGRES_PORT"
    echo "       - TAP_POSTGRES_USER"
    echo "       - TAP_POSTGRES_PASSWORD"
    echo "       - TAP_POSTGRES_DB"
    exit 1
fi

# Create a postgres password file for non-interaction connection
PGPASSFILE=~/.pgpass
echo ${TAP_POSTGRES_HOST}:${TAP_POSTGRES_PORT}:${TAP_POSTGRES_DB}:${TAP_POSTGRES_USER}:${TAP_POSTGRES_PASSWORD} > ${PGPASSFILE}
chmod 0600 ${PGPASSFILE}

# Build the test Databases
TEST_DB_SQL=${PWD}/tap_postgres_data.sql
psql -U ${TAP_POSTGRES_USER} -h ${TAP_POSTGRES_HOST} -f ${TEST_DB_SQL} -d ${TAP_POSTGRES_DB}

TEST_DB_SQL=${PWD}/tap_postgres_data_logical.sql
psql -U ${TAP_POSTGRES_USER} -h ${TAP_POSTGRES_HOST} -f ${TEST_DB_SQL} -d ${TAP_POSTGRES_DB}