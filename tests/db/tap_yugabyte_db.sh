#!/bin/bash -e
#
# Building a test YugabyteDB database for integration testing of tap-yugabyte
PWD="$(dirname "$0")"

echo "Building test YugabyteDB database..."

# To run this script some environment variables must be set.
# Normally it's defined in .circleci/config.yml
if [[ -z "${TAP_YUGABYTE_HOST}" || -z "${TAP_YUGABYTE_PORT}" || -z "${TAP_YUGABYTE_USER}" || -z "${TAP_YUGABYTE_PASSWORD}" || -z "${TAP_YUGABYTE_DB}" ]]; then
    echo "ERROR: One or more required environment variable is not defined:"
    echo "       - TAP_YUGABYTE_HOST"
    echo "       - TAP_YUGABYTE_PORT"
    echo "       - TAP_YUGABYTE_USER"
    echo "       - TAP_YUGABYTE_PASSWORD"
    echo "       - TAP_YUGABYTE_DB"
    exit 1
fi

# Create a postgres password file for non-interaction connection
PGPASSFILE=~/.pgpass
echo ${TAP_YUGABYTE_HOST}:${TAP_YUGABYTE_PORT}:${TAP_POSTGRES_DB}:${TAP_YUGABYTE_USER}:${TAP_YUGABYTE_PASSWORD} > ${PGPASSFILE}
chmod 0600 ${PGPASSFILE}

# Build the test Databases
TEST_DB_SQL=${PWD}/tap_postgres_data.sql
psql -U ${TAP_YUGABYTE_USER} -h ${TAP_YUGABYTE_HOST} -f ${TEST_DB_SQL} -d ${TAP_POSTGRES_DB}

TEST_DB_SQL=${PWD}/tap_postgres_data_logical.sql
psql -U ${TAP_YUGABYTE_USER} -h ${TAP_YUGABYTE_HOST} -f ${TEST_DB_SQL} -d ${TAP_POSTGRES_DB}