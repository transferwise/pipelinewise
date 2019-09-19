#!/bin/bash -e
#
# Building a test PostgreSQL database for integration testing of tap-postgres 


TEST_DB_SQL=./tests/db/tap_postgres_data.sql
echo "Building test PostgreSQL database from ${TEST_DB_URL}..."

# To run this script some environment variables must be set.
# Normally it's defined in .circleci/config.yml
if [[ -z "${DB_TAP_POSTGRES_HOST}" || -z "${DB_TAP_POSTGRES_PORT}" || -z "${DB_TAP_POSTGRES_USER}" || -z "${DB_TAP_POSTGRES_PASSWORD}" || -z "${DB_TAP_POSTGRES_DB}" ]]; then
    echo "ERROR: One or more required environment variable is not defined:"
    echo "       - DB_TAP_POSTGRES_HOST"
    echo "       - DB_TAP_POSTGRES_PORT"
    echo "       - DB_TAP_POSTGRES_USER"
    echo "       - DB_TAP_POSTGRES_PASSWORD"
    echo "       - DB_TAP_POSTGRES_DB"
    exit 1
fi

# Create a postgres password file for non-interaction connection
PGPASSFILE=~/.pgpass
echo ${DB_TAP_POSTGRES_HOST}:${DB_TAP_POSTGRES_PORT}:${DB_TAP_POSTGRES_DB}:${DB_TAP_POSTGRES_USER}:${DB_TAP_POSTGRES_PASSWORD} > ${PGPASSFILE}
chmod 0600 ${PGPASSFILE}

# Drop target tables if exists, the test db sql works only if tables not exist
psql -U ${DB_TAP_POSTGRES_USER} -h ${DB_TAP_POSTGRES_HOST} -c 'DROP TABLE IF EXISTS public2.edgyData CASCADE;' ${DB_TAP_POSTGRES_DB}
psql -U ${DB_TAP_POSTGRES_USER} -h ${DB_TAP_POSTGRES_HOST} -c 'DROP TABLE IF EXISTS public.edgyData CASCADE;' ${DB_TAP_POSTGRES_DB}
psql -U ${DB_TAP_POSTGRES_USER} -h ${DB_TAP_POSTGRES_HOST} -c 'DROP TABLE IF EXISTS public.countrylanguage CASCADE;' ${DB_TAP_POSTGRES_DB}
psql -U ${DB_TAP_POSTGRES_USER} -h ${DB_TAP_POSTGRES_HOST} -c 'DROP TABLE IF EXISTS public.country CASCADE;' ${DB_TAP_POSTGRES_DB}
psql -U ${DB_TAP_POSTGRES_USER} -h ${DB_TAP_POSTGRES_HOST} -c 'DROP TABLE IF EXISTS public.city CASCADE;' ${DB_TAP_POSTGRES_DB}

# Build the test DB
psql -U ${DB_TAP_POSTGRES_USER} -h ${DB_TAP_POSTGRES_HOST} -f ${TEST_DB_SQL} -d ${DB_TAP_POSTGRES_DB}