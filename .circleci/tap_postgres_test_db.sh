#!/bin/bash -e
#
# Building a test PostgreSQL database for integration testing of tap-postgres 
# The sample database available at https://github.com/morenoh149/postgresDBSamples


# To run this script some environment variables must be set.
# Normally it's defined in .circleci/config.yml
if [[ -z "${TAP_POSTGRES_HOST}" || -z "${TAP_POSTGRES_PORT}" || -z "${TAP_POSTGRES_USER}" || -z "${TAP_POSTGRES_PASSWORD}" || -z "${TAP_POSTGRES_DBNAME}" ]]; then
    echo "ERROR: One or more required environment variable is not defined:"
    echo "       - TAP_POSTGRES_HOST"
    echo "       - TAP_POSTGRES_PORT"
    echo "       - TAP_POSTGRES_USER"
    echo "       - TAP_POSTGRES_PASSWORD"
    echo "       - TAP_POSTGRES_DBNAME"
    exit 1
fi

# Create a postgres password file for non-interaction connection
PGPASSFILE=~/.pgpass
echo ${TAP_POSTGRES_HOST}:${TAP_POSTGRES_PORT}:${TAP_POSTGRES_DBNAME}:${TAP_POSTGRES_USER}:${TAP_POSTGRES_PASSWORD} > ${PGPASSFILE}
chmod 0600 ${PGPASSFILE}

# Download the sample database and build it
wget https://raw.githubusercontent.com/morenoh149/postgresDBSamples/master/chinook-1.4/Chinook_PostgreSql_utf8.sql
psql -U ${TAP_POSTGRES_USER} -h ${TAP_POSTGRES_HOST} -f Chinook_PostgreSql_utf8.sql -d ${TAP_POSTGRES_DBNAME}
