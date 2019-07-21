#!/bin/bash -e
#
# Building a test PostgreSQL database for integration testing of tap-postgres 
# The database is a sample database availabel at https://github.com/morenoh149/postgresDBSamples


# Create a postgres password file for non-interaction connection
# Envrionment variables are set in circleci config.yml
PGPASS=~/.pgpass
echo ${TAP_POSTGRES_HOST}:${TAP_POSTGRES_PORT}:${TAP_POSTGRES_DBNAME}:${TAP_POSTGRES_USER}:${TAP_POSTGRES_PASSWORD} > ${PGPASS}
chmod 0600 ${PGPASS}

# Download the sample database and build it
wget https://raw.githubusercontent.com/morenoh149/postgresDBSamples/master/chinook-1.4/Chinook_PostgreSql_utf8.sql
psql -f Chinook_PostgreSql_utf8.sql -d postgres_source_db
