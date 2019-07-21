#!/bin/bash -e
#
# Building a test PostgreSQL database for integration testing of tap-postgres 
# The database is a sample database availabel at https://github.com/morenoh149/postgresDBSamples
#
# The following envrionment variables must set in circleci config.yml
#   TAP_POSTGRES_HOST: localhost
#   TAP_POSTGRES_PORT: 5432
#   TAP_POSTGRES_USER: pipelinewise
#   TAP_POSTGRES_PASSWORD: secret
#   TAP_POSTGRES_DBNAME: postgres_source_db


# Create a postgres password file for non-interaction connection
PGPASSFILE=~/.pgpass
echo ${TAP_POSTGRES_HOST}:${TAP_POSTGRES_PORT}:${TAP_POSTGRES_DBNAME}:${TAP_POSTGRES_USER}:${TAP_POSTGRES_PASSWORD} > ${PGPASSFILE}
chmod 0600 ${PGPASSFILE}

DB_CONNECTION=`cat ${PGPASSFILE}`
echo "Postgres test DB connection: ${DB_CONNECTION}"

# Download the sample database and build it
wget https://raw.githubusercontent.com/morenoh149/postgresDBSamples/master/chinook-1.4/Chinook_PostgreSql_utf8.sql
psql -U ${TAP_POSTGRES_USER} -h ${TAP_POSTGRES_HOST} -f Chinook_PostgreSql_utf8.sql -d ${TAP_POSTGRES_DBNAME}
