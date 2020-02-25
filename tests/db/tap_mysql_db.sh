#!/bin/bash -e
#
# Building a test MySQL database for integration testing of tap-mysql 
# The sample database available at https://github.com/ikostan/RESTAURANT-DATABASE
PWD="$(dirname "$0")"

TEST_DB_SQL=${PWD}/tap_mysql_data.sql
echo "Building test MySQL database..."

# To run this script some environment variables must be set.
# Normally it's defined in .circleci/config.yml
if [[ -z "${DB_TAP_MYSQL_HOST}" || -z "${DB_TAP_MYSQL_PORT}" || -z "${DB_TAP_MYSQL_ROOT_PASSWORD}" || -z "${DB_TAP_MYSQL_USER}" || -z "${DB_TAP_MYSQL_PASSWORD}" || -z "${DB_TAP_MYSQL_DB}" ]]; then
    echo "ERROR: One or more required environment variable is not defined:"
    echo "       - DB_TAP_MYSQL_HOST"
    echo "       - DB_TAP_MYSQL_PORT"
    echo "       - DB_TAP_MYSQL_ROOT_PASSWORD"
    echo "       - DB_TAP_MYSQL_USER"
    echo "       - DB_TAP_MYSQL_PASSWORD"
    echo "       - DB_TAP_MYSQL_DB"
    exit 1
fi

# Grant Replication client and replication slave privileges that
# requires for LOG_BASED CDC replication
export MYSQL_PWD=${DB_TAP_MYSQL_ROOT_PASSWORD}
mysql --protocol TCP --host ${DB_TAP_MYSQL_HOST} --port ${DB_TAP_MYSQL_PORT} --user root -e "GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO ${DB_TAP_MYSQL_USER}"

# Grant insert privileges for testing
mysql --protocol TCP --host ${DB_TAP_MYSQL_HOST} --port ${DB_TAP_MYSQL_PORT} --user root -e "GRANT INSERT ON *.* TO ${DB_TAP_MYSQL_USER}"

# Download the sample database and build it
export MYSQL_PWD=${DB_TAP_MYSQL_PASSWORD}
mysql --protocol TCP --host ${DB_TAP_MYSQL_HOST} --port ${DB_TAP_MYSQL_PORT} --user ${DB_TAP_MYSQL_USER} ${DB_TAP_MYSQL_DB} < ${TEST_DB_SQL}