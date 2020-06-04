#!/bin/bash -e
#
# Building a test MySQL database for integration testing of tap-mysql 
# The sample database available at https://github.com/ikostan/RESTAURANT-DATABASE
PWD="$(dirname "$0")"

TEST_DB_SQL=${PWD}/tap_mysql_data.sql
echo "Building test MySQL database..."

# To run this script some environment variables must be set.
# Normally it's defined in .circleci/config.yml
if [[ -z "${TAP_MYSQL_HOST}" || -z "${TAP_MYSQL_PORT}" || -z "${TAP_MYSQL_ROOT_PASSWORD}" || -z "${TAP_MYSQL_USER}" || -z "${TAP_MYSQL_PASSWORD}" || -z "${TAP_MYSQL_DB}" ]]; then
    echo "ERROR: One or more required environment variable is not defined:"
    echo "       - TAP_MYSQL_HOST"
    echo "       - TAP_MYSQL_PORT"
    echo "       - TAP_MYSQL_ROOT_PASSWORD"
    echo "       - TAP_MYSQL_USER"
    echo "       - TAP_MYSQL_PASSWORD"
    echo "       - TAP_MYSQL_DB"
    exit 1
fi

# Grant Replication client and replication slave privileges that
# requires for LOG_BASED CDC replication
export MYSQL_PWD=${TAP_MYSQL_ROOT_PASSWORD}
mysql --protocol TCP --host ${TAP_MYSQL_HOST} --port ${TAP_MYSQL_PORT} --user root -e "GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO ${TAP_MYSQL_USER}"

# Grant insert privileges for testing
mysql --protocol TCP --host ${TAP_MYSQL_HOST} --port ${TAP_MYSQL_PORT} --user root -e "GRANT INSERT ON *.* TO ${TAP_MYSQL_USER}"

# Download the sample database and build it
export MYSQL_PWD=${TAP_MYSQL_PASSWORD}
mysql --protocol TCP --host ${TAP_MYSQL_HOST} --port ${TAP_MYSQL_PORT} --user ${TAP_MYSQL_USER} ${TAP_MYSQL_DB} < ${TEST_DB_SQL}