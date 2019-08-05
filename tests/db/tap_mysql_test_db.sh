#!/bin/bash -e
#
# Building a test MySQL database for integration testing of tap-mysql 
# The sample database available at https://github.com/ikostan/RESTAURANT-DATABASE

TEST_DB_URL=https://raw.githubusercontent.com/ikostan/RESTAURANT-DATABASE/master/DB_backup/structure_and_data/grp24.sql
TEST_DB_TMP_SQL=/tmp/test-mysql-db.sql
echo "Building test MySQL database from ${TEST_DB_URL}..."

# To run this script some environment variables must be set.
# Normally it's defined in .circleci/config.yml
if [[ -z "${DB_TAP_MYSQL_HOST}" || -z "${DB_TAP_MYSQL_PORT}" || -z "${DB_TAP_MYSQL_USER}" || -z "${DB_TAP_MYSQL_PASSWORD}" || -z "${DB_TAP_MYSQL_DB}" ]]; then
    echo "ERROR: One or more required environment variable is not defined:"
    echo "       - DB_TAP_MYSQL_HOST"
    echo "       - DB_TAP_MYSQL_PORT"
    echo "       - DB_TAP_MYSQL_USER"
    echo "       - DB_TAP_MYSQL_PASSWORD"
    echo "       - DB_TAP_MYSQL_DB"
    exit 1
fi

# Pass environment variables to MySQL compatible ones
export MYSQL_PWD=${DB_TAP_MYSQL_PASSWORD} 

# Download the sample database and build it
wget -O ${TEST_DB_TMP_SQL} ${TEST_DB_URL}
mysql --protocol TCP --host ${DB_TAP_MYSQL_HOST} --port ${DB_TAP_MYSQL_PORT} --user ${DB_TAP_MYSQL_USER} ${DB_TAP_MYSQL_DB} < ${TEST_DB_TMP_SQL}
rm -f ${TEST_DB_TMP_SQL}