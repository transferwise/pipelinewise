#!/bin/bash -e
#
# Building a test MySQL database for integration testing of tap-mysql 
# The sample database available at https://github.com/ikostan/RESTAURANT-DATABASE


# To run this script some environment variables must be set.
# Normally it's defined in .circleci/config.yml
if [[ -z "${TAP_MYSQL_HOST}" || -z "${TAP_MYSQL_PORT}" || -z "${TAP_MYSQL_USER}" || -z "${TAP_MYSQL_PASSWORD}" || -z "${TAP_MYSQL_DBNAME}" ]]; then
    echo "ERROR: One or more required environment variable is not defined:"
    echo "       - TAP_MYSQL_HOST"
    echo "       - TAP_MYSQL_PORT"
    echo "       - TAP_MYSQL_USER"
    echo "       - TAP_MYSQL_PASSWORD"
    echo "       - TAP_MYSQL_DBNAME"
    exit 1
fi

# Pass environment variables to MySQL compatible ones
MYSQL_HOST=${TAP_MYSQL_HOST}
MYSQL_TCP_PORT=${TAP_MYSQL_PORT}
MYSQL_PWD=${TAP_MYSQL_PASSWORD} 

# Download the sample database and build it
wget https://raw.githubusercontent.com/ikostan/RESTAURANT-DATABASE/master/DB_backup/structure_and_data/grp24.sql
mysql --protocol TCP --user ${TAP_MYSQL_USER} ${TAP_MYSQL_DBNAME} < grp24.sql
