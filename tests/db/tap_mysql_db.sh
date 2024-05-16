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

echo "SETTING UP MYSQL PRIMARY SERVER FOR REPLICATION"

mysql --protocol TCP \
--ssl \
--host ${TAP_MYSQL_HOST} \
--port ${TAP_MYSQL_PORT} \
--user root \
--password=${TAP_MYSQL_ROOT_PASSWORD} \
-e "CREATE DATABASE IF NOT EXISTS ${TAP_MYSQL_REPLICA_DB}; GRANT ALL PRIVILEGES ON ${TAP_MYSQL_REPLICA_DB}.* TO ${TAP_MYSQL_USER}; GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO ${TAP_MYSQL_USER}; FLUSH PRIVILEGES;"

echo "SET UP MYSQL REPLICA SERVER FOR REPLICATION"

mysql --protocol TCP \
--host ${TAP_MYSQL_REPLICA_HOST} \
--port ${TAP_MYSQL_REPLICA_PORT} \
--user root \
--password=${TAP_MYSQL_REPLICA_ROOT_PASSWORD} \
-e "GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO ${TAP_MYSQL_REPLICA_USER}; GRANT SUPER ON *.* TO ${TAP_MYSQL_REPLICA_USER}; FLUSH PRIVILEGES;"

echo "GETTING MYSQL PRIMARY SERVER LOG INFO"

MASTER_LOG_STATUS=`mysql --protocol TCP --ssl --host ${TAP_MYSQL_HOST} --port ${TAP_MYSQL_PORT} --user root --password=${TAP_MYSQL_ROOT_PASSWORD} -e "SHOW MASTER STATUS;"`
CURRENT_LOG=`echo $MASTER_LOG_STATUS | awk '{print $5}'`
CURRENT_POS=`echo $MASTER_LOG_STATUS | awk '{print $6}'`

echo "STARTING MYSQL REPLICATION"

mysql --protocol TCP \
--host=${TAP_MYSQL_REPLICA_HOST} \
--port ${TAP_MYSQL_REPLICA_PORT} \
--user ${TAP_MYSQL_REPLICA_USER} \
--password=${TAP_MYSQL_REPLICA_PASSWORD} \
-e "STOP SLAVE; CHANGE MASTER TO MASTER_SSL=1,MASTER_SSL_VERIFY_SERVER_CERT=0,MASTER_HOST='${TAP_MYSQL_HOST}',MASTER_USER='${TAP_MYSQL_USER}',MASTER_PASSWORD='${TAP_MYSQL_PASSWORD}',MASTER_LOG_FILE='${CURRENT_LOG}',MASTER_LOG_POS=${CURRENT_POS}; START SLAVE;"

# Download the sample database and build it
echo "DUMPING DATA INTO PRIMARY MYSQL DATABASE"

mysql --protocol TCP \
--ssl \
--host ${TAP_MYSQL_HOST} \
--port ${TAP_MYSQL_PORT} \
--user ${TAP_MYSQL_USER} \
--password=${TAP_MYSQL_PASSWORD} \
${TAP_MYSQL_DB} < ${TEST_DB_SQL}

echo "DUMPING DATA INTO PRIMARY MYSQL DATABASE2"

mysql --protocol TCP \
--ssl \
--host ${TAP_MYSQL_HOST} \
--port ${TAP_MYSQL_PORT} \
--user ${TAP_MYSQL_USER} \
--password=${TAP_MYSQL_PASSWORD} \
${TAP_MYSQL_REPLICA_DB} < ${TEST_DB_SQL}
