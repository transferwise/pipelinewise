#!/bin/bash -e

SCRIPT_DIR="$(dirname "$0")"
TEST_DB_SQL=${SCRIPT_DIR}/tap_oracle_mysql_data.sql

required_vars=(
  TAP_ORACLE_MYSQL_HOST
  TAP_ORACLE_MYSQL_PORT
  TAP_ORACLE_MYSQL_ROOT_PASSWORD
  TAP_ORACLE_MYSQL_USER
  TAP_ORACLE_MYSQL_PASSWORD
  TAP_ORACLE_MYSQL_DB
)
for required_var in "${required_vars[@]}"; do
  if [[ -z "${!required_var}" ]]; then
    echo "ERROR: ${required_var} is required"
    exit 1
  fi
done

echo "Building genuine MySQL test database..."

mysql --protocol TCP --ssl \
  --host "${TAP_ORACLE_MYSQL_HOST}" \
  --port "${TAP_ORACLE_MYSQL_PORT}" \
  --user root \
  --password="${TAP_ORACLE_MYSQL_ROOT_PASSWORD}" \
  -e "GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '${TAP_ORACLE_MYSQL_USER}'@'%'; FLUSH PRIVILEGES;"

mysql --protocol TCP --ssl \
  --host "${TAP_ORACLE_MYSQL_HOST}" \
  --port "${TAP_ORACLE_MYSQL_PORT}" \
  --user "${TAP_ORACLE_MYSQL_USER}" \
  --password="${TAP_ORACLE_MYSQL_PASSWORD}" \
  "${TAP_ORACLE_MYSQL_DB}" < "${TEST_DB_SQL}"
