#!/usr/bin/env bash

set -e

MONGOSH_VERSION=2.2.9
MONGODB_TOOLS_VERSION=100.9.5

# The repo root is bind-mounted here, so downloads go to /tmp to keep the
# developer's working tree clean.
DOWNLOAD_DIR=/tmp

# Retry wrapper for transient network and mirror errors. Pass refresh=1 to run
# apt-get update between attempts.
retry() {
  local refresh=$1
  shift
  local max_attempts=3
  local attempt=1
  while true; do
    if "$@"; then
      return 0
    fi
    if [ $attempt -ge $max_attempts ]; then
      echo "command failed after $max_attempts attempts: $*"
      return 1
    fi
    echo "command failed (attempt $attempt/$max_attempts), retrying in 5s..."
    attempt=$((attempt + 1))
    sleep 5
    if [ "$refresh" = 1 ]; then
      apt-get update
    fi
  done
}

apt_retry() {
  retry 1 "$@"
}

net_retry() {
  retry 0 "$@"
}

apt-get update
echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

apt_retry apt-get install -y --no-install-recommends \
  wget \
  git \
  gettext-base \
  make \
  mariadb-client \
  mbuffer \
  postgresql-client \
  python3 python3.12-venv

# Do a bunch of Mongo things
MONGOSH_DEB=mongodb-mongosh_${MONGOSH_VERSION}_amd64.deb
MONGODB_TOOLS_DEB=mongodb-database-tools-ubuntu2004-x86_64-${MONGODB_TOOLS_VERSION}.deb

net_retry wget -q -O "${DOWNLOAD_DIR}/${MONGOSH_DEB}" \
  "https://downloads.mongodb.com/compass/${MONGOSH_DEB}"
apt_retry apt-get install -y "${DOWNLOAD_DIR}/${MONGOSH_DEB}"
rm -f "${DOWNLOAD_DIR}/${MONGOSH_DEB}"

net_retry wget -q -O "${DOWNLOAD_DIR}/${MONGODB_TOOLS_DEB}" \
  "https://fastdl.mongodb.org/tools/db/${MONGODB_TOOLS_DEB}"
apt_retry apt-get install -y "${DOWNLOAD_DIR}/${MONGODB_TOOLS_DEB}"
rm -f "${DOWNLOAD_DIR}/${MONGODB_TOOLS_DEB}"

dev-project/mongo/initiate-replica-set.sh

# Build test databases
tests/db/tap_mysql_db.sh
tests/db/tap_oracle_mysql_db.sh
tests/db/tap_postgres_db.sh
tests/db/tap_yugabyte_db.sh
tests/db/tap_mongodb.sh
tests/db/target_postgres.sh

# Install PipelineWise before the connectors so their concurrent pip installs
# can reuse its warmed download cache. Each connector has its own virtualenv.
if ! make pipelinewise -e pw_acceptlicenses=y; then
    echo
    echo "ERROR: Docker container not started. Failed to install PipelineWise."
    exit 1
fi

CONNECTORS=(
  target-snowflake
  target-postgres
  tap-mysql
  tap-postgres
  tap-yugabyte
  tap-mongodb
  transform-field
  tap-s3-csv
)
CONNECTOR_INSTALL_PARALLELISM=4
CONNECTOR_INSTALL_LOG_DIR=${DOWNLOAD_DIR}/pipelinewise-connector-install
CONNECTOR_INSTALL_PIDS=()

stop_connector_installs() {
  local pid
  for pid in "${CONNECTOR_INSTALL_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${CONNECTOR_INSTALL_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
}

install_connector_batch() {
  local connector
  local failed=0
  local index
  local log_file
  local -a batch_connectors=("$@")
  local -a batch_logs=()

  CONNECTOR_INSTALL_PIDS=()
  for connector in "${batch_connectors[@]}"; do
    log_file=${CONNECTOR_INSTALL_LOG_DIR}/${connector}.log
    batch_logs+=("$log_file")
    echo "Starting ${connector} connector installation..."
    make connectors \
      -e pw_acceptlicenses=y \
      -e pw_connector="$connector" >"$log_file" 2>&1 &
    CONNECTOR_INSTALL_PIDS+=("$!")
  done

  for index in "${!CONNECTOR_INSTALL_PIDS[@]}"; do
    connector=${batch_connectors[$index]}
    if wait "${CONNECTOR_INSTALL_PIDS[$index]}"; then
      echo "Finished ${connector} connector installation."
    else
      echo "ERROR: Failed to install ${connector} connector."
      failed=1
    fi
  done

  for index in "${!batch_logs[@]}"; do
    connector=${batch_connectors[$index]}
    echo
    echo "----- ${connector} connector install log -----"
    sed "s/^/[${connector}] /" "${batch_logs[$index]}"
  done

  CONNECTOR_INSTALL_PIDS=()
  return "$failed"
}

mkdir -p "$CONNECTOR_INSTALL_LOG_DIR"
trap 'stop_connector_installs; exit 130' INT
trap 'stop_connector_installs; exit 143' TERM

connector_count=${#CONNECTORS[@]}
for ((batch_start = 0; batch_start < connector_count; batch_start += CONNECTOR_INSTALL_PARALLELISM)); do
  if ! install_connector_batch \
    "${CONNECTORS[@]:batch_start:CONNECTOR_INSTALL_PARALLELISM}"; then
    echo
    echo "ERROR: Docker container not started. Failed to install one or more PipelineWise connectors."
    exit 1
  fi
done

trap - INT TERM

# Activate CLI virtual environment at every login
sed -i '/motd/d' ~/.bashrc  # Delete any existing old DO_AT_LOGIN line from bashrc
DO_AT_LOGIN="cd $PIPELINEWISE_HOME && source $PIPELINEWISE_HOME/.virtualenvs/pipelinewise/bin/activate && CURRENT_YEAR=\$(date +'%Y') envsubst < $PIPELINEWISE_HOME/../motd"
echo $DO_AT_LOGIN >> ~/.bashrc

echo
echo "=========================================================================="
echo "PipelineWise Dev environment is ready in Docker container(s)."
echo
echo "Running containers:"
echo "   - PipelineWise CLI and connectors"
echo "   - PostgreSQL server with test database  (From host: localhost:${TAP_POSTGRES_PORT_ON_HOST} - From CLI: ${TAP_POSTGRES_HOST}:${TAP_POSTGRES_PORT})"
echo "   - YugabyteDB server with test database  (From host: localhost:${TAP_YUGABYTE_PORT_ON_HOST} - From CLI: ${TAP_YUGABYTE_HOST}:${TAP_YUGABYTE_PORT})"
echo "   - MariaDB server with test database     (From host: localhost:${TAP_MYSQL_PORT_ON_HOST} - From CLI: ${TAP_MYSQL_HOST}:${TAP_MYSQL_PORT})"
echo "   - MySQL server with test database       (From host: localhost:${TAP_ORACLE_MYSQL_PORT_ON_HOST} - From CLI: ${TAP_ORACLE_MYSQL_HOST}:${TAP_ORACLE_MYSQL_PORT})"
echo "   - MongoDB replicaSet server with test database (From host: localhost:${TAP_MONGODB_PORT_ON_HOST} - From CLI: ${TAP_MONGODB_HOST}:${TAP_MONGODB_PORT})"
echo "   - PostgreSQL server with empty database (From host: localhost:${TARGET_POSTGRES_PORT_ON_HOST} - From CLI: ${TARGET_POSTGRES_HOST}:${TARGET_POSTGRES_PORT})"
echo "   - PipelineWise backend PostgreSQL database (From host: localhost:${PIPELINEWISE_BACKEND_PORT_ON_HOST} - From CLI: ${PIPELINEWISE_BACKEND_HOST}:${PIPELINEWISE_BACKEND_PORT})"
echo "(For database credentials check .env file)"
echo
echo
echo "To login to the PipelineWise container and start using Pipelinewise CLI:"
echo " $ docker exec -it pipelinewise bash"
echo " $ pipelinewise status"
echo "=========================================================================="

# Continue running the container
tail -f /dev/null
