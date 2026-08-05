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
apt_retry apt-get install -y software-properties-common apt-utils

echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

apt_retry apt-get install -y --no-install-recommends \
  wget \
  gnupg \
  git \
  alien \
  gettext-base \
  libaio1t64 \
  mariadb-client \
  mbuffer \
  postgresql-client \
  python3.12-dev python3.12-venv

apt_retry apt-get upgrade -y

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
tests/db/tap_postgres_db.sh
tests/db/tap_mongodb.sh
tests/db/target_postgres.sh

# Install PipelineWise and connectors in the container
if ! make pipelinewise connectors -e pw_acceptlicenses=y -e pw_connector=target-snowflake,target-postgres,tap-mysql,tap-postgres,tap-mongodb,transform-field,tap-s3-csv; then
    echo
    echo "ERROR: Docker container not started. Failed to install one or more PipelineWise components."
    exit 1
fi

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
echo "   - MariaDB server with test database     (From host: localhost:${TAP_MYSQL_PORT_ON_HOST} - From CLI: ${TAP_MYSQL_HOST}:${TAP_MYSQL_PORT})"
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
