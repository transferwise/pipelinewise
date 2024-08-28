#!/usr/bin/env bash

set -e

apt update

DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata

apt install -y --no-install-recommends \
  wget \
  gnupg \
  git \
  alien \
  gettext-base \
  libaio1 \
  mariadb-client \
  mbuffer \
  postgresql-client \
  python3.8 python3-pip python3-venv python3-dev

apt upgrade -y
# rm -rf /var/lib/apt/lists/* \

# Do a bunch of Mongo things
wget -q --no-check-certificate https://downloads.mongodb.com/compass/mongodb-mongosh_2.2.9_amd64.deb
apt install ./mongodb-mongosh_2.2.9_amd64.deb
rm -f mongodb-mongosh_2.2.9_amd64.deb
wget -q --no-check-certificate https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2004-x86_64-100.9.5.deb
apt install ./mongodb-database-tools-ubuntu2004-x86_64-100.9.5.deb
rm -f mongodb-database-tools-ubuntu2004-x86_64-100.9.5.deb

dev-project/mongo/initiate-replica-set.sh

# Build test databases
tests/db/tap_mysql_db.sh
tests/db/tap_postgres_db.sh
tests/db/tap_mongodb.sh
tests/db/target_postgres.sh

# Install PipelineWise and connectors in the container
make pipelinewise connectors -e pw_acceptlicenses=y -e pw_connector=target-snowflake,target-postgres,tap-mysql,tap-postgres,tap-mongodb,transform-field,tap-s3-csv
if [[ $? != 0 ]]; then
    echo
    echo "ERROR: Docker container not started. Failed to install one or more PipelineWise components."
    ls -lah
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
echo "(For database credentials check .env file)"
echo
echo
echo "To login to the PipelineWise container and start using Pipelinewise CLI:"
echo " $ docker exec -it pipelinewise_dev bash"
echo " $ pipelinewise status"
echo "=========================================================================="

# Continue running the container
tail -f /dev/null
