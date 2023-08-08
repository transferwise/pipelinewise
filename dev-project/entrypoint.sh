#!/usr/bin/env bash

set -e

apt-get update
apt-get install -y --no-install-recommends \
  wget \
  gnupg \
  git

# Add Mongodb ppa
wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add -
echo "deb https://repo.mongodb.org/apt/debian buster/mongodb-org/4.4 main" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list

apt-get update
apt-get install -y --no-install-recommends \
  alien \
  gettext-base \
  libaio1 \
  mariadb-client \
  mbuffer \
  mongodb-database-tools \
  mongodb-org-shell \
  postgresql-client

rm -rf /var/lib/apt/lists/* \

# Install Oracle Instant Client required for tap-oracle
# ORA_INSTACLIENT_URL=https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm
# wget -O oracle-instantclient.rpm ${ORA_INSTACLIENT_URL}
# echo "Installing Oracle Instant Client for tap-oracle..."
# alien -i oracle-instantclient.rpm --scripts
# rm -f oracle-instantclient.rpm

# Build test databases

tests/db/tap_mysql_db.sh
tests/db/tap_postgres_db.sh

dev-project/mongo/init_rs.sh
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
