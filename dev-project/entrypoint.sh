#!/usr/bin/env bash

set -e

# Add Mongodb ppa
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 4B7C549A058F8B6B
echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse" | tee /etc/apt/sources.list.d/mongodb.list

# Install OS dependencies
apt-get update
apt-get install -y --no-install-recommends \
  alien \
  gettext-base \
  libaio1 \
  mariadb-client \
  mbuffer \
  mongo-tools \
  mongodb-org-shell=4.2.7 \
  postgresql-client

rm -rf /var/lib/apt/lists/* \

# Install Oracle Instant Client required for tap-oracle
# ORA_INSTACLIENT_URL=https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm
# wget -O oracle-instantclient.rpm ${ORA_INSTACLIENT_URL}
# echo "Installing Oracle Instant Client for tap-oracle..."
# alien -i oracle-instantclient.rpm --scripts
# rm -f oracle-instantclient.rpm


# Change to dev-project folder
cd dev-project

# Install PipelineWise in the container

# Build test databasese
../tests/db/tap_mysql_db.sh
../tests/db/tap_postgres_db.sh

./mongo/init_rs.sh
../tests/db/tap_mongodb.sh
../tests/db/target_postgres.sh

# Install PipelineWise and connectors in the container
../install.sh --acceptlicenses --nousage --connectors=target-snowflake,target-postgres,target-bigquery,tap-mysql,tap-postgres,tap-mongodb,transform-field,tap-s3-csv

# further populate mongodb test DB
$PIPELINEWISE_HOME/.virtualenvs/pipelinewise/bin/python3 mongo/populate_test_db.py

# Install PipelineWise in the container
../install.sh --acceptlicenses --nousage --connectors=target-snowflake,target-postgres,target-bigquery,tap-mysql,tap-postgres,tap-mongodb,transform-field,tap-s3-csv
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
