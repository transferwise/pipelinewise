#!/usr/bin/env bash

# Install OS dependencies
apt-get update
apt-get install -y mariadb-client postgresql-client alien libaio1

# Install Oracle Instant Client required for tap-oracle
cd 
ORA_INSTACLIENT_URL=https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm
wget -O oracle-instantclient.rpm ${ORA_INSTACLIENT_URL}
echo "Installing Oracle Instant Client for tap-oracle..."
alien -i oracle-instantclient.rpm --scripts
rm -f oracle-instantclient.rpm
cd -

# Build test databasese
tests/db/tap_mysql_db.sh
tests/db/tap_postgres_db.sh

# Install PipelineWise in the container
./install.sh --acceptlicenses --nousage
if [[ $? != 0 ]]; then
    echo
    echo "ERROR: Docker container not started. Failed to install one or more PipelineWise components."
    ls -lah
    exit 1
fi

# Activate CLI virtual environment at every login
DO_AT_LOGIN="source $PIPELINEWISE_HOME/.virtualenvs/pipelinewise/bin/activate && cat $PIPELINEWISE_HOME/motd"
if [[ `tail -n1 ~/.bashrc` != "$DO_AT_LOGIN" ]]; then
    echo $DO_AT_LOGIN >> ~/.bashrc
fi

echo
echo "=========================================================================="
echo "PipelineWise Dev environment is ready in Docker container(s)."
echo
echo "Running containers:"
echo "   - PipelineWise CLI and connectors"
echo "   - PostgreSQL server with test database  (From host: localhost:${DB_TAP_POSTGRES_PORT_ON_HOST} - From CLI: ${DB_TAP_POSTGRES_HOST}:${DB_TAP_POSTGRES_PORT})"
echo "   - MariaDB server with test database     (From host: localhost:${DB_TAP_MYSQL_PORT_ON_HOST} - From CLI: ${DB_TAP_MYSQL_HOST}:${DB_TAP_MYSQL_PORT})"
echo "   - PostgreSQL server with empty database (From host: localhost:${DB_TARGET_POSTGRES_PORT_ON_HOST} - From CLI: ${DB_TARGET_POSTGRES_HOST}:${DB_TARGET_POSTGRES_PORT})"
echo "(For database credentials check .env file)"
echo
echo
echo "To login to the PipelineWise container and start using Pipelinewise CLI:"
echo " $ docker exec -it pipelinewise_dev bash"
echo " $ pipelinewise status"
echo "=========================================================================="

# Continue running the container
tail -f /dev/null