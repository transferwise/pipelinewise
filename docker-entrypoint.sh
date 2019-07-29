#!/usr/bin/env bash

# Install PipelineWise in the container
./install.sh --acceptlicenses --nousage --withtestextras
if [[ $? != 0 ]]; then
    echo
    echo "ERROR: Docker container not started. Failed to install one or more PipelineWise components."
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
echo "   - PostgreSQL server with test database  (From host: localhost:${DB_TAP_POSTGRES_PORT} - From CLI: db_postgres_source:5432)"
echo "   - MySQL server with test database       (From host: localhost:${DB_TAP_MYSQL_PORT} - From CLI: db_mysql_source:3306)"
echo "   - PostgreSQL server with empty database (From host: localhost:${DB_TARGET_POSTGRES_PORT} - From CLI: db_postgres_dwh:5432)"
echo "(For database credentials check .env file)"
echo
echo
echo "To login to the PipelineWise container and start using Pipelinewise CLI:"
echo " $ docker exec -it pipelinewise_dev bash"
echo " $ pipelinewise status"
echo "=========================================================================="

# Continue running the container
tail -f /dev/null