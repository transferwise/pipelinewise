#!/usr/bin/env bash

set -e

apt-get update
apt-get install -y --no-install-recommends \
  wget \
  gnupg

# Add Mongodb ppa
wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add -
echo "deb https://repo.mongodb.org/apt/debian buster/mongodb-org/4.4 main" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list

apt-get update
apt-get install -y --no-install-recommends \
  make \
  build-essential \
  gettext-base \
  libaio1 \
  mariadb-client \
  mbuffer \
  mongodb-database-tools \
  mongodb-org-shell \
  postgresql-client

rm -rf /var/lib/apt/lists/* \

pwd

./dev-project/mongo/init_rs.sh

# Install PipelineWise and connectors in the container
make pipelinewise connectors -e pw_acceptlicenses=y -e pw_connector=transform-field,tap-mysql,tap-postgres,tap-mongodb

if [ "$TARGET" == "snowflake" ]
  then
  make connectors -e pw_acceptlicenses=y -e pw_connector=target-snowflake
else
  make connectors -e pw_acceptlicenses=y -e pw_connector=target-postgres
fi

# Activate CLI virtual environment at every login
sed -i '/motd/d' ~/.bashrc  # Delete any existing old DO_AT_LOGIN line from bashrc
DO_AT_LOGIN="cd $PIPELINEWISE_HOME && source $PIPELINEWISE_HOME/.virtualenvs/pipelinewise/bin/activate && CURRENT_YEAR=\$(date +'%Y') envsubst < $PIPELINEWISE_HOME/../motd"
echo $DO_AT_LOGIN >> ~/.bashrc

echo "PipelineWise test environment is ready."

# Continue running the container
tail -f /dev/null
