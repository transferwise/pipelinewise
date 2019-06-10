#!/bin/bash

# Exit script on first error
set -e

# Capture start_time
start_time=`date +%s`

# Ubuntu prerequisites
# apt install python3 python3-pip python3-venv libpq-dev libsnappy-dev -y

# Source directory defined as location of install.sh
SRC_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Install pipelinewise venvs in the present working directory
PIPELINEWISE_HOME=$(pwd)
VENV_DIR=${PIPELINEWISE_HOME}/.virtualenvs

make_virtualenv() {
    echo
    echo "--------------------------------------------------------------------------"
    echo "Making Virtual Environment for $1"
    echo "--------------------------------------------------------------------------"
    python3 -m venv $VENV_DIR/$1
    source $VENV_DIR/$1/bin/activate
    python3 -m pip install --upgrade pip
    python3 -m pip install -r requirements.txt
    if [ -f "setup.py" ]; then
        python3 -m pip install .
    fi
    deactivate
}

install_connector() {
    cd $SRC_DIR/singer-connectors/$1
    make_virtualenv $1
}

install_fastsync() {
    cd $SRC_DIR/fastsync/$1
    make_virtualenv $1
}

install_cli() {
    cd $SRC_DIR/cli
    make_virtualenv cli
}


# Install Singer connectors
install_connector tap-mysql
install_connector tap-postgres
install_connector tap-zendesk
install_connector tap-kafka
install_connector tap-adwords
install_connector tap-s3-csv
install_connector tap-snowflake
install_connector target-postgres
install_connector target-snowflake
install_connector target-s3-csv
install_connector transform-field


# Install fastsyncs
install_fastsync mysql-to-snowflake
install_fastsync postgres-to-snowflake

# Install CLI
install_cli

# Capture end_time
end_time=`date +%s`

echo
echo "--------------------------------------------------------------------------"
echo "PipelineWise installed successfully in $((end_time-start_time)) seconds"
echo "--------------------------------------------------------------------------"
echo
echo "To start CLI:"
echo " $ source $VENV_DIR/cli/bin/activate"
echo " $ export PIPELINEWISE_HOME=$PIPELINEWISE_HOME"

echo " $ pipelinewise status"
echo
echo "--------------------------------------------------------------------------"
