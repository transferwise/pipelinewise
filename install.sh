#!/usr/bin/bash

# Ubuntu prerequisites
# apt install python3 python3-pip python3-venv libpq-dev libsnappy-dev -y

# Source directory defined as location of install.sh
SRC_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Install pipelinewise venvs in the present working directory
VENV_DIR=$(pwd)/.virtualenvs

python3 -m pip install --upgrade pip

make_virtualenv() {
    echo ""
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

install_rest_api() {
    cd $SRC_DIR/rest-api
    make_virtualenv rest-api
}

install_admin_console() {
    cd $SRC_DIR/admin-console
    npm run setup
}

# Install Singer connectors
install_connector tap-mysql
install_connector tap-postgres
install_connector tap-zendesk
install_connector tap-kafka
install_connector tap-adwords
install_connector tap-s3-csv
install_connector target-postgres
install_connector target-snowflake
install_connector transform-field


# Install fastsyncs
install_fastsync mysql-to-snowflake
install_fastsync postgres-to-snowflake

# Install CLI
install_cli

# Install REST API
#install_rest_api

# Install web frontent
#install_admin_console

echo "--------------------------------------------------------------------------"
echo "PipelineWise installed successfully"
echo "--------------------------------------------------------------------------"
echo
echo "To start REST API:"
echo "> source .virtualenvs/rest-api/bin/activate && cd rest-api && export FLASK_APP=rest_api && export FLASK_DEBUG=1 && flask run"
echo
echo "To start Web Interface:"
echo "> cd admin-console && npm run start"
echo
echo "--------------------------------------------------------------------------"
