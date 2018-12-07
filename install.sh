
DIR=$(pwd)
VENV_DIR=$DIR/.virtualenvs

create_virtualenv() {
    python3 -m venv $VENV_DIR/$1
}

make_virtualenv() {
    create_virtualenv $1
    source $VENV_DIR/$1/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    if [ -f "setup.py" ]; then
        pip install .
    fi
    deactivate
}

install_connector() {
    cd $DIR/singer-connectors/$1
    make_virtualenv $1
}

install_fastsync() {
    cd $DIR/fastsync/$1
    make_virtualenv $1
}

install_cli() {
    cd $DIR/cli
    make_virtualenv cli
}

install_rest_api() {
    cd $DIR/rest-api
    make_virtualenv rest-api
}

install_admin_console() {
    cd $DIR/admin-console
    npm run setup
}

# Install Singer connectors
install_connector tap-mysql
install_connector tap-postgres
install_connector tap-zendesk
install_connector tap-kafka
install_connector tap-adwords
install_connector target-postgres
install_connector target-snowflake
install_connector transform-field

# Install fastsyncs
install_fastsync mysql-to-snowflake
install_fastsync postgres-to-snowflake

# Install CLI
install_cli

# Install REST API
install_rest_api 

# Install web frontent
install_admin_console

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
