
DIR=$(pwd)
VENV_DIR=$DIR/.virtualenvs
SINGER_ADMIN_DIR=$DIR/singer-admin

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

install_cli() {
    cd $DIR/cli
    make_virtualenv cli
}

install_rest_api() {
    cd $DIR/rest-api
    make_virtualenv rest-api
}

# Install Singer connectors
install_connector tap-mysql
install_connector tap-postgres
install_connector target-postgres

# Install CLI
install_cli

# Install REST API
install_rest_api 

# Install web frontent
cd $SINGER_ADMIN_DIR && npm run setup