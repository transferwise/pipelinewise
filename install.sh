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

check_license() {
    pip install pip-licenses

    echo
    echo "Checking license..."
    PKG_NAME=`pip-licenses | grep $1 | awk '{print $1}'`
    PKG_VERSION=`pip-licenses | grep $1 | awk '{print $2}'`
    PKG_LICENSE=`pip-licenses --from mixed | grep $1 | awk '{for (i=1; i<=NF-2; i++) $i = $(i+2); NF-=2; print}'`

    # Any License Agreement that is not MIT has to be accepted
    if [[ $PKG_LICENSE != "MIT License" && $PKG_LICENSE != 'UNKNOWN' ]]; then
        echo
        echo "  | $PKG_NAME ($PKG_VERSION) is licensed under $PKG_LICENSE"
        echo "  |"
        echo "  | WARNING. The license of this connector is different than the default PipelineWise license (MIT)."

        if [[ $ACCEPT_LICENSES != "YES" ]]; then
            echo "  | You need to accept the connector's license agreement to proceed."
            echo "  |"
            read -r -p "  | Do you accept the [$PKG_LICENSE] license agreement of $PKG_NAME connector? [y/N] " response
            case "$response" in
                [yY][eE][sS]|[yY])
                    ;;
                *)
                    echo
                    echo "EXIT. License agreement not accepted"
                    exit 1
                    ;;
            esac
        else
            echo "  | You automatically accepted this license agreement by running this script with --acceptlicenses option."
        fi

    fi
}

make_virtualenv() {
    echo "Making Virtual Environment for $1"
    python3 -m venv $VENV_DIR/$1
    source $VENV_DIR/$1/bin/activate
    python3 -m pip install --upgrade pip
    if [ -f "requirements.txt" ]; then
        python3 -m pip install -r requirements.txt
    fi
    if [ -f "setup.py" ]; then
        python3 -m pip install .
    fi

    check_license $1
    deactivate
}

install_connector() {
    echo
    echo "--------------------------------------------------------------------------"
    echo "Insalling $1 connector..."
    echo "--------------------------------------------------------------------------"
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

# Parse command line arguments
for arg in "$@"; do
    case $1 in
        # Auto accept license agreemnets. Useful if PipelineWise installed by an automated script
        --acceptlicenses)
            ACCEPT_LICENSES="YES"
    esac
done


# Install Singer connectors
install_connector tap-mysql
install_connector tap-postgres
install_connector tap-zendesk
install_connector tap-kafka
install_connector tap-adwords
install_connector tap-s3-csv
install_connector tap-snowflake
install_connector tap-salesforce
install_connector tap-jira
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
