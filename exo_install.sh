#!/bin/bash

# Capture start_time
start_time=`date +%s`

# Source directory defined as location of install.sh
SRC_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Install pipelinewise venvs in the present working directory
PIPELINEWISE_HOME=$(pwd)
VENV_DIR=${PIPELINEWISE_HOME}/.virtualenvs
FAILED_INSTALLS=()

clean_virtualenvs() {
    echo "Cleaning previous installations in $VENV_DIR"
    rm -rf $VENV_DIR
}

make_virtualenv() {
    echo "Making Virtual Environment for [$1] in $VENV_DIR"
    python3 -m venv $VENV_DIR/$1
    source $VENV_DIR/$1/bin/activate && echo "Venv for $1 activated"
    echo "excuting in venv $VIRTUAL_ENV"
    python3 -m pip install --upgrade pip setuptools 
    if [ -f "setup.py" ]; then
        echo "Installing pipelinewise package venv"
        python3 -m pip install --upgrade -e . --use-feature=2020-resolver
    else
        echo "Installing via requirements.txt"
        python3 -m pip install -r requirements.txt || $FAILED_INSTALLS += $1
    fi
    deactivate
    echo "virtualenv for $1 deactivated"
}

install_connector() {
    echo
    echo "--------------------------------------------------------------------------"
    echo "Installing $1 connector..."
    echo "--------------------------------------------------------------------------"

    CONNECTOR_DIR=$SRC_DIR/singer-connectors/$1
    if [[ ! -d $CONNECTOR_DIR ]]; then
        echo "ERROR: Directory not exists and does not look like a valid singer connector: $CONNECTOR_DIR"
        exit 1
    fi

    cd $CONNECTOR_DIR
    make_virtualenv $1
}

print_installed_connectors() {
    cd $SRC_DIR

    echo
    echo "--------------------------------------------------------------------------"
    echo "Installed components:"
    echo "--------------------------------------------------------------------------"
    echo
    echo "Component            Version"
    echo "-------------------- -------"

    for i in `ls $VENV_DIR`; do
        source $VENV_DIR/$i/bin/activate
        VERSION=`python3 -m pip list | grep "$i[[:space:]]" | awk '{print $2}'`
        printf "%-20s %s\n" $i "$VERSION"
    done
}
# Welcome message
cat $SRC_DIR/motd
clean_virtualenvs
# Install PipelineWise core components
cd $SRC_DIR
make_virtualenv pipelinewise

# Set default and extra singer connectors
DEFAULT_CONNECTORS=(
    tap-google-sheets
    tap-jira
    tap-kafka
    tap-mysql
    tap-postgres
    tap-s3-csv
    tap-salesforce
    tap-snowflake
    tap-zendesk
    tap-mongodb
    tap-github
    tap-slack
    tap-mixpanel
    target-s3-csv
    target-snowflake
    target-redshift
    target-postgres
    transform-field
)
EXTRA_CONNECTORS=(
    tap-adwords
    tap-oracle
    tap-zuora
    tap-google-analytics
    tap-shopify
)

CURRENT_CONNECTORS=(
    tap-google-sheets
    tap-s3-csv
    tap-mysql
    tap-postgres
    tap-kafka
    tap-s3-csv
    tap-adwords
    tap-google-analytics
    tap-github
    tap-slack
    target-snowflake
    target-s3-csv
    target-postgres
)

for i in ${CURRENT_CONNECTORS[@]}; do
    install_connector $i
done

# Capture end_time
end_time=`date +%s`
echo
echo "--------------------------------------------------------------------------"
echo "PipelineWise installed successfully in $((end_time-start_time)) seconds"
echo "--------------------------------------------------------------------------"

print_installed_connectors
