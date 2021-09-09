#!/bin/bash

# Exit script on first error
set -e

# Capture start_time
start_time=`date +%s`

# Source directory defined as location of install.sh
SRC_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Install pipelinewise venvs in the present working directory
PIPELINEWISE_HOME=$(pwd)
VENV_DIR=${PIPELINEWISE_HOME}/.virtualenvs

check_license() {
    python3 -m pip install pip-licenses

    echo
    echo "Checking license..."
    PKG_NAME=`pip-licenses | grep "$1[[:space:]]" | awk '{print $1}'`
    PKG_VERSION=`pip-licenses | grep "$1[[:space:]]" | awk '{print $2}'`
    PKG_LICENSE=`pip-licenses --from mixed | grep "$1[[:space:]]" | awk '{for (i=1; i<=NF-2; i++) $i = $(i+2); NF-=2; print}'`

    # Any License Agreement that is not Apache Software License (2.0) has to be accepted
    MAIN_LICENSE="Apache Software License"
    if [[ $PKG_LICENSE != $MAIN_LICENSE && $PKG_LICENSE != 'UNKNOWN' ]]; then
        echo
        echo "  | $PKG_NAME ($PKG_VERSION) is licensed under $PKG_LICENSE"
        echo "  |"
        echo "  | WARNING. The license of this connector is different than the default PipelineWise license ($MAIN_LICENSE)."

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

clean_virtualenvs() {
    echo "Cleaning previous installations in $VENV_DIR"
    rm -rf $VENV_DIR
}

make_virtualenv() {
    echo "Making Virtual Environment for [$1] in $VENV_DIR"
    python3 -m venv $VENV_DIR/$1
    source $VENV_DIR/$1/bin/activate
    python3 -m pip install --upgrade pip setuptools wheel

    if [ -f "pre_requirements.txt" ]; then
        python3 -m pip install --upgrade -r pre_requirements.txt
    fi
    if [ -f "requirements.txt" ]; then
        python3 -m pip install --upgrade -r requirements.txt
    fi
    if [ -f "setup.py" ]; then
        PIP_ARGS=
        if [[ ! $NO_TEST_EXTRAS == "YES" ]]; then
            PIP_ARGS=$PIP_ARGS"[test]"
        fi

        python3 -m pip install --upgrade -e .$PIP_ARGS
    fi

    echo ""

    check_license $1
    deactivate
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

    if [[ $CONNECTORS != "all" ]]; then
        echo
        echo "WARNING: Not every singer connector installed. If you are missing something use the --connectors=...,... argument"
        echo "         with an explicit list of required connectors or use the --connectors=all to install every available"
        echo "         connector"
    fi
}

# Parse command line arguments
for arg in "$@"; do
    case $arg in
        # Auto accept license agreements. Useful if PipelineWise installed by an automated script
        --acceptlicenses)
            ACCEPT_LICENSES="YES"
            ;;
        # Do not print usage information at the end of the install
        --nousage)
            NO_USAGE="YES"
            ;;
        # Install with test requirements that allows running tests
        --notestextras)
            NO_TEST_EXTRAS="YES"
            ;;
        # Install extra connectors
        --connectors=*)
            CONNECTORS="${arg#*=}"
            shift
            ;;
        # Clean previous installation
        --clean)
            clean_virtualenvs
            exit 0
            ;;
        *)
            echo "Invalid argument: $arg"
            exit 1
            ;;
    esac
done

# Welcome message
if ! ENVSUBST_LOC="$(type -p "envsubst")" || [[ -z ENVSUBST_LOC ]]; then
  echo "envsubst not found but it's required to run this script. Try to install gettext or gettext-base package"
  exit 1
fi

CURRENT_YEAR=$(date +"%Y") envsubst < $SRC_DIR/motd

# Install PipelineWise core components
cd $SRC_DIR
make_virtualenv pipelinewise

# Set default and extra singer connectors
DEFAULT_CONNECTORS=(
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
    tap-twilio
    target-s3-csv
    target-snowflake
    target-redshift
    target-postgres
    target-bigquery
    transform-field
)
EXTRA_CONNECTORS=(
    tap-adwords
    tap-oracle
    tap-zuora
    tap-google-analytics
    tap-shopify
)

# Install only the default connectors if --connectors argument not passed
if [[ -z $CONNECTORS ]]; then
    for i in ${DEFAULT_CONNECTORS[@]}; do
        install_connector $i
    done

# don't install any connectors if --connectors=none passed
elif [[ $CONNECTORS == "none" ]]; then
  echo "No connectors will be installed"

# Install every available connectors if --connectors=all passed
elif [[ $CONNECTORS == "all" ]]; then
    for i in ${DEFAULT_CONNECTORS[@]}; do
        install_connector $i
    done
    for i in ${EXTRA_CONNECTORS[@]}; do
        install_connector $i
    done

# Install the selected connectors if --connectors argument passed
elif [[ ! -z $CONNECTORS ]]; then
    OLDIFS=$IFS
    IFS=,
    for connector in $CONNECTORS; do
        install_connector $connector
    done
    IFS=$OLDIFS
fi

# Capture end_time
end_time=`date +%s`
echo
echo "--------------------------------------------------------------------------"
echo "PipelineWise installed successfully in $((end_time-start_time)) seconds"
echo "--------------------------------------------------------------------------"

if [[ $CONNECTORS != "none" ]]; then
  print_installed_connectors
fi

if [[ $NO_USAGE != "YES" ]]; then
    echo
    echo "To start CLI:"
    echo " $ source $VENV_DIR/pipelinewise/bin/activate"
    echo " $ export PIPELINEWISE_HOME=$PIPELINEWISE_HOME"

    echo " $ pipelinewise status"
    echo
    echo "--------------------------------------------------------------------------"
fi
