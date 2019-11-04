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
    PKG_NAME=`pip-licenses | grep $1 | awk '{print $1}'`
    PKG_VERSION=`pip-licenses | grep $1 | awk '{print $2}'`
    PKG_LICENSE=`pip-licenses --from mixed | grep $1 | awk '{for (i=1; i<=NF-2; i++) $i = $(i+2); NF-=2; print}'`

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

make_virtualenv() {
    echo "Making Virtual Environment for [$1] in $VENV_DIR"
    python3 -m venv $VENV_DIR/$1
    source $VENV_DIR/$1/bin/activate
    python3 -m pip install --upgrade pip
    if [ -f "requirements.txt" ]; then
        python3 -m pip install -r requirements.txt
    fi
    if [ -f "setup.py" ]; then
        PIP_ARGS=
        if [[ ! $NO_TEST_EXTRAS == "YES" ]]; then
            PIP_ARGS=$PIP_ARGS"[test]"
        fi

        python3 -m pip install -e .$PIP_ARGS
    fi

    check_license $1
    deactivate
}

install_connector() {
    echo
    echo "--------------------------------------------------------------------------"
    echo "Installing $1 connector..."
    echo "--------------------------------------------------------------------------"
    cd $SRC_DIR/singer-connectors/$1
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
        VERSION=`python3 -m pip list | grep $i | awk '{print $2}'`
        printf "%-20s %s\n" $i "$VERSION"
    done
}

# Parse command line arguments
for arg in "$@"; do
    case $arg in
        # Auto accept license agreemnets. Useful if PipelineWise installed by an automated script
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
        *)
            echo "Invalid argument: $arg"
            exit 1
            ;;
    esac
done

# Welcome message
cat $SRC_DIR/motd

# Install PipelineWise core components
cd $SRC_DIR
make_virtualenv pipelinewise

# Install Singer connectors
install_connector tap-adwords
install_connector tap-jira
install_connector tap-kafka
install_connector tap-mysql
install_connector tap-postgres
install_connector tap-s3-csv
install_connector tap-salesforce
install_connector tap-snowflake
install_connector tap-zendesk
install_connector target-s3-csv
install_connector target-snowflake
install_connector transform-field
install_connector tap-oracle
install_connector target-postgres
install_connector target-redshift


# Capture end_time
end_time=`date +%s`

pip list

echo
echo "--------------------------------------------------------------------------"
echo "PipelineWise installed successfully in $((end_time-start_time)) seconds"
echo "--------------------------------------------------------------------------"

print_installed_connectors
if [[ $NO_USAGE != "YES" ]]; then
    echo
    echo "To start CLI:"
    echo " $ source $VENV_DIR/pipelinewise/bin/activate"
    echo " $ export PIPELINEWISE_HOME=$PIPELINEWISE_HOME"

    echo " $ pipelinewise status"
    echo
    echo "--------------------------------------------------------------------------"
fi
