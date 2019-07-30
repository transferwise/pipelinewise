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
        PIP_ARGS=
        if [[ $WITH_TEST_EXTRAS == "YES" ]]; then
            PIP_ARGS=$PIP_ARGS"[test]"
        fi

        python3 -m pip install .$PIP_ARGS
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

    echo "Installed components:"
    echo
    echo "--------------------------------------------------------------------------"
    echo "Installed components"
    echo "--------------------------------------------------------------------------"
    echo
    echo "Component            Version"
    echo "-------------------- -------"

    for i in `ls singer-connectors`; do
        VERSION=1
        REQUIREMENTS_TXT=$SRC_DIR/singer-connectors/$i/requirements.txt
        SETUP_PY=$SRC_DIR/singer-connectors/$i/setup.py
        if [ -f $REQUIREMENTS_TXT ]; then
            VERSION=`grep $i $REQUIREMENTS_TXT | cut -f 3 -d "="`
        elif [ -f $SETUP_PY ]; then
            VERSION="`python3 $SETUP_PY -V` (not from PyPI)"
        fi

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
        --withtestextras)
            WITH_TEST_EXTRAS="YES"
            ;;
        *)
            echo "Invalid argument: $arg"
            exit 1
            ;;
    esac
done

# Welcome message
cat motd

# Install PipelineWise core components
make_virtualenv pipelinewise

# Install Singer connectors
for i in `ls singer-connectors`; do
    install_connector $i
done

# Capture end_time
end_time=`date +%s`

print_installed_connectors
if [[ $NO_USAGE != "YES" ]]; then
    echo
    echo "--------------------------------------------------------------------------"
    echo "PipelineWise installed successfully in $((end_time-start_time)) seconds"
    echo "--------------------------------------------------------------------------"
    echo
    echo "To start CLI:"
    echo " $ source $VENV_DIR/pipelinewise/bin/activate"
    echo " $ export PIPELINEWISE_HOME=$PIPELINEWISE_HOME"

    echo " $ pipelinewise status"
    echo
    echo "--------------------------------------------------------------------------"
fi
