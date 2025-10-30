#!/bin/bash
set -e

# Set some bashrc
cat >~/.bashrc <<EOL
# some more ls aliases
alias ll='ls -alF'
alias la='ls -A'
alias l='ls -CF'
alias python=python3.10
EOL

apt -y update
apt -y install \
    make git curl vim less \
    python3.10 \
    python3.10-venv
apt -y upgrade

# Keep container running
tail -f /dev/null
