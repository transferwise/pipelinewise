#!/usr/bin/env bash
set -e

# Activate virtual environment
source /app/.virtualenvs/pipelinewise/bin/activate
export PIPELINEWISE_HOME=/app

WORK_DIR=/app/wrk
mkdir -p ${WORK_DIR}
cd ${WORK_DIR}

exec pipelinewise \
    "$@"
