#!/usr/bin/env bash
set -e

# Activate virtual environment
source /app/.virtualenvs/pipelinewise/bin/activate
export PIPELINEWISE_HOME=/app

exec "$@"
