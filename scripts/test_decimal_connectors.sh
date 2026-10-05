#!/usr/bin/env bash
set -euo pipefail

run_decimal_tests() {
    local connector=$1
    shift
    make -B -C "singer-connectors/${connector}" venv
    (
        cd "singer-connectors/${connector}"
        venv/bin/python -m pytest -v "$@"
    )
}

case "${1:-}" in
    postgres)
        run_decimal_tests tap-mysql tests/integration/test_decimal_mapping.py
        run_decimal_tests tap-postgres tests/integration/test_decimal_mapping.py
        run_decimal_tests target-postgres tests/integration/test_decimals.py
        ;;
    snowflake)
        export CLIENT_SIDE_ENCRYPTION_MASTER_KEY=
        export TARGET_SNOWFLAKE_FILE_FORMAT_CSV="${TARGET_SNOWFLAKE_FILE_FORMAT_CSV:-${TARGET_SNOWFLAKE_FILE_FORMAT}}"
        shift
        decimal_tests=("$@")
        if [ "${#decimal_tests[@]}" -eq 0 ]; then
            decimal_tests=(tests/integration/test_decimals.py)
        fi
        run_decimal_tests target-snowflake "${decimal_tests[@]}"
        ;;
    *)
        echo 'Usage: bash scripts/test_decimal_connectors.sh postgres|snowflake [pytest-selector ...]' >&2
        exit 2
        ;;
esac
