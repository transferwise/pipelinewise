#!/usr/bin/env bash
set -euo pipefail

run_decimal_tests() {
    local connector=$1
    local test_file=$2
    make -B -C "singer-connectors/${connector}" venv
    (
        cd "singer-connectors/${connector}"
        venv/bin/python -m pytest -v "tests/integration/${test_file}"
    )
}

case "${1:-}" in
    postgres)
        run_decimal_tests tap-mysql test_decimal_mapping.py
        run_decimal_tests tap-postgres test_decimal_mapping.py
        run_decimal_tests target-postgres test_decimals.py
        ;;
    snowflake)
        export CLIENT_SIDE_ENCRYPTION_MASTER_KEY=
        export TARGET_SNOWFLAKE_FILE_FORMAT_CSV="${TARGET_SNOWFLAKE_FILE_FORMAT_CSV:-${TARGET_SNOWFLAKE_FILE_FORMAT}}"
        run_decimal_tests target-snowflake test_decimals.py
        ;;
    *)
        echo 'Usage: bash scripts/test_decimal_connectors.sh postgres|snowflake' >&2
        exit 2
        ;;
esac
