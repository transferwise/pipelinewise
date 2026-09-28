"""PostgreSQL-to-Snowflake data-diff E2E coverage.

The cross-engine route is where comparison semantics can diverge: Snowflake
uppercases identifiers, returns different numeric types, and hashes differently
from PostgreSQL. Only an end-to-end run against a real Snowflake proves the
adapters agree.
"""

import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from pipelinewise.data_diff.engine import DataDiffExecutionError, HistoricalWindowNotReady, run_check
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import MANAGED_ICEBERG_V3_TABLE_OPTIONS

from ..helpers import tasks
from ..helpers.env import E2EEnv


DIR = os.path.dirname(__file__)
PROJECT_DIR = os.path.join(DIR, 'test-project')
TAP_ID = 'data_diff_postgres_to_sf'
TARGET_ID = 'data_diff_snowflake_dwh'
FULL_CHECK_NAME = f'{TARGET_ID}/{TAP_ID}/logical1/logical1_table1'
EXPECTED_CHECKS = {
    'schema_compatibility',
    'row_count',
    'distinct_key_count',
    'null_key_count',
    'duplicate_key_count',
    'min_key',
    'max_key',
    'row_checksum',
}


@pytest.fixture(params=['postgres', 'mysql', 'mariadb'])
def overlap_source(request):
    """Create an isolated timestamp fixture without resetting replication data."""
    e2e = E2EEnv(PROJECT_DIR)
    if not e2e.env['TARGET_SNOWFLAKE']['is_configured']:
        pytest.skip('TARGET_SNOWFLAKE credentials are not configured')
    source_engine = request.param
    connector, query = {
        'postgres': ('TAP_POSTGRES', e2e.run_query_tap_postgres),
        'mysql': ('TAP_ORACLE_MYSQL', e2e.run_query_tap_oracle_mysql),
        'mariadb': ('TAP_MYSQL', e2e.run_query_tap_mysql),
    }[source_engine]
    if not e2e.env[connector]['is_configured']:
        pytest.skip(f'{connector} credentials are not configured')
    config = {key.lower(): e2e.get_conn_env_var(connector, key) for key in ('HOST', 'PORT', 'USER', 'PASSWORD')}
    config.update(dbname=e2e.get_conn_env_var(connector, 'DB'), engine=source_engine)
    schema = 'public' if source_engine == 'postgres' else config['dbname']
    table = f'ppw_dd_overlap_{uuid4().hex[:12]}'
    quote = '"' if source_engine == 'postgres' else '`'
    qualified_table = f'{quote}{schema}{quote}.{quote}{table}{quote}'
    timestamp_type = 'TIMESTAMP(6)' if source_engine == 'postgres' else 'DATETIME(6)'
    query(f'CREATE TABLE {qualified_table} (id INTEGER PRIMARY KEY, updated_at {timestamp_type}, payload VARCHAR(30))')
    try:
        rows = [
            (row_id, datetime(2026, 1, day) if day else None, f'value-{row_id}')
            for row_id, day in ((1, 1), (2, 2), (3, 3), (4, 9), (5, 10), (6, None))
        ]
        query(
            f'INSERT INTO {qualified_table} (id, updated_at, payload) VALUES '
            + ', '.join('(%s, %s, %s)' for _ in rows),
            tuple(value for row in rows for value in row),
        )
        yield e2e, source_engine, config, schema, table, qualified_table, query
    finally:
        query(f'DROP TABLE IF EXISTS {qualified_table}')


@pytest.mark.parametrize('table_format', ['native', 'iceberg_v3'])
def test_historical_overlap_on_snowflake(overlap_source, table_format):
    """Compare shared history across all source engines and both Snowflake formats."""
    e2e, source_engine, source_config, source_schema, source_table, qualified_source, source_query = overlap_source
    target_config = {
        key.lower(): e2e.get_conn_env_var('TARGET_SNOWFLAKE', key)
        for key in ('ACCOUNT', 'DBNAME', 'USER', 'PRIVATE_KEY', 'WAREHOUSE')
    }
    target_schema = f'PPW_DD_OVERLAP_{uuid4().hex[:12].upper()}'
    qualified_target = f'"{target_schema}"."OVERLAP"'
    target_query = e2e.run_query_target_snowflake
    target_query(f'CREATE SCHEMA "{target_schema}"')
    try:
        iceberg = table_format == 'iceberg_v3'
        table_kind = 'ICEBERG TABLE' if iceberg else 'TABLE'
        options = (
            f" CATALOG = 'SNOWFLAKE' ICEBERG_VERSION = 3 {MANAGED_ICEBERG_V3_TABLE_OPTIONS}"
            if iceberg else ''
        )
        target_query(
            f'CREATE {table_kind} {qualified_target} '
            '(ID NUMBER(38, 0), UPDATED_AT TIMESTAMP_NTZ(6), PAYLOAD VARCHAR(134217728))'
            + options
        )
        if iceberg:
            assert target_query(f'SHOW ICEBERG TABLES IN SCHEMA "{target_schema}"')
        target_query(
            f'INSERT INTO {qualified_target} VALUES '
            "(2, '2026-01-02', 'value-2'), (3, '2026-01-03', 'value-3'), "
            "(4, '2026-01-09', 'value-4'), (5, '2026-01-10', 'different-at-cutoff'), "
            "(6, NULL, 'different-without-timestamp')"
        )
        check = {
            'check_id': str(uuid4()),
            'source_type': 'tap-postgres' if source_engine == 'postgres' else 'tap-mysql',
            'target_type': 'target-snowflake',
            'source_schema': source_schema, 'source_table': source_table,
            'target_schema': target_schema, 'target_table': 'OVERLAP',
            'source_key_column': 'id', 'target_key_column': 'ID',
            'source_timestamp_column': 'updated_at', 'target_timestamp_column': 'UPDATED_AT',
            'source_compare_columns': ['payload'], 'target_compare_columns': ['PAYLOAD'],
            'statement_timeout_seconds': 60, 'checks': tuple(sorted(EXPECTED_CHECKS)),
        }
        cutoff = datetime(2026, 1, 10, tzinfo=timezone.utc)
        resolved_starts = []

        def compare(start=None):
            preflight, results, status = run_check(
                check, source_config, target_config, start, cutoff,
                on_window_start=resolved_starts.append,
            )
            assert preflight['status'] == 'PASS', preflight
            return {result['check_type']: result for result in results}, status

        results, status = compare()
        assert status == 'PASS', results
        assert resolved_starts == [datetime(2026, 1, 2, tzinfo=timezone.utc)]
        assert results['row_count']['source_value'] == results['row_count']['target_value'] == '3'

        source_query(f'DELETE FROM {qualified_source} WHERE id = 1')
        target_query(f"INSERT INTO {qualified_target} VALUES (0, '2025-12-31', 'target-only-history')")
        results, status = compare()
        assert status == 'PASS', results
        assert resolved_starts[-1] == datetime(2026, 1, 2, tzinfo=timezone.utc)

        results, status = compare(datetime(2025, 12, 31, tzinfo=timezone.utc))
        assert status == 'FAIL'
        assert results['row_count']['source_value'] == '3'
        assert results['row_count']['target_value'] == '4'
        assert len(resolved_starts) == 2

        target_query(f"UPDATE {qualified_target} SET PAYLOAD = 'changed' WHERE ID = 3")
        results, status = compare()
        assert status == 'FAIL'
        assert results['row_checksum']['status'] == 'FAIL'
        assert results['row_count']['status'] == 'PASS'

        target_query(f'DELETE FROM {qualified_target} WHERE UPDATED_AT < \'2026-01-10\'')
        with pytest.raises(DataDiffExecutionError, match='target'):
            compare()
        source_query(f'DELETE FROM {qualified_source} WHERE updated_at < \'2026-01-10\'')
        with pytest.raises(HistoricalWindowNotReady, match='source.*target'):
            compare()
    finally:
        target_query(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')


class TestPostgresToSnowflakeDataDiff:
    """Prove data-diff reconciles a PostgreSQL source against a Snowflake replica."""

    def setup_method(self):
        """Render the project and skip unless Snowflake credentials are present."""
        self.e2e = E2EEnv(PROJECT_DIR)
        if not self.e2e.env['TARGET_SNOWFLAKE']['is_configured']:
            pytest.skip('TARGET_SNOWFLAKE credentials are not configured')

        self.run_source_query = self.e2e.run_query_tap_postgres
        self.run_target_query = self.e2e.run_query_target_snowflake
        self.run_backend_query = self.e2e.run_query_pipelinewise_backend
        self.target_schema = (
            f'ppw_e2e_data_diff_sf{self.e2e.sf_schema_postfix}'.upper()
        )

    def teardown_method(self):
        """Drop the Snowflake schema this run created."""
        if hasattr(self, 'run_target_query'):
            self.run_target_query(
                f'DROP SCHEMA IF EXISTS {self.target_schema} CASCADE'
            )

    @staticmethod
    def _run_success(command):
        """Run one CLI command and return stdout after asserting success.

        Asserts on the exit code rather than using assert_command_success: the
        Snowflake connector logs its version banner to stderr, which that helper
        treats as a failure.
        """
        return_code, stdout, stderr = tasks.run_command(command)
        assert return_code == 0, f'{command} failed\n{stdout}\n{stderr}'
        return stdout

    @staticmethod
    def _run_json(command):
        """Run a CLI command expecting JSON output; strip leading log lines."""
        stdout = TestPostgresToSnowflakeDataDiff._run_success(command)
        for index, char in enumerate(stdout):
            if char in ('[', '{'):
                return json.loads(stdout[index:])
        raise ValueError(f'No JSON found in command output: {stdout[:200]}')

    def test_dd_reconciles_postgres_source_against_snowflake_target(self):
        """Every check type must PASS against a faithfully replicated table."""
        self.e2e.setup_tap_postgres()
        self.e2e.setup_pipelinewise_backend()
        self.run_target_query(
            f'DROP SCHEMA IF EXISTS {self.target_schema} CASCADE'
        )

        # Anchor fixture rows inside the previous completed UTC hour so the
        # comparison window is non-empty regardless of test start time.
        self.run_source_query(
            "UPDATE logical1.logical1_table1 "
            "SET updated_at = date_trunc('hour', CURRENT_TIMESTAMP) - interval '1 hour'"
        )

        self._run_success(f'pipelinewise validate --dir {PROJECT_DIR}')
        self._run_success(
            f'pipelinewise import_config --dir {PROJECT_DIR} --taps {TAP_ID}'
        )
        self._run_success(
            f'pipelinewise run_tap --tap {TAP_ID} --target {TARGET_ID}'
        )

        checks = self._run_json(
            f'pipelinewise list_data_diff_checks --tap {TAP_ID}'
            f' --target {TARGET_ID} --output-format json'
        )
        assert [check['full_check_name'] for check in checks] == [FULL_CHECK_NAME]
        assert set(checks[0]['checks']) == EXPECTED_CHECKS

        self._run_success(
            f'pipelinewise run_data_diff_checks --tap {TAP_ID}'
            f' --target {TARGET_ID}'
        )

        results = self.run_backend_query(
            f"""
            SELECT results.check_type, results.status
              FROM public.dd_run_results results
              JOIN public.dd_run_attempts runs ON runs.run_id = results.run_id
              JOIN public.dd_check_definitions checks ON checks.check_id = runs.check_id
             WHERE checks.full_check_name = '{FULL_CHECK_NAME}'
             ORDER BY results.check_type
            """
        )
        assert {check_type for check_type, _ in results} == EXPECTED_CHECKS
        failed = [(name, status) for name, status in results if status != 'PASS']
        assert not failed, f'cross-engine comparison did not agree: {failed}'

    def test_dd_detects_a_snowflake_target_that_lost_rows(self):
        """A row deleted only in Snowflake must fail the count-based checks."""
        self.e2e.setup_tap_postgres()
        self.e2e.setup_pipelinewise_backend()
        self.run_target_query(
            f'DROP SCHEMA IF EXISTS {self.target_schema} CASCADE'
        )
        self.run_source_query(
            "UPDATE logical1.logical1_table1 "
            "SET updated_at = date_trunc('hour', CURRENT_TIMESTAMP) - interval '1 hour'"
        )

        self._run_success(
            f'pipelinewise import_config --dir {PROJECT_DIR} --taps {TAP_ID}'
        )
        self._run_success(
            f'pipelinewise run_tap --tap {TAP_ID} --target {TARGET_ID}'
        )

        # Diverge the replica behind PipelineWise's back.
        self.run_target_query(
            f'DELETE FROM {self.target_schema}.LOGICAL1_TABLE1'
            ' WHERE CID = (SELECT MIN(CID) FROM'
            f' {self.target_schema}.LOGICAL1_TABLE1)'
        )

        return_code, stdout, stderr = tasks.run_command(
            f'pipelinewise run_data_diff_checks --tap {TAP_ID}'
            f' --target {TARGET_ID} --force'
        )
        assert return_code != 0, f'expected mismatch exit\n{stdout}\n{stderr}'

        statuses = dict(
            self.run_backend_query(
                f"""
                SELECT results.check_type, results.status
                  FROM public.dd_run_results results
                  JOIN public.dd_run_attempts runs ON runs.run_id = results.run_id
                  JOIN public.dd_check_definitions checks
                    ON checks.check_id = runs.check_id
                 WHERE checks.full_check_name = '{FULL_CHECK_NAME}'
                   AND runs.attempt = (
                       SELECT MAX(attempt) FROM public.dd_run_attempts inner_runs
                        WHERE inner_runs.check_id = runs.check_id
                   )
                 ORDER BY results.check_type
                """
            )
        )
        assert statuses['row_count'] == 'FAIL'
        assert statuses['row_checksum'] == 'FAIL'
        assert statuses['schema_compatibility'] == 'PASS'
