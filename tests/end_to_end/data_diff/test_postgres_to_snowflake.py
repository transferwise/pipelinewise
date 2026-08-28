"""PostgreSQL-to-Snowflake data-diff E2E coverage.

The cross-engine route is where comparison semantics can diverge: Snowflake
uppercases identifiers, returns different numeric types, and hashes differently
from PostgreSQL. Only an end-to-end run against a real Snowflake proves the
adapters agree.
"""

import json
import os

import pytest

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


# pylint: disable=attribute-defined-outside-init
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
              FROM public.dd_results results
              JOIN public.dd_runs runs ON runs.run_id = results.run_id
              JOIN public.dd_checks checks ON checks.check_id = runs.dd_check_id
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
                  FROM public.dd_results results
                  JOIN public.dd_runs runs ON runs.run_id = results.run_id
                  JOIN public.dd_checks checks
                    ON checks.check_id = runs.dd_check_id
                 WHERE checks.full_check_name = '{FULL_CHECK_NAME}'
                   AND runs.attempt = (
                       SELECT MAX(attempt) FROM public.dd_runs inner_runs
                        WHERE inner_runs.dd_check_id = runs.dd_check_id
                   )
                 ORDER BY results.check_type
                """
            )
        )
        assert statuses['row_count'] == 'FAIL'
        assert statuses['row_checksum'] == 'FAIL'
        assert statuses['schema_compatibility'] == 'PASS'
