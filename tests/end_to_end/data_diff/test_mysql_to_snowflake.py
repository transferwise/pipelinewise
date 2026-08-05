"""MySQL/MariaDB-to-Snowflake data-diff E2E coverage.

The only supported route where neither side is the PostgreSQL implementation:
MySQL sums ``CONV(SUBSTRING(MD5(...)))`` while Snowflake uses
``MD5_NUMBER_LOWER64``. Checksum arithmetic that disagrees between those two
dialects is invisible to every other route's tests.
"""

import json
import os

import pytest

from ..helpers import tasks
from ..helpers.env import E2EEnv


DIR = os.path.dirname(__file__)
PROJECT_DIR = os.path.join(DIR, "test-project")
TAP_ID = "data_diff_mysql_to_sf"
TARGET_ID = "data_diff_snowflake_dwh"
SOURCE_TABLE = "weight_unit"
EXPECTED_CHECKS = {
    "schema_compatibility",
    "row_count",
    "distinct_key_count",
    "null_key_count",
    "duplicate_key_count",
    "min_key",
    "max_key",
    "row_checksum",
}


class TestMySqlToSnowflakeDataDiff:
    """Prove data-diff reconciles a MySQL source against a Snowflake replica."""

    def setup_method(self):
        """Render the project and skip unless Snowflake credentials are present."""
        self.e2e = E2EEnv(PROJECT_DIR)
        if not self.e2e.env["TARGET_SNOWFLAKE"]["is_configured"]:
            pytest.skip("TARGET_SNOWFLAKE credentials are not configured")

        self.run_source_query = self.e2e.run_query_tap_mysql
        self.run_target_query = self.e2e.run_query_target_snowflake
        self.run_backend_query = self.e2e.run_query_pipelinewise_backend
        self.source_database = self.e2e.get_conn_env_var("TAP_MYSQL", "DB")
        self.full_check_name = f"{TARGET_ID}/{TAP_ID}/{self.source_database}/{SOURCE_TABLE}"
        self.target_schema = f"ppw_e2e_data_diff_mysql_sf{self.e2e.sf_schema_postfix}".upper()

    def teardown_method(self):
        """Drop the Snowflake schema this run created."""
        if hasattr(self, "run_target_query"):
            self.run_target_query(f"DROP SCHEMA IF EXISTS {self.target_schema} CASCADE")

    @staticmethod
    def _run_success(command):
        """Run one CLI command, asserting on the exit code.

        Not assert_command_success: the Snowflake connector logs its version to
        stderr, which that helper treats as a failure.
        """
        return_code, stdout, stderr = tasks.run_command(command)
        assert return_code == 0, f"{command} failed\n{stdout}\n{stderr}"
        return stdout

    @staticmethod
    def _run_json(command):
        """Run a CLI command expecting JSON output; strip leading log lines."""
        stdout = TestMySqlToSnowflakeDataDiff._run_success(command)
        for index, char in enumerate(stdout):
            if char in ("[", "{"):
                return json.loads(stdout[index:])
        raise ValueError(f"No JSON found in command output: {stdout[:200]}")

    def _latest_results(self):
        """Return {check_type: status} for the newest attempt of this check."""
        return dict(
            self.run_backend_query(
                f"""
                SELECT results.check_type, results.status
                  FROM public.dd_results results
                  JOIN public.dd_runs runs ON runs.run_id = results.run_id
                  JOIN public.dd_checks checks
                    ON checks.check_id = runs.dd_check_id
                 WHERE checks.full_check_name = '{self.full_check_name}'
                   AND runs.run_id = (
                       SELECT inner_runs.run_id
                         FROM public.dd_runs inner_runs
                        WHERE inner_runs.dd_check_id = runs.dd_check_id
                        ORDER BY inner_runs.scheduled_for DESC,
                                 inner_runs.attempt DESC
                        LIMIT 1
                   )
                """
            )
        )

    def _replicate(self):
        """Seed the source, import the project, and replicate into Snowflake."""
        self.e2e.setup_tap_mysql()
        self.e2e.setup_pipelinewise_backend()
        self.run_target_query(f"DROP SCHEMA IF EXISTS {self.target_schema} CASCADE")
        # Anchor every row inside the comparison window regardless of start time,
        # and make isActive NULL on one row so the boolean NULL path is compared.
        self.run_source_query(f"UPDATE {SOURCE_TABLE} SET date_updated = NOW() - INTERVAL 1 HOUR")
        self.run_source_query(
            f"UPDATE {SOURCE_TABLE} SET isActive = NULL "
            f"WHERE weight_unit_id = (SELECT MIN(weight_unit_id) FROM "
            f"(SELECT weight_unit_id FROM {SOURCE_TABLE}) inner_rows)"
        )
        self._run_success(f"pipelinewise import_config --dir {PROJECT_DIR}")
        self._run_success(f"pipelinewise run_tap --tap {TAP_ID} --target {TARGET_ID}")

    def test_dd_reconciles_mysql_source_against_snowflake_target(self):
        """Every check type must PASS, including the cross-dialect checksum."""
        self._replicate()

        checks = self._run_json(
            f"pipelinewise list_data_diff_checks --target {TARGET_ID} --tap {TAP_ID} --output-format json"
        )
        assert len(checks) == 1
        assert checks[0]["full_check_name"] == self.full_check_name
        assert set(checks[0]["checks"]) == EXPECTED_CHECKS

        source_count = self.run_source_query(f"SELECT COUNT(*) FROM {SOURCE_TABLE}")[0][0]
        target_count = self.run_target_query(f"SELECT COUNT(*) FROM {self.target_schema}.{SOURCE_TABLE.upper()}")[0][0]
        assert target_count == source_count

        self._run_success(f"pipelinewise run_data_diff_checks --target {TARGET_ID} --tap {TAP_ID}")

        statuses = self._latest_results()
        assert set(statuses) == EXPECTED_CHECKS
        failed = {name: status for name, status in statuses.items() if status != "PASS"}
        assert not failed, f"MySQL to Snowflake comparison disagreed: {failed}"

    def test_dd_detects_a_snowflake_target_that_diverged(self):
        """A compared column changed only in Snowflake must fail the checksum."""
        self._replicate()

        # Every row, not one: a single row could fall outside the window, where the
        # mismatch would be correctly invisible. Counts stay identical either way.
        self.run_target_query(
            f"UPDATE {self.target_schema}.{SOURCE_TABLE.upper()} SET WEIGHT_UNIT_NAME = WEIGHT_UNIT_NAME || '-diverged'"
        )

        return_code, stdout, stderr = tasks.run_command(
            f"pipelinewise run_data_diff_checks --target {TARGET_ID} --tap {TAP_ID} --force"
        )
        assert return_code != 0, f"expected a mismatch exit\n{stdout}\n{stderr}"

        statuses = self._latest_results()
        # Only the checksum inspects column values, so it alone sees this drift.
        assert statuses["row_checksum"] == "FAIL"
        assert statuses["row_count"] == "PASS"
        assert statuses["schema_compatibility"] == "PASS"
