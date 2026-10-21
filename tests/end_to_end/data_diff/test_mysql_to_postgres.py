"""MySQL/MariaDB-to-PostgreSQL data-diff E2E coverage.

MySQL generates checksum SQL through ``DATE_FORMAT``, whose ``%Y``-style
specifiers collide with PyMySQL's client-side parameter interpolation. Unit tests
assert the generated string; only executing it proves the query reaches the
database and agrees with the PostgreSQL replica.
"""

import json
import os
import uuid

from unittest.mock import Mock

import pytest

from pipelinewise.data_diff.engine import connect_source, preflight_source

from ..helpers import assertions, tasks
from ..helpers.env import E2EEnv


DIR = os.path.dirname(__file__)
PROJECT_DIR = os.path.join(DIR, 'test-project')
TAP_ID = 'data_diff_mysql_to_pg'
TARGET_ID = 'data_diff_postgres_dwh'
TARGET_SCHEMA = 'ppw_e2e_data_diff_mysql'
SOURCE_TABLE = 'weight_unit'
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


class TestMySqlToPostgresDataDiff:
    """Prove data-diff reconciles a MySQL source against a PostgreSQL replica."""

    def setup_method(self):
        """Render the isolated E2E project and initialize query helpers."""
        self.e2e = E2EEnv(PROJECT_DIR)
        self.run_source_query = self.e2e.run_query_tap_mysql
        self.run_target_query = self.e2e.run_query_target_postgres
        self.run_backend_query = self.e2e.run_query_pipelinewise_backend
        self.full_check_name = (
            f'{TARGET_ID}/{TAP_ID}/'
            f"{self.e2e.get_conn_env_var('TAP_MYSQL', 'DB')}/{SOURCE_TABLE}"
        )

    @pytest.mark.parametrize('connector', ['TAP_MYSQL', 'TAP_ORACLE_MYSQL'])
    def test_unindexed_preflight_counts_rows_including_null_timestamps(self, connector):
        """Exercise exact boundaries with MariaDB and genuine MySQL row counts."""
        connection_config = {
            field: self.e2e.get_conn_env_var(connector, environment_key)
            for field, environment_key in (
                ('host', 'HOST'), ('port', 'PORT'), ('user', 'USER'),
                ('password', 'PASSWORD'), ('dbname', 'DB'),
            )
        }
        run_source_query = (
            self.e2e.run_query_tap_mysql if connector == 'TAP_MYSQL' else self.e2e.run_query_tap_oracle_mysql
        )
        table = f'dd preflight_{uuid.uuid4().hex}'
        qualified = f'`{connection_config["dbname"]}`.`{table}`'
        check = {'source_type': 'tap-mysql', 'statement_timeout_seconds': 30}
        sql = f'SELECT COUNT(*) AS row_count FROM {qualified} WHERE updated_at IS NOT NULL'
        digits = ' UNION ALL '.join(f'SELECT {digit} AS n' for digit in range(10))
        positions = ('ones', 'tens', 'hundreds', 'thousands', 'ten_thousands')
        number = ' + '.join(f'{10 ** position} * {alias}.n' for position, alias in enumerate(positions))
        numbers = ' CROSS JOIN '.join(f'({digits}) AS {alias}' for alias in positions)
        run_source_query(f'CREATE TABLE {qualified} (id BIGINT PRIMARY KEY, updated_at DATETIME)')
        try:
            previous_count = 0
            for row_count in (49_999, 50_000, 99_999, 100_000, 100_001):
                if row_count - previous_count == 1:
                    run_source_query(f'INSERT INTO {qualified} (id) VALUES (%s)', (row_count,))
                else:
                    run_source_query(
                        f'INSERT INTO {qualified} (id) SELECT {number} + 1 FROM {numbers} '
                        f'WHERE {number} >= %s AND {number} < %s',
                        (previous_count, row_count),
                    )
                previous_count = row_count
                adapter = connect_source(check, connection_config)
                try:
                    preflight = preflight_source(adapter, connection_config['dbname'], table, 'updated_at', sql, ())
                    assert preflight['status'] == ('PASS' if row_count < 100_000 else 'BLOCKED'), preflight
                    assert preflight['table_rows'] == min(row_count, 100_000)
                    assert preflight['row_limit'] == 100_000
                    assert preflight['has_leading_index'] is False
                    assert preflight['index_warning'] is (50_000 <= row_count < 100_000)
                    if preflight['status'] == 'PASS':
                        assert adapter.execute_metrics(sql, (), ('row_count',)).values == {'row_count': '0'}
                finally:
                    adapter.close()

            run_source_query(f'CREATE INDEX dd_timestamp ON {qualified} (updated_at)')
            adapter = connect_source(check, connection_config)
            try:
                count = Mock(wraps=adapter.count_rows_up_to)
                adapter.count_rows_up_to = count
                preflight = preflight_source(adapter, connection_config['dbname'], table, 'updated_at', sql, ())
                assert preflight['status'] == 'PASS', preflight
                assert preflight['has_leading_index'] is True
                assert preflight['table_rows'] is preflight['row_limit'] is None
                assert preflight['index_warning'] is False
                count.assert_not_called()
            finally:
                adapter.close()
        finally:
            run_source_query(f'DROP TABLE {qualified}')

    @staticmethod
    def _run_success(command):
        """Run one CLI command and return stdout after asserting success."""
        return_code, stdout, stderr = tasks.run_command(command)
        assertions.assert_command_success(return_code, stdout, stderr)
        return stdout

    @staticmethod
    def _run_json(command):
        """Run a CLI command expecting JSON output; strip leading log lines."""
        stdout = TestMySqlToPostgresDataDiff._run_success(command)
        for index, char in enumerate(stdout):
            if char in ('[', '{'):
                return json.loads(stdout[index:])
        raise ValueError(f'No JSON found in command output: {stdout[:200]}')

    def _latest_results(self):
        """Return {check_type: status} for the newest attempt of this check."""
        return dict(
            self.run_backend_query(
                f"""
                SELECT results.check_type, results.status
                  FROM public.dd_run_results results
                  JOIN public.dd_run_attempts runs ON runs.run_id = results.run_id
                  JOIN public.dd_check_definitions checks
                    ON checks.check_id = runs.check_id
                 WHERE checks.full_check_name = '{self.full_check_name}'
                   AND runs.run_id = (
                       SELECT inner_runs.run_id
                         FROM public.dd_run_attempts inner_runs
                        WHERE inner_runs.check_id = runs.check_id
                        ORDER BY inner_runs.scheduled_for DESC,
                                 inner_runs.attempt DESC
                        LIMIT 1
                   )
                """
            )
        )

    def test_dd_reconciles_mysql_source_against_postgres_target(self):
        """Every check type, including row_checksum, must PASS across engines."""
        self.e2e.setup_tap_mysql()
        self.e2e.setup_pipelinewise_backend()
        self.run_target_query(f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE')

        # Anchor every row inside the comparison window regardless of start time.
        self.run_source_query(
            f'UPDATE {SOURCE_TABLE} SET date_updated = NOW() - INTERVAL 1 HOUR'
        )
        source_count = self.run_source_query(
            f'SELECT COUNT(*) FROM {SOURCE_TABLE}'
        )[0][0]
        assert source_count > 0

        self._run_success(f'pipelinewise validate --dir {PROJECT_DIR}')
        self._run_success(
            f'pipelinewise import_config --dir {PROJECT_DIR} --taps {TAP_ID}'
        )

        checks = self._run_json(
            'pipelinewise list_data_diff_checks '
            f'--target {TARGET_ID} --tap {TAP_ID} --output-format json'
        )
        assert len(checks) == 1
        assert checks[0]['full_check_name'] == self.full_check_name
        assert set(checks[0]['checks']) == EXPECTED_CHECKS

        # FULL_TABLE is handled entirely by FastSync, so no singer log is produced.
        assertions.assert_run_tap_success(TAP_ID, TARGET_ID, ['fastsync'])
        assert self.run_target_query(
            f'SELECT COUNT(*) FROM {TARGET_SCHEMA}.{SOURCE_TABLE}'
        )[0][0] == source_count

        stdout = self._run_success(
            'pipelinewise run_data_diff_checks '
            f'--target {TARGET_ID} --tap {TAP_ID}'
        )
        assert 'PASS' in stdout

        statuses = self._latest_results()
        assert set(statuses) == EXPECTED_CHECKS
        failed = {name: status for name, status in statuses.items() if status != 'PASS'}
        assert not failed, f'MySQL to PostgreSQL comparison disagreed: {failed}'

    def test_dd_detects_a_postgres_target_that_diverged(self):
        """A row changed only in the target must fail the checksum, not the count."""
        self.e2e.setup_tap_mysql()
        self.e2e.setup_pipelinewise_backend()
        self.run_target_query(f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE')
        self.run_source_query(
            f'UPDATE {SOURCE_TABLE} SET date_updated = NOW() - INTERVAL 1 HOUR'
        )

        self._run_success(
            f'pipelinewise import_config --dir {PROJECT_DIR} --taps {TAP_ID}'
        )
        # FULL_TABLE is handled entirely by FastSync, so no singer log is produced.
        assertions.assert_run_tap_success(TAP_ID, TARGET_ID, ['fastsync'])

        # Diverge a compared column only, leaving row and key counts identical.
        self.run_target_query(
            f'UPDATE {TARGET_SCHEMA}.{SOURCE_TABLE} '
            "SET weight_unit_name = 'diverged' "
            f'WHERE weight_unit_id = ('
            f'SELECT MIN(weight_unit_id) FROM {TARGET_SCHEMA}.{SOURCE_TABLE})'
        )

        return_code, stdout, stderr = tasks.run_command(
            'pipelinewise run_data_diff_checks '
            f'--target {TARGET_ID} --tap {TAP_ID} --force'
        )
        assert return_code != 0, f'expected a mismatch exit\n{stdout}\n{stderr}'

        statuses = self._latest_results()
        # Only the checksum sees column values, so it alone detects this drift.
        assert statuses['row_checksum'] == 'FAIL'
        assert statuses['row_count'] == 'PASS'
        assert statuses['schema_compatibility'] == 'PASS'
