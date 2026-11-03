"""YugabyteDB-to-PostgreSQL data-diff E2E coverage.

YSQL shares PostgreSQL's aggregate SQL, so what differs is index eligibility: every
index is ``lsm``, a default key column is HASH-sharded, and the tap-yugabyte keyset
index leads with a bucket expression. Only a real cluster proves the catalog reading
and that the bucket predicate lets the planner range-scan that index.
"""

import json
import os
import uuid

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from pipelinewise.data_diff.engine import build_metric_query, connect_source, preflight_source

from ..helpers import assertions, tasks
from ..helpers.env import E2EEnv


DIR = os.path.dirname(__file__)
PROJECT_DIR = os.path.join(DIR, 'test-project')
TAP_ID = 'data_diff_yugabyte_to_pg'
TARGET_ID = 'data_diff_postgres_dwh'
TARGET_SCHEMA = 'ppw_e2e_data_diff_yugabyte'
SOURCE_TABLE = 'dd_yugabyte_transfers'
FULL_CHECK_NAME = f'{TARGET_ID}/{TAP_ID}/public/{SOURCE_TABLE}'
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


def _keyset_index(name, qualified_table, buckets=3):
    """The tap-yugabyte shape-4 index: bucket, timestamp, primary key."""
    splits = ', '.join(f'({value})' for value in range(1, buckets))
    return (
        f'CREATE UNIQUE INDEX "{name}" ON {qualified_table} '
        f'(((yb_hash_code(id) % {buckets})) ASC, updated_at ASC, id ASC) SPLIT AT VALUES ({splits})'
    )


class TestYugabyteToPostgresDataDiff:
    """Prove data-diff reads YugabyteDB indexes and reconciles a PostgreSQL replica."""

    def setup_method(self):
        """Render the isolated E2E project and initialize query helpers."""
        self.e2e = E2EEnv(PROJECT_DIR)
        self.run_source_query = self.e2e.run_query_tap_yugabyte
        self.run_target_query = self.e2e.run_query_target_postgres
        self.run_backend_query = self.e2e.run_query_pipelinewise_backend
        self.connection_config = {
            field: self.e2e.get_conn_env_var('TAP_YUGABYTE', environment_key)
            for field, environment_key in (
                ('host', 'HOST'), ('port', 'PORT'), ('user', 'USER'),
                ('password', 'PASSWORD'), ('dbname', 'DB'),
            )
        }

    def _preflight(self, table, sql):
        check = {'source_type': 'tap-yugabyte', 'statement_timeout_seconds': 60}
        adapter = connect_source(check, self.connection_config)
        try:
            count = Mock(wraps=adapter.count_rows_up_to)
            adapter.count_rows_up_to = count
            return preflight_source(adapter, 'public', table, 'updated_at', sql, ()), count
        finally:
            adapter.close()

    def test_preflight_reads_yugabyte_index_shapes(self):
        """Hash-sharded indexes block, and ordered or keyset-bucket indexes qualify."""
        table = f'dd_yb_preflight_{uuid.uuid4().hex[:12]}'
        qualified = f'public."{table}"'
        sql = f'SELECT COUNT(*) AS row_count FROM {qualified}'
        self.run_source_query(f'CREATE TABLE {qualified} (id BIGINT PRIMARY KEY, updated_at TIMESTAMP)')
        try:
            self.run_source_query(
                f'INSERT INTO {qualified} SELECT g, TIMESTAMP \'2026-01-01\' + g * INTERVAL \'37 seconds\' '
                'FROM generate_series(1, 150000) g'
            )
            preflight, count = self._preflight(table, sql)
            assert preflight['status'] == 'BLOCKED', preflight
            assert preflight['table_rows'] == 100_000
            count.assert_called_once()

            self.run_source_query(f'CREATE INDEX "{table}_hash" ON {qualified} (updated_at)')
            preflight, count = self._preflight(table, sql)
            assert preflight['status'] == 'BLOCKED', preflight
            assert f'ignored as unusable: {table}_hash' in preflight['findings'][0]

            self.run_source_query(_keyset_index(f'{table}_keyset', qualified))
            self.run_source_query(f'ANALYZE {qualified}')
            preflight, count = self._preflight(table, sql)
            assert preflight['status'] == 'PASS', preflight
            assert preflight['has_leading_index'] is True
            count.assert_not_called()
            keyset, = (index for index in preflight['index_metadata'] if index['index_name'] == f'{table}_keyset')
            assert keyset['bucket'] == {'columns': ['id'], 'modulus': 3}
            assert keyset['columns'] == ['updated_at', 'id']

            # Without the bucket predicate YugabyteDB seq-scans a window this wide.
            adapter = connect_source(
                {'source_type': 'tap-yugabyte', 'statement_timeout_seconds': 60}, self.connection_config
            )
            try:
                window_sql = build_metric_query(adapter, 'public', table, 'id', 'updated_at', ('row_count',))
                with adapter.connection.cursor() as cursor:
                    cursor.execute(
                        f'EXPLAIN (COSTS OFF) {window_sql}',
                        (datetime(2026, 1, 1), datetime(2026, 1, 21)),
                    )
                    plan = '\n'.join(row[0] for row in cursor.fetchall())
            finally:
                adapter.close()
            assert f'{table}_keyset' in plan, plan
            assert 'Seq Scan' not in plan, plan

            self.run_source_query(f'DROP INDEX public."{table}_keyset"')
            self.run_source_query(f'CREATE INDEX "{table}_ordered" ON {qualified} (updated_at DESC)')
            preflight, count = self._preflight(table, sql)
            assert preflight['status'] == 'PASS', preflight
            count.assert_not_called()
        finally:
            self.run_source_query(f'DROP TABLE IF EXISTS {qualified}')

    @staticmethod
    def _run_success(command):
        """Run one CLI command and return stdout after asserting success."""
        return_code, stdout, stderr = tasks.run_command(command)
        assertions.assert_command_success(return_code, stdout, stderr)
        return stdout

    @staticmethod
    def _run_json(command):
        """Run a CLI command expecting JSON output; strip leading log lines."""
        stdout = TestYugabyteToPostgresDataDiff._run_success(command)
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
                 WHERE checks.full_check_name = '{FULL_CHECK_NAME}'
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

    def _replicate_keyset_table(self):
        """Create a source past the index threshold, keyed only by the keyset index."""
        self.e2e.setup_pipelinewise_backend()
        qualified = f'public."{SOURCE_TABLE}"'
        self.run_source_query(f'DROP TABLE IF EXISTS {qualified}')
        self.run_target_query(f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE')
        self.run_source_query(
            f'CREATE TABLE {qualified} '
            '(id BIGINT PRIMARY KEY, updated_at TIMESTAMP WITH TIME ZONE, amount NUMERIC(12, 2), note TEXT)'
        )
        # Anchor rows inside the previous day so the window is non-empty at any start time.
        anchor = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=1)
        self.run_source_query(
            f'INSERT INTO {qualified} '
            'SELECT g, %s - (g %% 1200) * INTERVAL \'1 minute\', (g %% 100000) / 100.0, '
            "CASE WHEN g %% 7 = 0 THEN NULL ELSE 'note-' || g END "
            'FROM generate_series(1, 100001) g',
            (anchor,),
        )
        self.run_source_query(_keyset_index(f'{SOURCE_TABLE}_updated_at_pw_keyset', qualified))
        self.run_source_query(f'ANALYZE {qualified}')

        self._run_success(f'pipelinewise validate --dir {PROJECT_DIR}')
        self._run_success(f'pipelinewise import_config --dir {PROJECT_DIR} --taps {TAP_ID}')
        # FULL_TABLE is handled entirely by FastSync, so no singer log is produced.
        assertions.assert_run_tap_success(TAP_ID, TARGET_ID, ['fastsync'])
        assert self.run_target_query(f'SELECT COUNT(*) FROM {TARGET_SCHEMA}.{SOURCE_TABLE}')[0][0] == 100_001

    def test_dd_reconciles_yugabyte_source_against_postgres_target(self):
        """Every check passes through the keyset index on a table past the row limit."""
        try:
            self._replicate_keyset_table()
            checks = self._run_json(
                f'pipelinewise list_data_diff_checks --target {TARGET_ID} --tap {TAP_ID} --output-format json'
            )
            assert [check['full_check_name'] for check in checks] == [FULL_CHECK_NAME]
            assert set(checks[0]['checks']) == EXPECTED_CHECKS

            stdout = self._run_success(f'pipelinewise run_data_diff_checks --target {TARGET_ID} --tap {TAP_ID}')
            assert 'PASS' in stdout

            statuses = self._latest_results()
            assert set(statuses) == EXPECTED_CHECKS
            failed = {name: status for name, status in statuses.items() if status != 'PASS'}
            assert not failed, f'YugabyteDB to PostgreSQL comparison disagreed: {failed}'

            # A changed compared column alone leaves every count intact.
            self.run_target_query(
                f"UPDATE {TARGET_SCHEMA}.{SOURCE_TABLE} SET note = 'diverged' "
                f'WHERE id = (SELECT MIN(id) FROM {TARGET_SCHEMA}.{SOURCE_TABLE})'
            )
            return_code, stdout, stderr = tasks.run_command(
                f'pipelinewise run_data_diff_checks --target {TARGET_ID} --tap {TAP_ID} --force'
            )
            assert return_code != 0, f'expected a mismatch exit\n{stdout}\n{stderr}'
            statuses = self._latest_results()
            assert statuses['row_checksum'] == 'FAIL'
            assert statuses['row_count'] == 'PASS'
            assert statuses['schema_compatibility'] == 'PASS'
        finally:
            self.run_source_query(f'DROP TABLE IF EXISTS public."{SOURCE_TABLE}"')
