"""PostgreSQL-to-PostgreSQL data-diff lifecycle E2E coverage.

The same-dialect route, so it covers the lifecycle the others do not: a passing
check, a failure, remediation through ``rerun_data_diff_check``, and the coverage
watermark advancing. Also proves Alembic builds the schema on an empty backend as a
separate ``ddl_user``. Cross-dialect checksum agreement belongs to the other routes.
"""

import json
import os
import shutil

from datetime import timedelta
from pathlib import Path

from pipelinewise.data_diff.repository import DataDiffRepository
from pipelinewise.data_diff.runner import run_due_checks
from pipelinewise.data_diff.runtime import RuntimeConnectorConfigLoader

from ..helpers import assertions, tasks
from ..helpers.env import E2EEnv


DIR = os.path.dirname(__file__)
PROJECT_DIR = os.path.join(DIR, 'test-project')
TAP_ID = 'data_diff_postgres_to_pg'
TARGET_ID = 'data_diff_postgres_dwh'
FULL_CHECK_NAME = f'{TARGET_ID}/{TAP_ID}/logical1/logical1_table1'
TARGET_SCHEMA = 'ppw_e2e_data_diff'
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
class TestPostgresToPostgresDataDiff:
    """Exercise persisted checks, failures, remediation, and coverage."""

    def setup_method(self):
        """Render the isolated E2E project and initialize query helpers."""
        self.e2e = E2EEnv(PROJECT_DIR)
        self.run_source_query = self.e2e.run_query_tap_postgres
        self.run_target_query = self.e2e.run_query_target_postgres
        self.run_backend_query = self.e2e.run_query_pipelinewise_backend

    @staticmethod
    def _run_success(command):
        """Run one CLI command and return stdout after asserting success."""
        return_code, stdout, stderr = tasks.run_command(command)
        assertions.assert_command_success(return_code, stdout, stderr)
        return stdout

    @staticmethod
    def _run_json(command):
        """Run a CLI command expecting JSON output; strip leading log lines."""
        stdout = TestPostgresToPostgresDataDiff._run_success(command)
        # PipelineWise logs INFO lines to stdout before the JSON payload.
        # Find the first '[' or '{' that starts the actual JSON.
        for index, char in enumerate(stdout):
            if char in ('[', '{'):
                return json.loads(stdout[index:])
        raise ValueError(f'No JSON found in command output: {stdout[:200]}')

    # pylint: disable=too-many-locals,too-many-statements
    def test_dd_pass_failure_and_remediation_lifecycle(self):
        """Prove source-target comparison and immutable remediation evidence."""
        self.e2e.setup_tap_postgres()
        self.e2e.setup_pipelinewise_backend()
        self.run_target_query(f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE')
        shutil.rmtree(Path.home() / '.pipelinewise' / TARGET_ID, ignore_errors=True)

        # Anchor every fixture row inside the previous completed UTC hour. This
        # makes the comparison non-empty without depending on test start time.
        self.run_source_query(
            "UPDATE logical1.logical1_table1 "
            "SET updated_at = date_trunc('hour', CURRENT_TIMESTAMP) - interval '1 hour'"
        )
        source_count = self.run_source_query(
            'SELECT COUNT(*) FROM logical1.logical1_table1'
        )[0][0]
        assert source_count == 4

        self._run_success(f'pipelinewise validate --dir {PROJECT_DIR}')
        self._run_success(
            f'pipelinewise import_config --dir {PROJECT_DIR} --taps {TAP_ID}'
        )

        checks = self._run_json(
            'pipelinewise list_data_diff_checks '
            f'--target {TARGET_ID} --tap {TAP_ID} --output-format json'
        )
        assert len(checks) == 1
        definition = checks[0]
        assert definition['full_check_name'] == FULL_CHECK_NAME
        assert definition['revision'] == 1
        assert definition['current']
        assert definition['target_type'] == 'target-postgres'
        assert definition['frequency'] == '0 * * * *'
        assert definition['window_start_seconds'] == 86400
        assert set(definition['checks']) == EXPECTED_CHECKS
        assert 'password' not in json.dumps(definition['canonical_config']).lower()

        backend_database = self.run_backend_query(
            'SELECT current_database()'
        )[0][0]
        target_database = self.run_target_query(
            'SELECT current_database()'
        )[0][0]
        assert backend_database != target_database
        assert self.run_backend_query(
            "SELECT COUNT(*) FROM information_schema.schemata "
            f"WHERE schema_name = '{TARGET_SCHEMA}'"
        )[0][0] == 0

        assertions.assert_run_tap_success(
            TAP_ID, TARGET_ID, ['fastsync', 'singer']
        )
        target_count = self.run_target_query(
            f'SELECT COUNT(*) FROM {TARGET_SCHEMA}.logical1_table1'
        )[0][0]
        assert target_count == source_count

        run_command = (
            'pipelinewise run_data_diff_checks '
            f'--target {TARGET_ID} --tap {TAP_ID} --check {FULL_CHECK_NAME}'
        )
        pass_stdout = self._run_success(run_command)
        assert 'PASS' in pass_stdout

        pass_run = self.run_backend_query(
            """
            SELECT runs.run_id::text, runs.dd_check_id::text,
                   runs.scheduled_for, runs.window_start, runs.window_end,
                   runs.status, runs.attempt, runs.trigger, preflights.status
              FROM public.dd_runs runs
              JOIN public.dd_preflights preflights
                ON preflights.preflight_id = runs.preflight_id
             ORDER BY runs.started_at DESC
             LIMIT 1
            """
        )[0]
        (
            pass_run_id,
            check_id,
            pass_scheduled_for,
            pass_window_start,
            pass_window_end,
            pass_status,
            pass_attempt,
            pass_trigger,
            pass_preflight_status,
        ) = pass_run
        assert pass_status == 'PASS'
        assert pass_attempt == 1
        assert pass_trigger == 'SCHEDULED'
        assert pass_preflight_status == 'PASS'
        assert pass_scheduled_for == pass_window_end
        assert pass_window_end - pass_window_start == timedelta(days=1)
        assert pass_window_start.utcoffset() == timedelta(0)

        pass_results = self.run_backend_query(
            f"""
            SELECT check_type, status, source_value, target_value
              FROM public.dd_results
             WHERE run_id = '{pass_run_id}'
             ORDER BY check_type
            """
        )
        assert {result[0] for result in pass_results} == EXPECTED_CHECKS
        assert {result[1] for result in pass_results} == {'PASS'}
        row_count_result = next(
            result for result in pass_results if result[0] == 'row_count'
        )
        assert row_count_result[2:] == ('4', '4')

        initial_coverage = self.run_backend_query(
            f"""
            SELECT coverage_status, blocking_run_id::text,
                   evaluated_run_id::text, verified_through
              FROM public.dd_coverage_state
             WHERE check_id = '{check_id}'
            """
        )[0]
        assert initial_coverage == (
            'CONTIGUOUS',
            None,
            pass_run_id,
            pass_window_end,
        )
        assert self.run_backend_query(
            f"""
            SELECT run_id::text, attempt, status
              FROM public.dd_effective_attempts
             WHERE check_id = '{check_id}'
            """
        ) == [(pass_run_id, 1, 'PASS')]
        assert self.run_backend_query(
            f"""
            SELECT state_version, evaluated_run_id::text
              FROM public.dd_coverage_state
             WHERE check_id = '{check_id}'
            """
        ) == [(1, pass_run_id)]

        source_value = self.run_source_query(
            'SELECT cvarchar FROM logical1.logical1_table1 WHERE cid = 1'
        )[0][0]
        assert source_value == 'inserted row'
        self.run_target_query(
            f"UPDATE {TARGET_SCHEMA}.logical1_table1 "
            "SET cvarchar = 'intentional data-diff mismatch' WHERE cid = 1"
        )

        fail_code, fail_stdout, fail_stderr = tasks.run_command(
            f'{run_command} --force'
        )
        self.run_target_query(
            f"UPDATE {TARGET_SCHEMA}.logical1_table1 "
            "SET cvarchar = 'inserted row' WHERE cid = 1"
        )
        assert fail_code == 1
        assert 'Traceback' not in fail_stderr
        assert 'FAIL' in fail_stdout

        failed_run = self.run_backend_query(
            f"""
            SELECT run_id::text, scheduled_for, window_start, window_end,
                   attempt, status, trigger
              FROM public.dd_runs
             WHERE dd_check_id = '{check_id}'
               AND status = 'FAIL'
             ORDER BY started_at DESC
             LIMIT 1
            """
        )[0]
        (
            failed_run_id,
            failed_scheduled_for,
            failed_window_start,
            failed_window_end,
            failed_attempt,
            failed_status,
            failed_trigger,
        ) = failed_run
        assert failed_status == 'FAIL'
        assert failed_trigger == 'MANUAL'

        failed_results = self.run_backend_query(
            f"""
            SELECT check_type, status
              FROM public.dd_results
             WHERE run_id = '{failed_run_id}'
             ORDER BY check_type
            """
        )
        assert {result[0] for result in failed_results} == EXPECTED_CHECKS
        assert dict(failed_results)['row_checksum'] == 'FAIL'
        assert {
            status for check_type, status in failed_results
            if check_type != 'row_checksum'
        } == {'PASS'}
        assert self.run_backend_query(
            f"SELECT status FROM public.dd_runs WHERE run_id = '{pass_run_id}'"
        )[0][0] == 'PASS'

        blocked_coverage = self.run_backend_query(
            f"""
            SELECT coverage_status, blocking_run_id::text, verified_through
              FROM public.dd_coverage_state
             WHERE check_id = '{check_id}'
            """
        )[0]
        assert blocked_coverage == (
            'BLOCKED',
            failed_run_id,
            failed_window_start,
        )
        assert self.run_backend_query(
            f"""
            SELECT run_id::text, attempt, status
              FROM public.dd_effective_attempts
             WHERE check_id = '{check_id}'
            """
        ) == [(failed_run_id, failed_attempt, 'FAIL')]
        assert self.run_backend_query(
            f"""
            SELECT state_version, evaluated_run_id::text
              FROM public.dd_coverage_state
             WHERE check_id = '{check_id}'
            """
        ) == [(2, failed_run_id)]

        remediation_stdout = self._run_success(
            'pipelinewise rerun_data_diff_check '
            f'--run-id {failed_run_id} --remediation-ref E2E-DATA-FIX'
        )
        assert 'PASS' in remediation_stdout

        remediation = self.run_backend_query(
            f"""
            SELECT run_id::text, dd_check_id::text, scheduled_for,
                   window_start, window_end, attempt, status, trigger,
                   rerun_of_run_id::text, remediation_reference
              FROM public.dd_runs
             WHERE rerun_of_run_id = '{failed_run_id}'
            """
        )[0]
        (
            remediation_run_id,
            remediation_check_id,
            remediation_scheduled_for,
            remediation_window_start,
            remediation_window_end,
            remediation_attempt,
            remediation_status,
            remediation_trigger,
            rerun_of_run_id,
            remediation_reference,
        ) = remediation
        assert remediation_check_id == check_id
        assert remediation_scheduled_for == failed_scheduled_for
        assert remediation_window_start == failed_window_start
        assert remediation_window_end == failed_window_end
        assert remediation_attempt == failed_attempt + 1
        assert remediation_status == 'PASS'
        assert remediation_trigger == 'REMEDIATION'
        assert rerun_of_run_id == failed_run_id
        assert remediation_reference == 'E2E-DATA-FIX'

        remediation_results = self.run_backend_query(
            f"""
            SELECT check_type, status
              FROM public.dd_results
             WHERE run_id = '{remediation_run_id}'
             ORDER BY check_type
            """
        )
        assert {result[0] for result in remediation_results} == EXPECTED_CHECKS
        assert {result[1] for result in remediation_results} == {'PASS'}
        assert self.run_backend_query(
            f"""
            SELECT status
              FROM public.dd_results
             WHERE run_id = '{failed_run_id}'
               AND check_type = 'row_checksum'
            """
        )[0][0] == 'FAIL'
        assert self.run_backend_query(
            f"SELECT status FROM public.dd_runs WHERE run_id = '{failed_run_id}'"
        )[0][0] == 'FAIL'
        assert self.run_backend_query(
            f"""
            SELECT COALESCE(remediation.status = 'PASS', FALSE) AS recovered
              FROM public.dd_runs original
              LEFT JOIN public.dd_runs remediation
                ON remediation.rerun_of_run_id = original.run_id
             WHERE original.run_id = '{failed_run_id}'
               AND remediation.run_id = '{remediation_run_id}'
            """
        )[0][0]
        assert self.run_backend_query(
            f"""
            SELECT event_type
              FROM public.dd_coverage_events
             WHERE check_id = '{check_id}'
             ORDER BY event_sequence
            """
        ) == [('INITIALIZE',), ('INVALIDATE',), ('ADVANCE',)]

        final_coverage = self.run_backend_query(
            f"""
            SELECT coverage_status, blocking_run_id::text,
                   evaluated_run_id::text, verified_through
              FROM public.dd_coverage_state
             WHERE check_id = '{check_id}'
            """
        )[0]
        assert final_coverage == (
            'CONTIGUOUS',
            None,
            remediation_run_id,
            remediation_window_end,
        )
        assert self.run_backend_query(
            f"""
            SELECT run_id::text, attempt, status
              FROM public.dd_effective_attempts
             WHERE check_id = '{check_id}'
            """
        ) == [(remediation_run_id, remediation_attempt, 'PASS')]
        assert self.run_backend_query(
            f"""
            SELECT state_version, evaluated_run_id::text
              FROM public.dd_coverage_state
             WHERE check_id = '{check_id}'
            """
        ) == [(3, remediation_run_id)]
        assert self.run_backend_query(
            'SELECT COUNT(*) FROM public.dd_runs'
        )[0][0] == 3

        backend_config = {
            'host': self.e2e.get_conn_env_var('PIPELINEWISE_BACKEND', 'HOST'),
            'port': int(self.e2e.get_conn_env_var('PIPELINEWISE_BACKEND', 'PORT')),
            'user': self.e2e.get_conn_env_var('PIPELINEWISE_BACKEND', 'USER'),
            'password': self.e2e.get_conn_env_var('PIPELINEWISE_BACKEND', 'PASSWORD'),
            'dbname': self.e2e.get_conn_env_var('PIPELINEWISE_BACKEND', 'DB'),
            'ddl_user': self.e2e.get_conn_env_var('PIPELINEWISE_BACKEND', 'DDL_USER'),
            'ddl_password': self.e2e.get_conn_env_var(
                'PIPELINEWISE_BACKEND', 'DDL_PASSWORD'
            ),
        }
        with DataDiffRepository.from_backend_config(backend_config) as repository:
            appended = run_due_checks(
                repository,
                RuntimeConnectorConfigLoader(Path.home() / '.pipelinewise'),
                now=remediation_scheduled_for + timedelta(hours=1, minutes=1),
                target_id=TARGET_ID,
                tap_id=TAP_ID,
                check_filter=FULL_CHECK_NAME,
            )

        assert len(appended) == 1
        assert appended[0]['status'] == 'PASS'
        assert appended[0]['scheduled_for'] == (
            remediation_scheduled_for + timedelta(hours=1)
        )
        appended_run_id = str(appended[0]['run_id'])
        assert self.run_backend_query(
            f"""
            SELECT run_id::text, attempt, status
              FROM public.dd_effective_attempts
             WHERE check_id = '{check_id}'
             ORDER BY scheduled_for
            """
        ) == [
            (remediation_run_id, remediation_attempt, 'PASS'),
            (appended_run_id, 1, 'PASS'),
        ]
        assert self.run_backend_query(
            f"""
            SELECT state_version, evaluated_run_id::text,
                   verified_through, event_type
              FROM public.dd_coverage_state
             WHERE check_id = '{check_id}'
            """
        ) == [(
            4,
            appended_run_id,
            appended[0]['window_end'],
            'ADVANCE',
        )]
        assert self.run_backend_query(
            'SELECT COUNT(*) FROM public.dd_runs'
        )[0][0] == 4

    def test_migrations_build_the_schema_on_an_empty_backend(self):
        """Prove Alembic pins its objects and version table to public."""
        # setup_pipelinewise_backend drops the tables and alembic_version, so the
        # next import_config has to migrate from nothing.
        self.e2e.setup_pipelinewise_backend()
        ddl_user = self.e2e.get_conn_env_var('PIPELINEWISE_BACKEND', 'DDL_USER')
        escaped_ddl_user = ddl_user.replace('"', '""')
        ddl_schema = f'"{escaped_ddl_user}"'
        self.e2e.run_ddl_pipelinewise_backend(
            f'DROP SCHEMA IF EXISTS {ddl_schema} CASCADE; '
            f'CREATE SCHEMA {ddl_schema} AUTHORIZATION {ddl_schema}'
        )

        try:
            # PostgreSQL's default "$user", public search path now resolves to the
            # DDL-role schema first. Alembic must still keep versioning in public.
            assert self.e2e.run_ddl_pipelinewise_backend(
                'SELECT current_schema()'
            )[0][0] == ddl_user
            assert self.run_backend_query(
                "SELECT COUNT(*) FROM information_schema.tables"
                " WHERE table_schema = 'public' AND table_name LIKE 'dd_%'"
            )[0][0] == 0

            self._run_success(
                f'pipelinewise import_config --dir {PROJECT_DIR} --taps {TAP_ID}'
            )

            tables = {
                row[0] for row in self.run_backend_query(
                    "SELECT table_name FROM information_schema.tables"
                    " WHERE table_schema = 'public' AND table_name LIKE 'dd_%'"
                )
            }
            assert {
                'dd_checks', 'dd_preflights', 'dd_runs', 'dd_results',
                'dd_effective_attempts', 'dd_coverage_state',
                'dd_coverage_events',
            } <= tables
            assert {
                'dd_current_coverage', 'dd_remediation_history',
            }.isdisjoint(tables)

            assert self.e2e.run_ddl_pipelinewise_backend(
                "SELECT to_regclass(quote_ident(current_user) || '.alembic_version')"
            )[0][0] is None
            # Alembic stamped its version, so a second import is a no-op migration.
            assert self.run_backend_query(
                'SELECT COUNT(*) FROM public.alembic_version'
            )[0][0] == 1
            self._run_success(
                f'pipelinewise import_config --dir {PROJECT_DIR} --taps {TAP_ID}'
            )
        finally:
            self.e2e.run_ddl_pipelinewise_backend(
                f'DROP SCHEMA IF EXISTS {ddl_schema} CASCADE'
            )
