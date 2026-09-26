"""PostgreSQL-to-PostgreSQL data-diff lifecycle E2E coverage.

The same-dialect route, so it covers the lifecycle the others do not: a passing
check, a failure, remediation through ``rerun_data_diff_check``, and the coverage
watermark advancing. Also proves Alembic builds the schema on an empty backend as a
separate ``ddl_user``. Cross-dialect checksum agreement belongs to the other routes.
"""

import json
import os
import shutil

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError

from pipelinewise.data_diff.config import CheckDefinition
from pipelinewise.data_diff.engine import HistoricalWindowNotReady
from pipelinewise.data_diff.repository import DataDiffRepository, RunLeaseLostError
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


def _repository_definition(tap_id, source_table, *, frequency='0 * * * *'):
    return CheckDefinition(
        full_check_name=f'{TARGET_ID}/{tap_id}/logical1/{source_table}',
        target_id=TARGET_ID,
        tap_id=tap_id,
        source_type='tap-postgres',
        target_type='target-postgres',
        source_database='postgres_source_db',
        target_database='postgres_dwh',
        source_schema='logical1',
        source_table=source_table,
        target_schema=TARGET_SCHEMA,
        target_table=source_table,
        source_key_column='cid',
        target_key_column='cid',
        source_timestamp_column='updated_at',
        target_timestamp_column='updated_at',
        source_compare_columns=(),
        target_compare_columns=(),
        checks=('row_count',),
        frequency=frequency,
        window_start_seconds=3600,
        window_end_seconds=0,
        statement_timeout_seconds=60,
    )


class TestPostgresToPostgresDataDiff:
    """Exercise persisted checks, failures, remediation, and coverage."""

    def setup_method(self):
        """Render the isolated E2E project and initialize query helpers."""
        self.e2e = E2EEnv(PROJECT_DIR)
        self.run_source_query = self.e2e.run_query_tap_postgres
        self.run_target_query = self.e2e.run_query_target_postgres
        self.run_backend_query = self.e2e.run_query_pipelinewise_backend

    def _backend_config(self):
        return {
            'host': self.e2e.get_conn_env_var('PIPELINEWISE_BACKEND', 'HOST'),
            'port': int(
                self.e2e.get_conn_env_var('PIPELINEWISE_BACKEND', 'PORT')
            ),
            'user': self.e2e.get_conn_env_var('PIPELINEWISE_BACKEND', 'USER'),
            'password': self.e2e.get_conn_env_var(
                'PIPELINEWISE_BACKEND', 'PASSWORD'
            ),
            'dbname': self.e2e.get_conn_env_var('PIPELINEWISE_BACKEND', 'DB'),
            'ddl_user': self.e2e.get_conn_env_var(
                'PIPELINEWISE_BACKEND', 'DDL_USER'
            ),
            'ddl_password': self.e2e.get_conn_env_var(
                'PIPELINEWISE_BACKEND', 'DDL_PASSWORD'
            ),
        }

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

    def test_dd_pass_failure_and_remediation_lifecycle(self):
        """Prove source-target comparison and immutable remediation evidence."""
        self.e2e.setup_tap_postgres()
        self.e2e.setup_pipelinewise_backend()
        self.run_target_query(f'DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE')
        shutil.rmtree(Path.home() / '.pipelinewise' / TARGET_ID, ignore_errors=True)

        # Keep three rows in the previous completed UTC hour and one seven days
        # old, outside the configured one-day rolling window.
        self.run_source_query(
            "UPDATE logical1.logical1_table1 "
            "SET updated_at = date_trunc('hour', CURRENT_TIMESTAMP) - interval '1 hour'"
        )
        self.run_source_query(
            "UPDATE logical1.logical1_table1 "
            "SET updated_at = date_trunc('hour', CURRENT_TIMESTAMP) - interval '7 days' "
            "WHERE cid = 1"
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
        assert definition['is_current']
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
            SELECT runs.run_id::text, runs.check_id::text,
                   runs.scheduled_for, runs.window_start, runs.window_end,
                   runs.status, runs.attempt, runs.trigger_type, preflights.status
              FROM public.dd_run_attempts runs
              JOIN public.dd_preflight_log preflights
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
        assert pass_window_start == self.run_source_query(
            'SELECT MIN(updated_at) FROM logical1.logical1_table1'
        )[0][0]
        assert pass_window_start.utcoffset() == timedelta(0)

        historical_cutoff = (pass_scheduled_for - timedelta(days=1)).isoformat()
        assert self.run_source_query(
            "SELECT COUNT(*) FROM logical1.logical1_table1 "
            f"WHERE cid = 1 AND updated_at < '{historical_cutoff}'"
        )[0][0] == 1
        assert self.run_target_query(
            f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.logical1_table1 "
            f"WHERE cid = 1 AND updated_at < '{historical_cutoff}'"
        )[0][0] == 1

        pass_results = self.run_backend_query(
            f"""
            SELECT check_type, status, source_value, target_value
              FROM public.dd_run_results
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
            SELECT verified_status, blocking_run_id::text,
                   last_evaluated_run_id::text, verified_start, verified_end,
                   furthest_observed_end
              FROM public.dd_watermark_state
             WHERE check_id = '{check_id}'
            """
        )[0]
        assert initial_coverage == (
            'CONTIGUOUS',
            None,
            pass_run_id,
            pass_window_start,
            pass_window_end,
            pass_window_end,
        )
        assert self.run_backend_query(
            f"""
            SELECT run_id::text, attempt, status
              FROM public.dd_run_slot_state
             WHERE check_id = '{check_id}'
            """
        ) == [(pass_run_id, 1, 'PASS')]
        assert self.run_backend_query(
            f"""
            SELECT state_version, last_evaluated_run_id::text
              FROM public.dd_watermark_state
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
                   attempt, status, trigger_type
              FROM public.dd_run_attempts
             WHERE check_id = '{check_id}'
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
              FROM public.dd_run_results
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
            f"SELECT status FROM public.dd_run_attempts WHERE run_id = '{pass_run_id}'"
        )[0][0] == 'PASS'

        blocked_coverage = self.run_backend_query(
            f"""
            SELECT verified_status, blocking_run_id::text, verified_end,
                   furthest_observed_end
              FROM public.dd_watermark_state
             WHERE check_id = '{check_id}'
            """
        )[0]
        assert blocked_coverage == (
            'BLOCKED',
            failed_run_id,
            failed_window_start,
            failed_window_end,
        )
        assert self.run_backend_query(
            f"""
            SELECT run_id::text, attempt, status
              FROM public.dd_run_slot_state
             WHERE check_id = '{check_id}'
            """
        ) == [(failed_run_id, failed_attempt, 'FAIL')]
        assert self.run_backend_query(
            f"""
            SELECT state_version, last_evaluated_run_id::text
              FROM public.dd_watermark_state
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
            SELECT run_id::text, check_id::text, scheduled_for,
                   window_start, window_end, attempt, status, trigger_type,
                   rerun_of_run_id::text, remediation_reference
              FROM public.dd_run_attempts
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
              FROM public.dd_run_results
             WHERE run_id = '{remediation_run_id}'
             ORDER BY check_type
            """
        )
        assert {result[0] for result in remediation_results} == EXPECTED_CHECKS
        assert {result[1] for result in remediation_results} == {'PASS'}
        assert self.run_backend_query(
            f"""
            SELECT status
              FROM public.dd_run_results
             WHERE run_id = '{failed_run_id}'
               AND check_type = 'row_checksum'
            """
        )[0][0] == 'FAIL'
        assert self.run_backend_query(
            f"SELECT status FROM public.dd_run_attempts WHERE run_id = '{failed_run_id}'"
        )[0][0] == 'FAIL'
        assert self.run_backend_query(
            f"""
            SELECT COALESCE(remediation.status = 'PASS', FALSE) AS recovered
              FROM public.dd_run_attempts original
              LEFT JOIN public.dd_run_attempts remediation
                ON remediation.rerun_of_run_id = original.run_id
             WHERE original.run_id = '{failed_run_id}'
               AND remediation.run_id = '{remediation_run_id}'
            """
        )[0][0]
        assert self.run_backend_query(
            f"""
            SELECT event_type
              FROM public.dd_watermark_events
             WHERE check_id = '{check_id}'
             ORDER BY event_sequence
            """
        ) == [('INITIALIZE',), ('INVALIDATE',), ('ADVANCE',)]

        final_coverage = self.run_backend_query(
            f"""
            SELECT verified_status, blocking_run_id::text,
                   last_evaluated_run_id::text, verified_end
              FROM public.dd_watermark_state
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
              FROM public.dd_run_slot_state
             WHERE check_id = '{check_id}'
            """
        ) == [(remediation_run_id, remediation_attempt, 'PASS')]
        assert self.run_backend_query(
            f"""
            SELECT state_version, last_evaluated_run_id::text
              FROM public.dd_watermark_state
             WHERE check_id = '{check_id}'
            """
        ) == [(3, remediation_run_id)]
        assert self.run_backend_query(
            'SELECT COUNT(*) FROM public.dd_run_attempts'
        )[0][0] == 3

        with DataDiffRepository.from_backend_config(
            self._backend_config()
        ) as repository:
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
              FROM public.dd_run_slot_state
             WHERE check_id = '{check_id}'
             ORDER BY scheduled_for
            """
        ) == [
            (remediation_run_id, remediation_attempt, 'PASS'),
            (appended_run_id, 1, 'PASS'),
        ]
        assert self.run_backend_query(
            f"""
            SELECT state_version, last_evaluated_run_id::text,
                   verified_end, event_type
              FROM public.dd_watermark_state
             WHERE check_id = '{check_id}'
            """
        ) == [(
            4,
            appended_run_id,
            appended[0]['window_end'],
            'ADVANCE',
        )]
        assert self.run_backend_query(
            'SELECT COUNT(*) FROM public.dd_run_attempts'
        )[0][0] == 4

        bounded_definition = replace(
            _repository_definition(TAP_ID, 'logical1_table1'),
            checks=tuple(definition['checks']),
            source_compare_columns=('cvarchar',),
            target_compare_columns=('cvarchar',),
            window_start_seconds=86400,
            initial_full_scan=False,
        )
        assert bounded_definition.canonical_config == {
            **definition['canonical_config'],
            'initial_full_scan': False,
        }
        with DataDiffRepository.from_backend_config(
            self._backend_config()
        ) as repository:
            assert repository.sync_definitions(
                [bounded_definition], selected_taps=[TAP_ID]
            ) == {
                'created': 1,
                'unchanged': 0,
                'superseded': 1,
                'deactivated': 0,
                'historical_scans_pending': 0,
            }
            bounded_check, = repository.list_checks(target_id=TARGET_ID, tap_id=TAP_ID)
            assert bounded_check['revision'] == 2
            assert str(bounded_check['check_id']) != check_id
            assert bounded_check['initial_full_scan'] is False
            assert repository.latest_scheduled_for(bounded_check['check_id']) is None
            bounded_summary, = run_due_checks(
                repository,
                RuntimeConnectorConfigLoader(Path.home() / '.pipelinewise'),
                now=pass_scheduled_for + timedelta(minutes=1),
                target_id=TARGET_ID,
                tap_id=TAP_ID,
                check_filter=FULL_CHECK_NAME,
            )

        assert bounded_summary['status'] == 'PASS'
        assert bounded_summary['window_start'] == pass_scheduled_for - timedelta(days=1)
        assert bounded_summary['window_end'] == pass_scheduled_for
        assert self.run_backend_query(
            f"""
            SELECT source_value, target_value
              FROM public.dd_run_results
             WHERE run_id = '{bounded_summary['run_id']}'
               AND check_type = 'row_count'
            """
        ) == [('3', '3')]

    def test_definition_sync_preserves_excluded_tap_in_postgres(self):
        """Prove exclusion and deactivation against the real backend schema."""
        self.e2e.setup_pipelinewise_backend()
        failed = _repository_definition('failed', 'failed_table')
        deleted = _repository_definition('deleted', 'deleted_table')
        successful = _repository_definition('successful', 'successful_table')
        changed_failed = replace(failed, frequency='30 * * * *')

        with DataDiffRepository.from_backend_config(
            self._backend_config()
        ) as repository:
            assert repository.sync_definitions([failed, deleted]) == {
                'created': 2,
                'unchanged': 0,
                'superseded': 0,
                'deactivated': 0,
                'historical_scans_pending': 2,
            }
            stats = repository.sync_definitions(
                [changed_failed, successful],
                selected_taps=['*'],
                excluded_taps=['failed'],
            )
            inventory = {check['tap_id']: check for check in repository.list_checks(include_versioned=True)}
            assert inventory['deleted']['historical_scan_pending'] is False
            assert inventory['failed']['historical_scan_pending'] is True
            assert inventory['successful']['historical_scan_pending'] is True

        assert stats == {
            'created': 1,
            'unchanged': 0,
            'superseded': 0,
            'deactivated': 1,
            'historical_scans_pending': 1,
        }
        assert self.run_backend_query(
            """
            SELECT tap_id, revision, config_hash, is_current,
                   superseded_at IS NOT NULL
              FROM public.dd_check_definitions
             ORDER BY tap_id, revision
            """
        ) == [
            ('deleted', 1, deleted.config_hash, False, True),
            ('failed', 1, failed.config_hash, True, False),
            ('successful', 1, successful.config_hash, True, False),
        ]

    def test_rolling_failure_before_historical_start_keeps_valid_watermark(self):
        """Persist a rolling failure whose lower bound predates a recent table."""
        self.e2e.setup_pipelinewise_backend()
        definition = replace(
            _repository_definition('historical_coverage', 'recent_table'),
            window_start_seconds=86400,
        )
        slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]
        historical_start = slot - timedelta(minutes=30)
        next_slot = slot + timedelta(hours=1)
        rolling_start = next_slot - timedelta(days=1)

        with DataDiffRepository.from_backend_config(self._backend_config()) as repository:
            repository.sync_definitions([definition], selected_taps=[definition.tap_id])
            check, = repository.list_checks(tap_id=definition.tap_id)
            baseline = repository.start_run(check, slot, slot - timedelta(days=1), slot)
            repository.set_run_window_start(baseline['run_id'], historical_start)
            repository.finish_run(
                baseline['run_id'], 'PASS', [{'check_type': 'row_count', 'status': 'PASS'}],
            )
            rolling = repository.start_run(check, next_slot, rolling_start, next_slot)
            repository.finish_run(
                rolling['run_id'], 'FAIL', [{'check_type': 'row_count', 'status': 'FAIL'}],
            )
            coverage = repository.get_check_version(check['check_id'])
            assert repository.get_run(rolling['run_id'])['status'] == 'FAIL'

        assert coverage['verified_start'] == coverage['verified_end'] == rolling_start
        assert coverage['furthest_observed_end'] == next_slot
        assert coverage['verified_status'] == 'BLOCKED'
        assert str(coverage['blocking_run_id']) == str(rolling['run_id'])

    @pytest.mark.parametrize('swept', [False, True])
    def test_unresolved_historical_error_blocks_coverage_until_remediation(self, swept):
        """Keep unknown bounds visible through failure, later passes and recovery."""
        self.e2e.setup_pipelinewise_backend()
        definition = _repository_definition('unresolved_history', 'history_table')
        slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]
        with DataDiffRepository.from_backend_config(self._backend_config()) as repository:
            repository.sync_definitions([definition], selected_taps=[definition.tap_id])
            check, = repository.list_checks(tap_id=definition.tap_id)
            baseline = repository.start_run(check, slot, slot - timedelta(hours=1), slot)
            assert baseline['window_start'] is None
            if swept:
                assert repository.expire_stale_running_attempts(check['check_id'], slot + timedelta(hours=1)) == 1
            else:
                repository.finish_run(baseline['run_id'], 'ERROR', [], error='Source unavailable before MIN')
            original = repository.get_run(baseline['run_id'])
            assert original['window_start'] is None
            assert original['status'] == 'ERROR'
            next_slot = slot + timedelta(hours=1)
            rolling = repository.start_run(check, next_slot, slot, next_slot)
            repository.finish_run(rolling['run_id'], 'PASS', [{'check_type': 'row_count', 'status': 'PASS'}])
            coverage = repository.get_check_version(check['check_id'])
            assert coverage['verified_start'] is None
            assert coverage['verified_end'] is None
            assert coverage['verified_status'] == 'BLOCKED'
            assert str(coverage['blocking_run_id']) == str(baseline['run_id'])
            remediation = repository.start_remediation_run(original, 'test historical recovery')
            start = slot - timedelta(days=7)
            repository.set_run_window_start(remediation['run_id'], start)
            repository.finish_run(
                remediation['run_id'], 'PASS', [{'check_type': 'row_count', 'status': 'PASS'}],
            )
            coverage = repository.get_check_version(check['check_id'])
            assert coverage['verified_start'] == start
            assert coverage['verified_end'] == next_slot
            assert coverage['verified_status'] == 'CONTIGUOUS'
            assert coverage['blocking_run_id'] is None
        assert self.run_backend_query(
            "SELECT verified_start, verified_end FROM public.dd_watermark_events "
            f"WHERE evaluated_run_id = '{baseline['run_id']}'"
        ) == [(None, None)]

    def test_deferred_historical_scan_waits_for_the_next_slot_without_claiming_coverage(self):
        """A no-data deferral keeps first-run discovery pending without an error."""
        self.e2e.setup_pipelinewise_backend()
        definition = _repository_definition('deferred_history', 'empty_table')
        slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]
        with DataDiffRepository.from_backend_config(self._backend_config()) as repository:
            repository.sync_definitions([definition], selected_taps=[definition.tap_id])
            check, = repository.list_checks(tap_id=definition.tap_id)
            first = repository.start_run(check, slot, slot - timedelta(hours=1), slot)
            repository.finish_run(first['run_id'], 'DEFERRED', [], error='Neither side has settled history')
            skipped = repository.start_run(check, slot, slot - timedelta(hours=1), slot)
            assert skipped['status'] == 'SKIPPED'
            assert skipped['slot_status'] == 'DEFERRED'
            assert skipped['window_start'] is None
            assert skipped['window_end'] == slot
            assert skipped['error']
            assert repository.get_run(first['run_id'])['status'] == 'DEFERRED'
            assert repository.get_check_version(check['check_id'])['verified_status'] is None
            next_slot = slot + timedelta(hours=1)
            next_run = repository.start_run(check, next_slot, slot, next_slot)
            assert next_run['window_start'] is None
            repository.set_run_window_start(next_run['run_id'], slot - timedelta(days=7))
            repository.finish_run(next_run['run_id'], 'PASS', [{'check_type': 'row_count', 'status': 'PASS'}])
            assert repository.get_check_version(check['check_id'])['verified_status'] == 'CONTIGUOUS'
        assert self.run_backend_query(
            f"SELECT COUNT(*) FROM public.dd_run_slot_state WHERE run_id = '{first['run_id']}'"
        ) == [(0,)]

    @staticmethod
    def _run_evidence(repository, run_id, check_id):
        """Snapshot one check's persisted results and watermark evidence."""
        evidence = {}
        with repository.cursor() as cursor:
            for table, column, value in (
                ('dd_run_attempts', 'run_id', run_id),
                ('dd_run_results', 'run_id', run_id),
                ('dd_run_slot_state', 'check_id', check_id),
                ('dd_watermark_state', 'check_id', check_id),
                ('dd_watermark_events', 'check_id', check_id),
            ):
                cursor.execute(f'SELECT * FROM public.{table} WHERE {column} = %s', (value,))
                evidence[table] = [dict(row) for row in cursor.fetchall()]
        return evidence

    @pytest.mark.parametrize('late_action', ['PASS', 'DEFERRED', 'resolve_start'])
    def test_expired_worker_cannot_change_results_or_watermark(self, late_action):
        """A worker that lost its lease cannot change the sweep's terminal evidence."""
        self.e2e.setup_pipelinewise_backend()
        definition = _repository_definition('expired_worker', 'history_table')
        slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]
        with DataDiffRepository.from_backend_config(self._backend_config()) as repository:
            repository.sync_definitions([definition], selected_taps=[definition.tap_id])
            check, = repository.list_checks(tap_id=definition.tap_id)
            run = repository.start_run(check, slot, slot - timedelta(hours=1), slot)
            assert repository.expire_stale_running_attempts(check['check_id'], slot + timedelta(hours=1)) == 1
            before = self._run_evidence(repository, run['run_id'], check['check_id'])
            assert before['dd_run_attempts'][0]['status'] == 'ERROR'
            assert before['dd_run_results'] == []
            assert len(before['dd_watermark_events']) == 1

            with pytest.raises(RunLeaseLostError) as failure:
                if late_action == 'resolve_start':
                    repository.set_run_window_start(run['run_id'], slot - timedelta(days=7))
                else:
                    results = [{'check_type': 'row_count', 'status': 'PASS'}] if late_action == 'PASS' else []
                    repository.finish_run(run['run_id'], late_action, results)
            assert failure.value.status == 'ERROR'
            assert self._run_evidence(repository, run['run_id'], check['check_id']) == before
            assert repository.expire_stale_running_attempts(check['check_id'], slot + timedelta(hours=1)) == 0
            assert self._run_evidence(repository, run['run_id'], check['check_id']) == before

    def test_duplicate_completion_preserves_first_result_and_watermark(self):
        """Repeated completion cannot replace results or append another transition."""
        self.e2e.setup_pipelinewise_backend()
        definition = _repository_definition('duplicate_completion', 'history_table')
        slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]
        with DataDiffRepository.from_backend_config(self._backend_config()) as repository:
            repository.sync_definitions([definition], selected_taps=[definition.tap_id])
            check, = repository.list_checks(tap_id=definition.tap_id)
            run = repository.start_run(check, slot, slot - timedelta(hours=1), slot)
            repository.set_run_window_start(run['run_id'], slot - timedelta(days=7))
            repository.finish_run(run['run_id'], 'PASS', [
                {'check_type': 'row_count', 'status': 'PASS', 'source_value': 4, 'target_value': 4},
            ])
            before = self._run_evidence(repository, run['run_id'], check['check_id'])
            assert len(before['dd_run_results']) == len(before['dd_watermark_events']) == 1
            for late_status in ('FAIL', 'DEFERRED'):
                with pytest.raises(RunLeaseLostError) as failure:
                    repository.finish_run(run['run_id'], late_status, [
                        {'check_type': 'row_count', 'status': 'FAIL', 'source_value': 4, 'target_value': 0},
                    ])
                assert failure.value.status == 'PASS'
                assert self._run_evidence(repository, run['run_id'], check['check_id']) == before

    @pytest.mark.parametrize('late_outcome', ['PASS', 'DEFERRED', 'resolve_start'])
    def test_expired_worker_does_not_prevent_the_next_check(self, monkeypatch, late_outcome):
        """Exercise lease loss inside execution with real terminal database writes."""
        self.e2e.setup_pipelinewise_backend()
        definitions = [
            _repository_definition('lease_loss_runner', table)
            for table in ('a_expired', 'b_next')
        ]
        slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]
        expired = {}
        preflight = {'status': 'PASS', 'query_fingerprint': '0' * 64, 'findings': [], 'index_metadata': []}
        results = [{'check_type': 'row_count', 'status': 'PASS'}]

        with DataDiffRepository.from_backend_config(self._backend_config()) as repository:
            repository.sync_definitions(definitions, selected_taps=[definitions[0].tap_id])

            def complete_after_expiry(check, _source, _target, _start, end, *, on_preflight, on_window_start):
                on_preflight(preflight)
                if check['source_table'] == 'a_expired':
                    assert repository.expire_stale_running_attempts(check['check_id'], end + timedelta(hours=1)) == 1
                    expired['check_id'] = check['check_id']
                    expired['run_id'] = repository.get_check_version(check['check_id'])['blocking_run_id']
                    expired['evidence'] = self._run_evidence(repository, expired['run_id'], check['check_id'])
                    if late_outcome == 'DEFERRED':
                        raise HistoricalWindowNotReady('Neither side has settled history')
                    if late_outcome == 'resolve_start':
                        on_window_start(end - timedelta(days=7))
                else:
                    on_window_start(end - timedelta(days=7))
                return preflight, results, 'PASS'

            monkeypatch.setattr('pipelinewise.data_diff.runner.run_check', complete_after_expiry)
            summaries = run_due_checks(
                repository,
                lambda check: ({'dbname': check['source_database']}, {'dbname': check['target_database']}),
                now=slot + timedelta(minutes=1),
                tap_id=definitions[0].tap_id,
            )
            assert [(summary['check']['source_table'], summary['status']) for summary in summaries] == [
                ('a_expired', 'SKIPPED'), ('b_next', 'PASS'),
            ]
            assert summaries[0]['slot_status'] == 'ERROR'
            assert summaries[0]['error']
            assert self._run_evidence(repository, expired['run_id'], expired['check_id']) == expired['evidence']

    @pytest.mark.parametrize('completed_status', ['PASS', 'FAIL', 'ERROR'])
    def test_historical_scan_inventory_uses_persisted_attempts(self, completed_status):
        """List only history still awaiting a first non-deferred attempt."""
        self.e2e.setup_pipelinewise_backend()
        historical = _repository_definition('history_inventory', 'history_table')
        definitions = [
            historical,
            replace(historical, full_check_name='history_inventory/opt_out', source_table='opt_out',
                    target_table='opt_out', initial_full_scan=False),
            replace(historical, full_check_name='history_inventory/schema_only', source_table='schema_only',
                    target_table='schema_only', checks=('schema_compatibility',)),
            replace(historical, full_check_name='history_inventory/legacy_rolling', source_table='legacy_rolling',
                    target_table='legacy_rolling'),
        ]
        slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]
        with DataDiffRepository.from_backend_config(self._backend_config()) as repository:
            stats = repository.sync_definitions(definitions, selected_taps=[historical.tap_id])
            assert stats['historical_scans_pending'] == 2
            checks = {check['source_table']: check for check in repository.list_checks(tap_id=historical.tap_id)}
            assert {name: check['historical_scan_pending'] for name, check in checks.items()} == {
                'history_table': True, 'opt_out': False, 'schema_only': False, 'legacy_rolling': True,
            }
            legacy_check = {**checks['legacy_rolling'], 'initial_full_scan': False}
            legacy = repository.start_run(legacy_check, slot, slot - timedelta(hours=1), slot)
            repository.finish_run(legacy['run_id'], 'PASS', [{'check_type': 'row_count', 'status': 'PASS'}])
            check = checks['history_table']
            first = repository.start_run(check, slot, slot - timedelta(hours=1), slot)
            stats = repository.sync_definitions(definitions, selected_taps=[historical.tap_id])
            assert stats['historical_scans_pending'] == 0
            repository.finish_run(first['run_id'], 'DEFERRED', [], error='Neither side has settled history')
            stats = repository.sync_definitions(definitions, selected_taps=[historical.tap_id])
            assert stats['historical_scans_pending'] == 1
            checks = {item['source_table']: item for item in repository.list_checks(tap_id=historical.tap_id)}
            assert checks['history_table']['historical_scan_pending'] is True
            assert checks['legacy_rolling']['historical_scan_pending'] is False
            next_slot = slot + timedelta(hours=1)
            next_run = repository.start_run(check, next_slot, slot, next_slot)
            repository.set_run_window_start(next_run['run_id'], slot - timedelta(days=7))
            repository.finish_run(next_run['run_id'], completed_status, [
                {'check_type': 'row_count', 'status': completed_status},
            ])
            stats = repository.sync_definitions(definitions, selected_taps=[historical.tap_id])
            assert stats['historical_scans_pending'] == 0
            assert all(not item['historical_scan_pending'] for item in repository.list_checks(tap_id=historical.tap_id))

    @staticmethod
    def _age_completed_attempt(repository, run_id, next_slot):
        """Place one synthetic attempt before a cron boundary without sleeping."""
        with repository.cursor() as cursor:
            cursor.execute(
                "UPDATE public.dd_run_attempts SET started_at = %s, finished_at = %s WHERE run_id = %s",
                (next_slot - timedelta(minutes=2), next_slot - timedelta(minutes=1), run_id),
            )

    @pytest.mark.parametrize('failed_status', ['FAIL', 'ERROR'])
    def test_failed_windows_retry_once_per_cron_slot_and_repair_watermark(self, failed_status):
        """Retry original historical bounds and retain failures until a retry passes."""
        self.e2e.setup_pipelinewise_backend()
        definition = _repository_definition('scheduled_retry', 'history_table')
        current_slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]
        failed_slot = current_slot - timedelta(hours=1)
        historical_start = failed_slot - timedelta(days=7)
        failure_results = [{'check_type': 'row_count', 'status': failed_status}]
        pass_results = [{'check_type': 'row_count', 'status': 'PASS'}]

        with DataDiffRepository.from_backend_config(self._backend_config()) as repository:
            repository.sync_definitions([definition], selected_taps=[definition.tap_id])
            check, = repository.list_checks(tap_id=definition.tap_id)
            failed = repository.start_run(check, failed_slot, failed_slot - timedelta(hours=1), failed_slot)
            repository.set_run_window_start(failed['run_id'], historical_start)
            repository.finish_run(failed['run_id'], failed_status, failure_results)
            self._age_completed_attempt(repository, failed['run_id'], current_slot)
            eligible, = repository.list_retryable_runs(check['check_id'], current_slot, limit=24)
            assert str(eligible['run_id']) == str(failed['run_id'])

            retry = repository.start_run(
                check, failed_slot, current_slot, current_slot + timedelta(hours=1), retry_before=current_slot,
            )
            assert retry['trigger_type'] == 'RETRY'
            assert retry['attempt'] == 2
            assert (retry['window_start'], retry['window_end']) == (historical_start, failed_slot)
            assert repository.list_retryable_runs(check['check_id'], current_slot, limit=24) == []
            repository.finish_run(retry['run_id'], failed_status, failure_results)
            assert repository.list_retryable_runs(check['check_id'], current_slot, limit=24) == []
            skipped = repository.start_run(
                check, failed_slot, historical_start, failed_slot, retry_before=current_slot,
            )
            assert skipped['status'] == 'SKIPPED'
            assert skipped['slot_status'] == failed_status
            assert (skipped['window_start'], skipped['window_end']) == (historical_start, failed_slot)
            assert skipped['error']

            rolling = repository.start_run(check, current_slot, failed_slot, current_slot)
            repository.finish_run(rolling['run_id'], 'PASS', pass_results)
            blocked = repository.get_check_version(check['check_id'])
            assert blocked['verified_status'] == 'BLOCKED'
            assert str(blocked['blocking_run_id']) == str(retry['run_id'])

            next_slot = current_slot + timedelta(hours=1)
            eligible, = repository.list_retryable_runs(check['check_id'], next_slot, limit=24)
            assert str(eligible['run_id']) == str(retry['run_id'])
            recovered = repository.start_run(
                check, failed_slot, current_slot, next_slot, retry_before=next_slot,
            )
            assert recovered['attempt'] == 3
            assert (recovered['window_start'], recovered['window_end']) == (historical_start, failed_slot)
            repository.finish_run(recovered['run_id'], 'PASS', pass_results)
            assert repository.list_retryable_runs(check['check_id'], next_slot, limit=24) == []
            coverage = repository.get_check_version(check['check_id'])
            assert coverage['verified_status'] == 'CONTIGUOUS'
            assert coverage['blocking_run_id'] is None
            assert (coverage['verified_start'], coverage['verified_end']) == (historical_start, current_slot)
            assert repository.get_run(failed['run_id'])['status'] == failed_status
            assert repository.get_run(retry['run_id'])['status'] == failed_status

    @pytest.mark.parametrize('failed_status', ['FAIL', 'ERROR'])
    def test_scheduled_retry_does_not_prevent_the_new_window(self, failed_status):
        """A failed retry leaves the new cron window runnable, once per interval."""
        self.e2e.setup_pipelinewise_backend()
        definition = replace(_repository_definition('retry_and_rolling', 'history_table'), initial_full_scan=False)
        current_slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]
        failed_slot = current_slot - timedelta(hours=1)
        failed_start = failed_slot - timedelta(hours=1)

        def unavailable_source(_check):
            raise ConnectionError('Synthetic unavailable source')

        with DataDiffRepository.from_backend_config(self._backend_config()) as repository:
            repository.sync_definitions([definition], selected_taps=[definition.tap_id])
            check, = repository.list_checks(tap_id=definition.tap_id)
            failed = repository.start_run(check, failed_slot, failed_start, failed_slot)
            repository.finish_run(
                failed['run_id'], failed_status, [{'check_type': 'row_count', 'status': failed_status}],
            )
            self._age_completed_attempt(repository, failed['run_id'], current_slot)
            summaries = run_due_checks(
                repository, unavailable_source, now=current_slot + timedelta(minutes=1), tap_id=definition.tap_id,
            )
            assert [(run['trigger_type'], run['scheduled_for'], run['status']) for run in summaries] == [
                ('RETRY', failed_slot, 'ERROR'),
                ('SCHEDULED', current_slot, 'ERROR'),
            ]
            assert (summaries[0]['window_start'], summaries[0]['window_end']) == (failed_start, failed_slot)
            repeated = run_due_checks(
                repository, unavailable_source, now=current_slot + timedelta(minutes=2), tap_id=definition.tap_id,
            )
            assert [(run['status'], run['scheduled_for']) for run in repeated] == [('SKIPPED', current_slot)]
            assert repeated[0]['slot_status'] == 'ERROR'
            assert (repeated[0]['window_start'], repeated[0]['window_end']) == (failed_slot, current_slot)
            assert repeated[0]['error']
            with repository.cursor() as cursor:
                cursor.execute(
                    'SELECT COUNT(*) AS count FROM public.dd_run_attempts WHERE check_id = %s',
                    (check['check_id'],),
                )
                assert cursor.fetchone()['count'] == 3

    def test_deferred_retry_keeps_the_unresolved_error_until_next_cron_slot(self):
        """A retry finding empty history cannot clear its earlier coverage blocker."""
        self.e2e.setup_pipelinewise_backend()
        definition = _repository_definition('deferred_retry', 'empty_table')
        current_slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]
        failed_slot = current_slot - timedelta(hours=1)
        with DataDiffRepository.from_backend_config(self._backend_config()) as repository:
            repository.sync_definitions([definition], selected_taps=[definition.tap_id])
            check, = repository.list_checks(tap_id=definition.tap_id)
            failed = repository.start_run(check, failed_slot, failed_slot - timedelta(hours=1), failed_slot)
            repository.finish_run(failed['run_id'], 'ERROR', [], error='Source unavailable before MIN')
            self._age_completed_attempt(repository, failed['run_id'], current_slot)
            retry = repository.start_run(check, failed_slot, None, failed_slot, retry_before=current_slot)
            repository.finish_run(retry['run_id'], 'DEFERRED', [], error='Neither side has settled history')
            assert repository.list_retryable_runs(check['check_id'], current_slot, limit=24) == []
            coverage = repository.get_check_version(check['check_id'])
            assert coverage['verified_status'] == 'BLOCKED'
            assert coverage['verified_start'] is coverage['verified_end'] is None
            assert str(coverage['blocking_run_id']) == str(failed['run_id'])
            next_slot = current_slot + timedelta(hours=1)
            eligible, = repository.list_retryable_runs(check['check_id'], next_slot, limit=24)
            assert str(eligible['run_id']) == str(failed['run_id'])
            next_retry = repository.start_run(check, failed_slot, None, failed_slot, retry_before=next_slot)
            assert next_retry['attempt'] == 3
            assert next_retry['window_start'] is None
            assert next_retry['window_end'] == failed_slot
            repository.finish_run(next_retry['run_id'], 'ERROR', [], error='Source still unavailable')

    def test_metadata_only_check_does_not_allocate_a_historical_window(self):
        """Schema comparisons cannot claim historical row coverage."""
        self.e2e.setup_pipelinewise_backend()
        definition = replace(_repository_definition('schema_only', 'metadata_table'), checks=('schema_compatibility',))
        slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]
        start = slot - timedelta(hours=1)
        with DataDiffRepository.from_backend_config(self._backend_config()) as repository:
            repository.sync_definitions([definition], selected_taps=[definition.tap_id])
            check, = repository.list_checks(tap_id=definition.tap_id)
            run = repository.start_run(check, slot, start, slot)
            assert run['window_start'] == start
            repository.finish_run(run['run_id'], 'PASS', [{'check_type': 'schema_compatibility', 'status': 'PASS'}])
            coverage = repository.get_check_version(check['check_id'])
            assert coverage['verified_start'] == coverage['verified_end'] == start
            assert coverage['verified_status'] == 'BLOCKED'

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
                'dd_check_definitions', 'dd_preflight_log', 'dd_run_attempts', 'dd_run_results',
                'dd_run_slot_state', 'dd_watermark_state',
                'dd_watermark_events',
            } <= tables
            assert {
                'dd_current_coverage', 'dd_remediation_history',
            }.isdisjoint(tables)

            watermark_columns = {
                row[0] for row in self.run_backend_query(
                    "SELECT column_name FROM information_schema.columns"
                    " WHERE table_schema = 'public'"
                    " AND table_name = 'dd_watermark_state'"
                )
            }
            assert {
                'verified_start', 'verified_end', 'furthest_observed_end',
                'verified_status', 'last_evaluated_run_id',
            } <= watermark_columns
            assert {
                'coverage_start', 'verified_through', 'max_observed_end',
                'coverage_status', 'evaluated_run_id',
            }.isdisjoint(watermark_columns)

            watermark_event_columns = {
                row[0] for row in self.run_backend_query(
                    "SELECT column_name FROM information_schema.columns"
                    " WHERE table_schema = 'public'"
                    " AND table_name = 'dd_watermark_events'"
                )
            }
            assert {
                'verified_start', 'previous_verified_end', 'verified_end',
                'furthest_observed_end', 'verified_status', 'evaluated_run_id',
            } <= watermark_event_columns
            assert {
                'coverage_start', 'previous_verified_through',
                'verified_through', 'max_observed_end', 'coverage_status',
            }.isdisjoint(watermark_event_columns)

            assert self.e2e.run_ddl_pipelinewise_backend(
                "SELECT to_regclass(quote_ident(current_user) || '.alembic_version')"
            )[0][0] is None
            assert self.run_backend_query(
                'SELECT version_num FROM public.alembic_version'
            ) == [('003',)]
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

    @pytest.mark.parametrize('unresolved_status', ['ERROR', 'DEFERRED'])
    def test_historical_migration_preserves_data_and_refuses_unsafe_downgrade(self, unresolved_status):
        """Round-trip known bounds and retain incompatible history on rollback."""
        self.e2e.setup_pipelinewise_backend()
        backend = self._backend_config()
        migration_dir = Path(__file__).resolve().parents[3] / 'pipelinewise' / 'backend_db' / 'migrations'
        migration_config = Config(str(migration_dir / 'alembic.ini'))
        migration_config.set_main_option('script_location', str(migration_dir))
        url = URL.create(
            'postgresql+psycopg2', username=backend['ddl_user'], password=backend['ddl_password'],
            host=backend['host'], port=backend['port'], database=backend['dbname'],
            query={'options': '-c timezone=UTC'},
        )
        migration_config.set_main_option('sqlalchemy.url', url.render_as_string(hide_password=False).replace('%', '%%'))
        migration_config.set_main_option('pipelinewise_application_user', backend['user'])
        resolved = replace(_repository_definition('migration_history', 'resolved_table'), initial_full_scan=False)
        unresolved = _repository_definition('migration_history', 'unresolved_table')
        slot = self.run_backend_query("SELECT date_trunc('hour', CURRENT_TIMESTAMP)")[0][0]

        with DataDiffRepository.from_backend_config(backend) as repository:
            repository.sync_definitions([resolved, unresolved], selected_taps=[resolved.tap_id])
            checks = {check['source_table']: check for check in repository.list_checks(tap_id=resolved.tap_id)}
        command.downgrade(migration_config, '002')
        with DataDiffRepository.from_backend_config(backend) as repository:
            run = repository.start_run(checks['resolved_table'], slot, slot - timedelta(hours=1), slot)
            repository.finish_run(run['run_id'], 'PASS', [{'check_type': 'row_count', 'status': 'PASS'}])

        def evidence():
            return {
                table: self.run_backend_query(
                    f'SELECT row_to_json(evidence_row)::text FROM public.{table} evidence_row ORDER BY 1'
                )
                for table in (
                    'dd_check_definitions', 'dd_preflight_log', 'dd_run_attempts', 'dd_run_results',
                    'dd_run_slot_state', 'dd_watermark_state', 'dd_watermark_events',
                )
            }

        def schema_contract():
            return self.run_backend_query(
                "SELECT table_name, column_name, is_nullable FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name LIKE 'dd_%' ORDER BY table_name, ordinal_position"
            ), self.run_backend_query(
                "SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE connamespace = 'public'::regnamespace AND conrelid::regclass::text LIKE 'dd_%' "
                "ORDER BY conrelid::regclass::text, conname"
            )

        revision_002_evidence = evidence()
        revision_002_contract = schema_contract()
        assert len(revision_002_evidence['dd_run_attempts']) == 1
        assert len(revision_002_evidence['dd_watermark_state']) == 1
        assert len(revision_002_evidence['dd_watermark_events']) == 1
        command.upgrade(migration_config, '003')
        assert evidence() == revision_002_evidence
        command.downgrade(migration_config, '002')
        assert evidence() == revision_002_evidence
        assert schema_contract() == revision_002_contract
        assert self.run_backend_query('SELECT version_num FROM public.alembic_version') == [('002',)]
        command.upgrade(migration_config, '003')

        with DataDiffRepository.from_backend_config(backend) as repository:
            pending = repository.start_run(checks['unresolved_table'], slot, slot - timedelta(hours=1), slot)
            assert pending['window_start'] is None
            repository.finish_run(pending['run_id'], unresolved_status, [], error='Historical start unavailable')
        revision_003_evidence = evidence()
        revision_003_contract = schema_contract()
        with pytest.raises(DBAPIError, match='Cannot downgrade data-diff to revision 002'):
            command.downgrade(migration_config, '002')
        assert evidence() == revision_003_evidence
        assert schema_contract() == revision_003_contract
        assert self.run_backend_query('SELECT version_num FROM public.alembic_version') == [('003',)]
