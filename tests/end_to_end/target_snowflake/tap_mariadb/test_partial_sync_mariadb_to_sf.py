import os
import shlex
import signal
import subprocess
import tempfile
import time

import pymysql

from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB


BLOCKED_SESSION_WAIT_SECONDS = 300
TERMINATION_WAIT_SECONDS = 30
POLL_SECONDS = 0.5


def _connect(e2e_env, connector):
    return pymysql.connect(
        host=e2e_env.get_conn_env_var(connector, 'HOST'),
        port=int(e2e_env.get_conn_env_var(connector, 'PORT')),
        user=e2e_env.get_conn_env_var(connector, 'USER'),
        password=e2e_env.get_conn_env_var(connector, 'PASSWORD'),
        database=e2e_env.get_conn_env_var(connector, 'DB'),
        charset='utf8mb4',
        ssl={'': True},
        autocommit=True,
    )


def _user_sessions(conn):
    """Return this user's sessions as ``{id: (state, info)}``."""
    with conn.cursor() as cursor:
        cursor.execute(
            'SELECT ID, STATE, INFO FROM information_schema.PROCESSLIST '
            "WHERE USER = SUBSTRING_INDEX(CURRENT_USER(), '@', 1)"
        )
        return {row[0]: (row[1] or '', row[2] or '') for row in cursor.fetchall()}


def _kill_sessions(conn, session_ids):
    with conn.cursor() as cursor:
        for session_id in session_ids:
            try:
                cursor.execute('KILL CONNECTION %s', (session_id,))
            except pymysql.MySQLError:
                pass


def _stop_process_group(proc):
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()


def _process_output(log):
    log.seek(0)
    return log.read().decode('utf-8', errors='replace')


def assert_table_locked(test_case, proc, monitor_conn, table, preexisting_sessions, log):
    """Wait until the PartialSync source query is blocked by the table lock."""
    deadline = time.monotonic() + BLOCKED_SESSION_WAIT_SECONDS
    while True:
        test_case.assertIsNone(
            proc.poll(), f'PartialSync exited before its export blocked:\n{_process_output(log)}'
        )
        test_case.assertLess(
            time.monotonic(), deadline, f'PartialSync export never blocked:\n{_process_output(log)}'
        )
        time.sleep(POLL_SECONDS)
        sessions = _user_sessions(monitor_conn)
        blocked_sessions = {
            session_id for session_id, (state, info) in sessions.items()
            if session_id not in preexisting_sessions
            and 'metadata lock' in state.lower()
            and table in info
        }
        if blocked_sessions:
            return set(sessions) - preexisting_sessions



def _safe_mysql_datetime(column):
    """Mirror the FastSync export rule that nulls invalid MySQL datetimes."""
    quoted_column = f'`{column}`'
    return (
        f'CASE WHEN YEAR({quoted_column}) = 0 '
        f'OR MONTH({quoted_column}) NOT BETWEEN 1 AND 12 '
        f'OR DAY({quoted_column}) = 0 '
        f'OR DAY({quoted_column}) > '
        f'DAY(LAST_DAY(DATE_FORMAT({quoted_column}, "%Y-%m-01"))) '
        f'THEN NULL ELSE {quoted_column} END'
    )


MARIADB_FASTSYNC_COMPARISON_COLUMNS = [
    {
        'name': 'weight_unit_id',
        'source_expression': '`weight_unit_id`',
        'target_expression': '"WEIGHT_UNIT_ID"',
        'normalizer': 'integer',
    },
    {
        'name': 'weight_unit_name',
        'source_expression': '`weight_unit_name`',
        'target_expression': '"WEIGHT_UNIT_NAME"',
        'normalizer': 'text',
        'source_normalizer': 'hash_skip_first_2',
    },
    {
        'name': 'isActive',
        'source_expression': '`isActive`',
        'target_expression': '"ISACTIVE"',
        'normalizer': 'boolean',
    },
    {
        'name': 'original_date_created',
        'source_expression': '`original_date_created`',
        'target_expression': '"ORIGINAL_DATE_CREATED"',
        'normalizer': 'text',
    },
    {
        'name': 'date_created',
        'source_expression': _safe_mysql_datetime('date_created'),
        'target_expression': '"DATE_CREATED"',
        'normalizer': 'datetime',
    },
    {
        'name': 'date_updated',
        'source_expression': _safe_mysql_datetime('date_updated'),
        'target_expression': '"DATE_UPDATED"',
        'normalizer': 'datetime',
    },
]


class TestPartialSyncMariaDBToSF(TapMariaDB):
    """
    Test cases for Partial sync table from MariaDB to Snowflake
    """

    def setUp(self):
        self.table = 'weight_unit'
        self.column = 'weight_unit_id'
        super().setUp(tap_id='mariadb_to_sf', target_id='snowflake')
        self.tap_parameters = {
            'env': self.e2e_env,
            'tap': self.tap_id,
            'tap_type': 'mysql',
            'target': self.target_id,
            'source_db': self.e2e_env.get_conn_env_var('TAP_MYSQL', 'DB'),
            'table': self.table,
            'column': self.column,
            'comparison_columns': MARIADB_FASTSYNC_COMPARISON_COLUMNS,
        }
        assertions.assert_resync_populates_target(
            self.tap_parameters, primary_key=self.column
        )

        # Deleting all records from the target with primary key greater than 1
        self.e2e_env.delete_record_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.tap_parameters['table'],
            where_clause=f'WHERE {self.column} > 1'
        )

    def test_partial_sync_mariadb_to_sf(self):
        """
        Test partial sync table from MariaDB to Snowflake
        """

        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=4,
            end_value=6,
        )
        assertions.assert_source_target_rows_equal(
            self.tap_parameters,
            primary_key=self.column,
            where_clause=(
                f'WHERE {self.column} = 1 '
                f'OR {self.column} BETWEEN 4 AND 6'
            ),
            operation='PartialSync',
        )

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        expected_records_for_column = [1, 4, 5, 6]
        column_to_check = primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', self.table, column_to_check, primary_key, expected_records_for_column
        )

    def test_terminated_partial_sync_kills_blocked_source_query(self):
        """SIGTERM ends the run's MariaDB sessions instead of leaving the export running."""
        # Setup
        lock_conn = _connect(self.tap_parameters['env'], 'TAP_MYSQL')
        monitor_conn = _connect(self.tap_parameters['env'], 'TAP_MYSQL')
        self.addCleanup(lock_conn.close)
        self.addCleanup(monitor_conn.close)
        own_sessions = {lock_conn.thread_id(), monitor_conn.thread_id()}
        preexisting_sessions = set(_user_sessions(monitor_conn)) | own_sessions

        with lock_conn.cursor() as cursor:
            cursor.execute(f'LOCK TABLES `{self.tap_parameters['table']}` WRITE')

        log = tempfile.TemporaryFile()
        self.addCleanup(log.close)
        command = assertions._get_command_for_partial_sync(self.tap_parameters, start_value=4, end_value=6)
        proc = subprocess.Popen(
            shlex.split(command), stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        run_sessions = set()

        # Cleanups run in reverse: stop the run, kill leftovers, then release the lock.
        self.addCleanup(lambda: _kill_sessions(monitor_conn, run_sessions))
        self.addCleanup(_stop_process_group, proc)

        # Wait for the table to be locked before proceeding
        run_sessions.update(
            assert_table_locked(self, proc, monitor_conn, self.tap_parameters['table'], preexisting_sessions, log)
        )

        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=TERMINATION_WAIT_SECONDS)
        except subprocess.TimeoutExpired:
            self.fail(f'PartialSync did not exit after SIGTERM:\n{_process_output(log)}')
        self.assertNotEqual(proc.returncode, 0, _process_output(log))

        deadline = time.monotonic() + TERMINATION_WAIT_SECONDS
        remaining = run_sessions & set(_user_sessions(monitor_conn))
        while remaining and time.monotonic() < deadline:
            time.sleep(POLL_SECONDS)
            remaining = run_sessions & set(_user_sessions(monitor_conn))
        self.assertEqual(
            remaining, set(),
            f'Source sessions outlived the terminated PartialSync:\n{_process_output(log)}',
        )
    
    def test_unbounded_empty_range_removes_target_only_row(self):
        """Publish an empty source range so its target-only row is deleted."""
        source_max = self.e2e_env.run_query_tap_mysql(
            f'SELECT MAX({self.column}) FROM {self.table}'
        )[0][0]
        target_only_key = int(source_max) + 1

        self.e2e_env.run_query_target_snowflake(
            f'UPDATE ppw_e2e_tap_mysql{self.e2e_env.sf_schema_postfix}.{self.table} '
            f'SET "{self.column.upper()}" = {target_only_key} '
            f'WHERE "{self.column.upper()}" = 1'
        )
        source_rows = self.e2e_env.get_rows_from_source(
            tap_type='mysql',
            source_db=self.tap_parameters['source_db'],
            table=self.table,
            columns=[self.column],
            primary_key=self.column,
            where_clause=f'WHERE {self.column} >= {target_only_key}',
        )
        target_rows = self.e2e_env.get_rows_from_target_snowflake(
            tap_type='mysql',
            table=self.table,
            columns=[f'"{self.column.upper()}"'],
            primary_key=self.column,
        )
        self.assertEqual(source_rows, [])
        self.assertEqual(target_rows, [(target_only_key,)])

        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=target_only_key,
        )

        target_rows = self.e2e_env.get_rows_from_target_snowflake(
            tap_type='mysql',
            table=self.table,
            columns=[f'"{self.column.upper()}"'],
            primary_key=self.column,
        )
        self.assertEqual(target_rows, [])

    def test_partial_sync_if_there_is_additional_column_in_source(self):
        """
        Test partial sync table from MariaDB to Snowflake if there are additional columns in source
        """

        additional_column = 'FOO_NEW_COLUMN_SOURCE'
        additional_column_value = 345
        target_row_before = self.e2e_env.get_rows_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.table,
            columns=[
                column['target_expression']
                for column in self.tap_parameters['comparison_columns']
            ],
            primary_key=self.column,
            where_clause=f'WHERE {self.column} = 1',
        )
        assertions.assert_partial_sync_table_with_source_additional_columns(
            self.tap_parameters,
            additional_column={'name': additional_column, 'value': additional_column_value},
            start_value=4,
            end_value=6,
        )
        assertions.assert_source_target_rows_equal(
            self.tap_parameters,
            primary_key=self.column,
            where_clause=f'WHERE {self.column} BETWEEN 4 AND 6',
            operation='PartialSync',
        )
        target_row_after = self.e2e_env.get_rows_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.table,
            columns=[
                column['target_expression']
                for column in self.tap_parameters['comparison_columns']
            ],
            primary_key=self.column,
            where_clause=f'WHERE {self.column} = 1',
        )
        self.assertEqual(target_row_after, target_row_before)

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        # records start_value to end_value will be with the value same as source because
        # out of this range wont be touched and they will have None

        expected_records_for_column = [None, additional_column_value, additional_column_value, additional_column_value]
        primary_key = self.column
        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', self.table, additional_column, primary_key, expected_records_for_column
        )

    def test_partial_sync_if_there_is_additional_column_in_target(self):
        """
        Test partial sync table from MariaDB to Snowflake if there are additional columns in target
        """

        additional_column_value = 567
        additional_column = 'FOO_NEW_COLUMN_TARGET'
        assertions.assert_partial_sync_table_with_target_additional_columns(
            self.tap_parameters,
            additional_column={'name': additional_column, 'value': additional_column_value},
            start_value=4,
            end_value=6,
        )
        assertions.assert_source_target_rows_equal(
            self.tap_parameters,
            primary_key=self.column,
            where_clause=(
                f'WHERE {self.column} = 1 '
                f'OR {self.column} BETWEEN 4 AND 6'
            ),
            operation='PartialSync',
        )

        # for this test, all records with id > 1 are deleted from the target and then will do a partial sync
        # records start_value to end_value should be None because these columns do not exist in the source and records
        # out of this range wont be touched and they will have their original value
        expected_records_for_column = [additional_column_value, None, None, None]
        primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', self.table, additional_column, primary_key, expected_records_for_column
        )

    def test_partial_sync_removes_existing_row_deleted_from_source(self):
        """Delete a stale target row within the range and preserve the row outside it."""
        # The shared setup trims the target; restore the row before deleting it at source.
        assertions.assert_resync_populates_target(
            self.tap_parameters, primary_key=self.column
        )
        self.e2e_env.delete_record_from_source('mysql', self.table, 'WHERE weight_unit_id=5')

        self.e2e_env.delete_record_from_target_snowflake(
            tap_type=self.tap_parameters['tap_type'],
            table=self.table,
            where_clause=f'WHERE {self.column} > 1 AND {self.column} <> 5'
        )
        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', self.table, self.column, self.column, [1, 5]
        )

        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=4,
            end_value=6,
        )
        assertions.assert_source_target_rows_equal(
            self.tap_parameters,
            primary_key=self.column,
            where_clause=(
                f'WHERE {self.column} = 1 '
                f'OR {self.column} BETWEEN 4 AND 6'
            ),
            operation='PartialSync',
        )

        # Row 1 survives outside the range; row 5 is removed and rows 4 and 6 are inserted.
        expected_records_for_column = [1, 4, 6]
        column_to_check = primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', self.table, column_to_check, primary_key, expected_records_for_column
        )

    def test_partial_sync_if_table_does_not_exist_in_target(self):
        """Test partial sync if table does not exist in target"""
        # Dropping the table
        self.e2e_env.run_query_target_snowflake(
            f'DROP TABLE ppw_e2e_tap_{self.tap_parameters["tap_type"]}{self.e2e_env.sf_schema_postfix}.{self.table}'
        )

        assertions.assert_partial_sync_table_success(
            self.tap_parameters,
            start_value=4,
            end_value=6,
        )
        assertions.assert_source_target_rows_equal(
            self.tap_parameters,
            primary_key=self.column,
            where_clause=f'WHERE {self.column} BETWEEN 4 AND 6',
            operation='PartialSync',
        )

        expected_records_for_column = [4, 5, 6]
        column_to_check = primary_key = self.column

        assertions.assert_partial_sync_rows_in_target(
            self.e2e_env, 'mysql', self.table, column_to_check, primary_key, expected_records_for_column
        )
