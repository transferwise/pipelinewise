import datetime
import io

import psycopg2

from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, Mock, call, patch

from pipelinewise.fastsync.commons.tap_yugabyte import FastSyncTapYugabyte
from pipelinewise.fastsync.commons import tap_yugabyte, yb_retry
from pipelinewise.fastsync.commons.partial_sync_boundary import (
    PartialSyncBoundary,
)


class TestFastSyncTapYugabyte(TestCase):
    """
    Unit tests for fastsync tap yugabyte
    """

    def setUp(self) -> None:
        """Initialise test FastSyncTapYugabyte object"""
        self.yugabyte = FastSyncTapYugabyte(
            connection_config={'dbname': 'test_database', 'tap_id': 'test_tap'},
            tap_type_to_target_type={},
        )

    def test_generate_replication_slot_name(self):
        """Validate if the replication slot name generated correctly"""
        self.assertEqual(
            self.yugabyte.generate_replication_slot_name('some_db'),
            'pipelinewise_some_db',
        )

        self.assertEqual(
            self.yugabyte.generate_replication_slot_name('some_db', 'some_tap'),
            'pipelinewise_some_db_some_tap',
        )

        self.assertEqual(
            self.yugabyte.generate_replication_slot_name(
                'some_db', 'some_tap', prefix='custom_prefix'
            ),
            'custom_prefix_some_db_some_tap',
        )

        self.assertEqual(
            self.yugabyte.generate_replication_slot_name('SoMe_DB', 'SoMe_TaP'),
            'pipelinewise_some_db_some_tap',
        )

        self.assertEqual(
            self.yugabyte.generate_replication_slot_name('some-db', 'some-tap'),
            'pipelinewise_some_db_some_tap',
        )

        self.assertEqual(
            self.yugabyte.generate_replication_slot_name('some.db', 'some.tap'),
            'pipelinewise_some_db_some_tap',
        )

    def test_create_replication_slot_issues_hybrid_time_slot_creation(self):
        """create_replication_slot must request a HYBRID_TIME wal2json slot"""
        with patch.object(self.yugabyte, 'query') as query_mock:
            self.yugabyte.create_replication_slot()

        query_mock.assert_called_once_with(
            "SELECT * FROM pg_create_logical_replication_slot("
            "'pipelinewise_test_database_test_tap', 'wal2json', false, false, 'HYBRID_TIME')"
        )

    def test_create_replication_slot_is_idempotent_if_slot_already_exists(self):
        """A pre-existing slot (SQL state 42710) must not raise"""
        already_exists = Exception('replication slot already exists')
        already_exists.pgcode = '42710'

        with patch.object(self.yugabyte, 'query', side_effect=already_exists):
            self.yugabyte.create_replication_slot()

    def test_create_replication_slot_raises_on_other_errors(self):
        """Any other database error must propagate"""
        other_error = Exception('permission denied')
        other_error.pgcode = '42501'

        with patch.object(self.yugabyte, 'query', side_effect=other_error):
            with self.assertRaises(Exception) as context:
                self.yugabyte.create_replication_slot()

        self.assertEqual('permission denied', str(context.exception))

    def test_fetch_table_sizes_mb_reads_each_schema_once_and_marks_unsized_tables(self):
        """Sizes are MB per selected table; NULL (colocated) and absent tables (views) are None"""
        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            [('orders', 2 * 1024 * 1024), ('unselected', 99 * 1024 * 1024)],
            [('coloc', None)],
        ]
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        with patch.object(FastSyncTapYugabyte, 'admin_connection', return_value=connection) as admin_connection:
            sizes = FastSyncTapYugabyte.fetch_table_sizes_mb(
                {'host': 'h', 'load_balance': 'true'}, ['public.orders', 'public.a_view', 'sales.coloc'],
            )

        self.assertEqual({'public.orders': 2.0, 'public.a_view': None, 'sales.coloc': None}, sizes)
        admin_connection.assert_called_once_with({'host': 'h', 'load_balance': 'true'})
        self.assertEqual(
            [call(tap_yugabyte._TABLE_SIZE_SQL, ('public',)), call(tap_yugabyte._TABLE_SIZE_SQL, ('sales',))],
            cursor.execute.call_args_list,
        )
        connection.close.assert_called_once_with()

    def test_fetch_table_sizes_mb_closes_connection_after_query_failure(self):
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value.execute.side_effect = psycopg2.OperationalError('down')
        with patch.object(FastSyncTapYugabyte, 'admin_connection', return_value=connection):
            with self.assertRaises(psycopg2.OperationalError):
                FastSyncTapYugabyte.fetch_table_sizes_mb({}, ['public.orders'])

        connection.close.assert_called_once_with()

    def test_fetch_current_log_pos_returns_hybrid_time_boundary(self):
        """fetch_current_log_pos must read yb_restart_commit_ht, not the placeholder lsn"""
        with patch.object(
            self.yugabyte, 'create_replication_slot'
        ) as create_replication_slot, patch.object(
            self.yugabyte,
            'query',
            return_value=[{'yb_restart_commit_ht': 123456789, 'plugin': 'wal2json', 'yb_lsn_type': 'HYBRID_TIME'}],
        ) as query_mock:
            bookmark = self.yugabyte.fetch_current_log_pos()

        self.assertEqual({'lsn': 123456789, 'version': 1}, bookmark)
        create_replication_slot.assert_called_once_with()
        query_mock.assert_called_once_with(
            "SELECT yb_restart_commit_ht, plugin, yb_lsn_type FROM pg_replication_slots "
            "WHERE slot_name = 'pipelinewise_test_database_test_tap'"
        )
        self.assertEqual(123456789, self.yugabyte._snapshot_ht)

    def test_fetch_current_log_pos_rejects_incompatible_existing_slot(self):
        """A tolerated pre-existing slot must be wal2json HYBRID_TIME before any copy is pinned to it"""
        for plugin, lsn_type in (('wal2json', 'SEQUENCE'), ('pgoutput', 'HYBRID_TIME')):
            with self.subTest(plugin=plugin, lsn_type=lsn_type), patch.object(
                self.yugabyte, 'create_replication_slot'
            ), patch.object(
                self.yugabyte,
                'query',
                return_value=[{'yb_restart_commit_ht': 123456789, 'plugin': plugin, 'yb_lsn_type': lsn_type}],
            ):
                with self.assertRaises(RuntimeError) as context:
                    self.yugabyte.fetch_current_log_pos()

                self.assertIn(f'plugin {plugin} and LSN type {lsn_type}', str(context.exception))
                self.assertIsNone(self.yugabyte._snapshot_ht)

    def test_fetch_current_log_pos_raises_if_slot_missing_after_creation(self):
        """An empty result after slot creation is unexpected and must raise"""
        with patch.object(self.yugabyte, 'create_replication_slot'), patch.object(
            self.yugabyte, 'query', return_value=[]
        ):
            with self.assertRaises(Exception) as context:
                self.yugabyte.fetch_current_log_pos()

        self.assertIn('not found after creation', str(context.exception))

    def test_fetch_current_incremental_key_pos_empty_result_expect_exception(self):
        """test fetch_current_incremental_key_pos where result is empty, it should raise an exception"""
        with patch.object(self.yugabyte, 'query') as query_mock:
            query_mock.return_value = None

            with self.assertRaises(Exception) as context:
                self.yugabyte.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertEqual(
                'Cannot get replication key value for table: schema.table1',
                str(context.exception),
            )

    def test_fetch_current_incremental_key_pos_empty_key_value_return_empty_state(self):
        """test fetch_current_incremental_key_pos where key value is empty, it should return an empty state"""
        with patch.object(self.yugabyte, 'query') as query_mock:
            query_mock.return_value = [{'key_value': None}]

            state = self.yugabyte.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertFalse(state)

    def test_fetch_current_incremental_key_pos_non_empty_key_value_return_state(self):
        """test fetch_current_incremental_key_pos where result exists, it should return a non empty state"""
        with patch.object(self.yugabyte, 'query') as query_mock:
            query_mock.return_value = [{'key_value': 123}]

            state = self.yugabyte.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual(
                {
                    'replication_key': 'id',
                    'replication_key_value': 123,
                    'version': 1,
                },
                state,
            )

    def test_fetch_current_incremental_key_pos_datetime_key_value_return_state(self):
        """test fetch_current_incremental_key_pos where result is datetime, it should be iso formatted"""
        with patch.object(self.yugabyte, 'query') as query_mock:
            query_mock.return_value = [{'key_value': datetime.datetime(2020, 1, 24, 7, 12, 6)}]

            state = self.yugabyte.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual(
                {
                    'replication_key': 'id',
                    'replication_key_value': '2020-01-24T07:12:06',
                    'version': 1,
                },
                state,
            )

    def test_fetch_current_incremental_key_pos_date_key_value_return_state(self):
        """test fetch_current_incremental_key_pos where result is date, it should be iso formatted"""
        with patch.object(self.yugabyte, 'query') as query_mock:
            query_mock.return_value = [{'key_value': datetime.date(2020, 1, 24)}]

            state = self.yugabyte.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual(
                {
                    'replication_key': 'id',
                    'replication_key_value': '2020-01-24T00:00:00',
                    'version': 1,
                },
                state,
            )

    def test_fetch_current_incremental_key_pos_decimal_key_value_return_state(self):
        """Decimal bookmarks preserve every digit as a JSON string."""
        with patch.object(self.yugabyte, 'query') as query_mock:
            query_mock.return_value = [{'key_value': Decimal('12345678901234567890.123456789')}]

            state = self.yugabyte.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual(
                {
                    'replication_key': 'id',
                    'replication_key_value': '12345678901234567890.123456789',
                    'version': 1,
                },
                state,
            )

    def test_get_connection_builds_conn_params(self):
        """get_connection must use the right credentials and enable autocommit"""
        creds = {
            'host': 'my_host',
            'user': 'my_user',
            'password': 'my_password',
            'dbname': 'my_db',
            'port': 'my_port',
        }

        with patch.object(tap_yugabyte.psycopg2, 'connect') as connect_mock:
            connection = FastSyncTapYugabyte.get_connection(creds)

        self.assertEqual(connection, connect_mock.return_value)
        connect_mock.assert_called_once_with(
            host='my_host',
            port='my_port',
            user='my_user',
            password='my_password',
            dbname='my_db',
        )
        self.assertTrue(connection.autocommit)

    def test_get_connection_passes_password_verbatim(self):
        """get_connection must not quote or escape a password containing spaces and quotes"""
        creds = {
            'host': 'my_host',
            'user': 'my_user',
            'password': "pa ss'word\\",
            'dbname': 'my_db',
            'port': 'my_port',
        }

        with patch.object(tap_yugabyte.psycopg2, 'connect') as connect_mock:
            FastSyncTapYugabyte.get_connection(creds)

        self.assertEqual("pa ss'word\\", connect_mock.call_args.kwargs['password'])

    def test_get_connection_with_ssl(self):
        """get_connection must request sslmode=require when ssl is requested"""
        creds = {
            'host': 'my_host',
            'user': 'my_user',
            'password': 'my_password',
            'dbname': 'my_db',
            'port': 'my_port',
            'ssl': 'true',
        }

        with patch.object(tap_yugabyte.psycopg2, 'connect') as connect_mock:
            FastSyncTapYugabyte.get_connection(creds)

        connect_mock.assert_called_once_with(
            host='my_host',
            port='my_port',
            user='my_user',
            password='my_password',
            dbname='my_db',
            sslmode='require',
        )

    def test_get_connection_with_load_balance(self):
        """get_connection must forward an unquoted load_balance when set in connection_config"""
        creds = {
            'host': 'my_host',
            'user': 'my_user',
            'password': 'my_password',
            'dbname': 'my_db',
            'port': 'my_port',
            'load_balance': 'true',
        }

        with patch.object(tap_yugabyte.psycopg2, 'connect') as connect_mock:
            FastSyncTapYugabyte.get_connection(creds)

        # The driver strips these two options itself, so a quoted value would reach libpq
        # and be rejected as an invalid connection option.
        connect_mock.assert_called_once_with(
            host='my_host',
            port='my_port',
            user='my_user',
            password='my_password',
            dbname='my_db',
            load_balance='true',
        )

    def test_get_connection_with_topology_keys(self):
        """get_connection must forward an unquoted topology_keys when set in connection_config"""
        creds = {
            'host': 'my_host',
            'user': 'my_user',
            'password': 'my_password',
            'dbname': 'my_db',
            'port': 'my_port',
            'load_balance': 'any',
            'topology_keys': 'cloud1.region1.zone1,cloud1.region1.zone2',
        }

        with patch.object(tap_yugabyte.psycopg2, 'connect') as connect_mock:
            FastSyncTapYugabyte.get_connection(creds)

        connect_mock.assert_called_once_with(
            host='my_host',
            port='my_port',
            user='my_user',
            password='my_password',
            dbname='my_db',
            load_balance='any',
            topology_keys='cloud1.region1.zone1,cloud1.region1.zone2',
        )

    def test_get_connection_omits_unset_load_balancing_options(self):
        """get_connection must not send load_balance/topology_keys when they are unset"""
        creds = {
            'host': 'my_host',
            'user': 'my_user',
            'password': 'my_password',
            'dbname': 'my_db',
            'port': 'my_port',
            'load_balance': '',
            'topology_keys': '',
        }

        with patch.object(tap_yugabyte.psycopg2, 'connect') as connect_mock:
            FastSyncTapYugabyte.get_connection(creds)

        self.assertNotIn('load_balance', connect_mock.call_args.kwargs)
        self.assertNotIn('topology_keys', connect_mock.call_args.kwargs)

    def test_drop_slot_retries_while_slot_is_active(self):
        """drop_slot must retry on 'slot is active' and eventually succeed"""
        slot_active_error = _psycopg2_error('replication slot "my_db_tap_test" is active')

        cursor_mock = MagicMock()
        cursor_mock.__enter__.return_value.execute.side_effect = [
            slot_active_error,
            None,
        ]
        connection = Mock()
        connection.cursor.return_value = cursor_mock

        creds = {'dbname': 'my_db', 'tap_id': 'tap_test'}

        with patch.object(
            FastSyncTapYugabyte, 'get_connection', return_value=connection
        ), patch.object(tap_yugabyte.time, 'sleep') as sleep_mock:
            FastSyncTapYugabyte.drop_slot(creds)

        self.assertEqual(2, cursor_mock.__enter__.return_value.execute.call_count)
        sleep_mock.assert_called_once()
        connection.close.assert_called_once_with()

    def test_drop_slot_raises_non_active_errors_immediately(self):
        """A non 'slot is active' database error must propagate without retrying"""
        other_error = _psycopg2_error('permission denied')

        cursor_mock = MagicMock()
        cursor_mock.__enter__.return_value.execute.side_effect = other_error
        connection = Mock()
        connection.cursor.return_value = cursor_mock

        creds = {'dbname': 'my_db', 'tap_id': 'tap_test'}

        with patch.object(
            FastSyncTapYugabyte, 'get_connection', return_value=connection
        ), patch.object(tap_yugabyte.time, 'sleep') as sleep_mock:
            with self.assertRaises(Exception):
                FastSyncTapYugabyte.drop_slot(creds)

        sleep_mock.assert_not_called()
        connection.close.assert_called_once_with()

    def test_reset_slot_drops_only_current_slot_after_state_invalidation(self):
        """Only the preflighted tap-specific slot is dropped, after state is durable."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False)]
        creds = {'dbname': 'my_db', 'tap_id': 'my_tap', 'load_balance': 'true', 'topology_keys': 'aws.*'}
        before_reset = MagicMock(return_value='state.backup')
        calls = MagicMock()
        calls.attach_mock(cursor.execute, 'execute')
        calls.attach_mock(before_reset, 'before_reset')

        with patch.object(FastSyncTapYugabyte, 'get_connection', return_value=connection) as get_connection:
            FastSyncTapYugabyte.reset_slot(creds, before_reset=before_reset)

        get_connection.assert_called_once_with({'dbname': 'my_db', 'tap_id': 'my_tap'})
        self.assertEqual(calls.mock_calls, [
            call.execute(
                'SELECT slot_name, database, plugin, active FROM pg_replication_slots '
                'WHERE slot_name IN (%s, %s)',
                ('pipelinewise_my_db', 'pipelinewise_my_db_my_tap'),
            ),
            call.before_reset(),
            call.execute('SELECT pg_drop_replication_slot(%s)', ('pipelinewise_my_db_my_tap',)),
            call.execute(
                'SELECT * FROM pg_create_logical_replication_slot(%s, %s, %s, %s, %s)',
                ('pipelinewise_my_db_my_tap', 'wal2json', False, False, 'HYBRID_TIME'),
            ),
        ])
        connection.close.assert_called_once_with()

    def test_reset_slot_creates_missing_slot_without_dropping_any_slot(self):
        """A missing slot still requires state invalidation before creation."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []
        before_reset = MagicMock(return_value=None)

        with patch.object(FastSyncTapYugabyte, 'get_connection', return_value=connection):
            FastSyncTapYugabyte.reset_slot({'dbname': 'my_db', 'tap_id': 'my_tap'}, before_reset=before_reset)

        before_reset.assert_called_once_with()
        self.assertEqual(cursor.execute.call_count, 2)
        cursor.execute.assert_called_with(
            'SELECT * FROM pg_create_logical_replication_slot(%s, %s, %s, %s, %s)',
            ('pipelinewise_my_db_my_tap', 'wal2json', False, False, 'HYBRID_TIME'),
        )
        connection.close.assert_called_once_with()

    def test_reset_slot_rejects_legacy_and_incompatible_slots_before_state_changes(self):
        """Legacy lookup takes precedence even when an inactive tap-specific slot also exists."""
        current = ('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False)
        cases = [
            ([('pipelinewise_my_db', 'my_db', 'wal2json', active)] + modern, 'legacy')
            for active in (False, True) for modern in ([], [current])
        ] + [
            ([(current[0], database, plugin, False)], 'must use wal2json')
            for database, plugin in (('other_db', 'wal2json'), ('my_db', 'yboutput'))
        ]
        for rows, message in cases:
            with self.subTest(rows=rows):
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = rows
                before_reset = MagicMock()
                with patch.object(FastSyncTapYugabyte, 'get_connection', return_value=connection):
                    with self.assertRaisesRegex(RuntimeError, message):
                        FastSyncTapYugabyte.reset_slot(
                            {'dbname': 'my_db', 'tap_id': 'my_tap'}, before_reset=before_reset,
                        )
                before_reset.assert_not_called()
                self.assertEqual(cursor.execute.call_count, 1)
                connection.close.assert_called_once_with()

    def test_reset_slot_waits_out_the_active_slot_window(self):
        """A lingering active flag delays the reset; one that outlasts the window rejects it."""
        active = ('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', True)
        inactive = active[:3] + (False,)
        attempts = tap_yugabyte._DROP_SLOT_RETRY_ATTEMPTS
        for rows, succeeds in (([[active], [inactive]], True), ([[active]] * attempts, False)):
            with self.subTest(succeeds=succeeds):
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.side_effect = rows
                before_reset = MagicMock(return_value=None)
                with patch.object(FastSyncTapYugabyte, 'get_connection', return_value=connection), patch.object(
                    tap_yugabyte.time, 'sleep',
                ) as sleep_mock:
                    if succeeds:
                        FastSyncTapYugabyte.reset_slot(
                            {'dbname': 'my_db', 'tap_id': 'my_tap'}, before_reset=before_reset,
                        )
                        before_reset.assert_called_once_with()
                    else:
                        with self.assertRaisesRegex(RuntimeError, 'still active'):
                            FastSyncTapYugabyte.reset_slot(
                                {'dbname': 'my_db', 'tap_id': 'my_tap'}, before_reset=before_reset,
                            )
                        before_reset.assert_not_called()
                self.assertEqual(sleep_mock.call_count, len(rows) - 1)
                connection.close.assert_called_once_with()

    def test_reset_slot_name_length_boundary(self):
        """Accept exactly 63 characters; reject a longer name before SQL or state changes."""
        slot_prefix = FastSyncTapYugabyte.generate_replication_slot_name('my_db') + '_'
        for length in (63, 64):
            with self.subTest(length=length):
                config = {'dbname': 'my_db', 'tap_id': 't' * (length - len(slot_prefix))}
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = []
                before_reset = MagicMock(return_value=None)
                with patch.object(FastSyncTapYugabyte, 'get_connection', return_value=connection):
                    if length == 64:
                        with self.assertRaisesRegex(RuntimeError, 'at most 63 characters'):
                            FastSyncTapYugabyte.reset_slot(config, before_reset=before_reset)
                        cursor.execute.assert_not_called()
                        before_reset.assert_not_called()
                    else:
                        FastSyncTapYugabyte.reset_slot(config, before_reset=before_reset)
                        before_reset.assert_called_once_with()
                connection.close.assert_called_once_with()

    def test_reset_slot_reports_failed_phase_and_backup_without_restoring_state(self):
        """A source error after invalidation names its phase and the pre-reset backup."""
        for phase, sql in (('drop', 'pg_drop_'), ('create', 'pg_create_')):
            with self.subTest(phase=phase):
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = [('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False)]

                def execute(statement, _params, failing_sql=sql):
                    if failing_sql in statement:
                        raise _psycopg2_error('permission denied')

                cursor.execute.side_effect = execute
                before_reset = MagicMock(return_value='state.backup')
                with patch.object(FastSyncTapYugabyte, 'get_connection', return_value=connection), patch.object(
                    tap_yugabyte.time, 'sleep',
                ) as sleep_mock:
                    with self.assertRaisesRegex(RuntimeError, f'failed during {phase}') as error:
                        FastSyncTapYugabyte.reset_slot(
                            {'dbname': 'my_db', 'tap_id': 'my_tap'}, before_reset=before_reset,
                        )
                self.assertIn('state.backup', str(error.exception))
                self.assertIn('Do not restore old LOG_BASED bookmarks', str(error.exception))
                before_reset.assert_called_once_with()
                sleep_mock.assert_not_called()
                connection.close.assert_called_once_with()

    def test_copy_table_pins_snapshot_when_hybrid_time_is_known(self):
        """copy_table must pin yb_read_time to the captured snapshot boundary before exporting"""
        table_columns = [{'safe_sql_value': '"id"'}]
        self.yugabyte.curr = MagicMock()
        self.yugabyte._snapshot_ht = 123456789

        with patch.object(
            self.yugabyte, 'get_table_columns', return_value=table_columns
        ), patch.object(tap_yugabyte.split_gzip, 'open', return_value=io.BytesIO()):
            self.yugabyte.copy_table('public.my_table', 'unused.csv')

        self.yugabyte.curr.execute.assert_called_once_with(
            "SET yb_read_time TO '123456789 ht'"
        )
        export_sql = self.yugabyte.curr.copy_expert.call_args.args[0]
        self.assertIn('FROM public."my_table"', export_sql)

    def test_unpin_snapshot_clears_slot_boundary(self):
        """A bounded PartialSync must not export at a slot boundary it never saves"""
        self.yugabyte._snapshot_ht = 123456789

        self.yugabyte.unpin_snapshot()

        self.assertIsNone(self.yugabyte._snapshot_ht)

    def test_copy_table_skips_snapshot_pin_when_hybrid_time_is_unknown(self):
        """copy_table must not attempt to set yb_read_time for FULL_TABLE/non-CDC syncs"""
        table_columns = [{'safe_sql_value': '"id"'}]
        self.yugabyte.curr = MagicMock()

        with patch.object(
            self.yugabyte, 'get_table_columns', return_value=table_columns
        ), patch.object(tap_yugabyte.split_gzip, 'open', return_value=io.BytesIO()):
            self.yugabyte.copy_table('public.my_table', 'unused.csv')

        self.yugabyte.curr.execute.assert_not_called()

    def test_copy_table_raises_if_table_not_found(self):
        """An empty column list means the table does not exist"""
        self.yugabyte.curr = MagicMock()

        with patch.object(self.yugabyte, 'get_table_columns', return_value=[]):
            with self.assertRaises(Exception) as context:
                self.yugabyte.copy_table('public.missing_table', 'unused.csv')

        self.assertEqual('public.missing_table table not found.', str(context.exception))

    def test_copy_table_mogrifies_only_the_structured_boundary(self):
        """COPY keeps projection percent signs outside placeholder parsing."""
        table_columns = [{
            0: 'rate%s',
            1: 'text',
            2: 'to_char("event_date", \'%Y-%m-01\')',
            3: None,
            'safe_sql_value': 'to_char("event_date", \'%Y-%m-01\')',
        }]
        self.yugabyte.curr = MagicMock()
        self.yugabyte.curr.connection.encoding = 'UTF8'
        self.yugabyte.curr.mogrify.return_value = (
            b' WHERE "rate%s" >= \'x\\\'\' OR 1=1 --\''
        )
        boundary = PartialSyncBoundary('rate%s', "x' OR 1=1 --")

        with patch.object(
            self.yugabyte, 'get_table_columns', return_value=table_columns
        ), patch.object(
            tap_yugabyte.split_gzip, 'open', return_value=io.BytesIO()
        ):
            self.yugabyte.copy_table(
                'public.my_table', 'unused.csv', boundary=boundary
            )

        self.yugabyte.curr.mogrify.assert_called_once_with(
            ' WHERE "rate%%s" >= %s',
            ("x' OR 1=1 --",),
        )
        export_sql = self.yugabyte.curr.copy_expert.call_args.args[0]
        self.assertIn('to_char("event_date", \'%Y-%m-01\')', export_sql)
        self.assertIn(
            ' WHERE "rate%s" >= \'x\\\'\' OR 1=1 --\'', export_sql
        )

    def test_copy_table_without_boundary_omits_where_clause(self):
        """No boundary means the exported COPY statement has no WHERE clause."""
        table_columns = [{'safe_sql_value': '"id"'}]
        self.yugabyte.curr = MagicMock()

        with patch.object(
            self.yugabyte, 'get_table_columns', return_value=table_columns
        ), patch.object(tap_yugabyte.split_gzip, 'open', return_value=io.BytesIO()):
            self.yugabyte.copy_table('public.my_table', 'unused.csv')

        self.yugabyte.curr.mogrify.assert_not_called()
        export_sql = self.yugabyte.curr.copy_expert.call_args.args[0]
        self.assertNotIn('WHERE', export_sql)

    def test_copy_table_retries_on_transient_mismatched_schema_and_succeeds(self):
        """A transient MISMATCHED_SCHEMA catalog-version race must be retried, not fatal"""
        table_columns = [{'safe_sql_value': '"id"'}]
        self.yugabyte.curr = MagicMock()
        self.yugabyte._snapshot_ht = 123456789
        mismatched_schema_error = psycopg2.errors.InternalError_(
            'The catalog snapshot used for this transaction has been invalidated: '
            'expected: 1044, got: 1042: MISMATCHED_SCHEMA'
        )
        self.yugabyte.curr.copy_expert.side_effect = [mismatched_schema_error, None]

        with patch.object(
            self.yugabyte, 'get_table_columns', return_value=table_columns
        ), patch.object(
            tap_yugabyte.split_gzip, 'open', side_effect=lambda *args, **kwargs: io.BytesIO()
        ), patch.object(tap_yugabyte.time, 'sleep') as sleep_mock:
            self.yugabyte.copy_table('public.my_table', 'unused.csv')

        self.assertEqual(2, self.yugabyte.curr.copy_expert.call_count)
        self.assertEqual(2, self.yugabyte.curr.execute.call_count)
        sleep_mock.assert_called_once()

    def test_copy_table_gives_up_after_persistent_mismatched_schema(self):
        """Persistent MISMATCHED_SCHEMA must exhaust retries and propagate"""
        table_columns = [{'safe_sql_value': '"id"'}]
        self.yugabyte.curr = MagicMock()
        self.yugabyte._snapshot_ht = 123456789
        mismatched_schema_error = psycopg2.errors.InternalError_(
            'The catalog snapshot used for this transaction has been invalidated: '
            'expected: 1044, got: 1042: MISMATCHED_SCHEMA'
        )
        self.yugabyte.curr.copy_expert.side_effect = mismatched_schema_error

        with patch.object(
            self.yugabyte, 'get_table_columns', return_value=table_columns
        ), patch.object(
            tap_yugabyte.split_gzip, 'open', side_effect=lambda *args, **kwargs: io.BytesIO()
        ), patch.object(tap_yugabyte.time, 'sleep') as sleep_mock:
            with self.assertRaises(psycopg2.errors.InternalError_):
                self.yugabyte.copy_table('public.my_table', 'unused.csv')

        self.assertEqual(
            yb_retry.DEFAULT_MAX_ATTEMPTS,
            self.yugabyte.curr.copy_expert.call_count,
        )
        self.assertEqual(
            yb_retry.DEFAULT_MAX_ATTEMPTS - 1,
            sleep_mock.call_count,
        )

    def test_copy_table_does_not_retry_permanent_errors(self):
        """A permanent SQLSTATE must fail on the first attempt without retrying"""
        table_columns = [{'safe_sql_value': '"id"'}]
        self.yugabyte.curr = MagicMock()
        self.yugabyte._snapshot_ht = 123456789
        self.yugabyte.curr.copy_expert.side_effect = _InsufficientPrivilege('permission denied for yb_read_time')

        with patch.object(
            self.yugabyte, 'get_table_columns', return_value=table_columns
        ), patch.object(
            tap_yugabyte.split_gzip, 'open', side_effect=lambda *args, **kwargs: io.BytesIO()
        ), patch.object(tap_yugabyte.time, 'sleep') as sleep_mock:
            with self.assertRaises(yb_retry.PermanentSourceError) as raised:
                self.yugabyte.copy_table('public.my_table', 'unused.csv')

        self.assertIsInstance(raised.exception.__cause__, _InsufficientPrivilege)
        self.assertEqual(1, self.yugabyte.curr.copy_expert.call_count)
        sleep_mock.assert_not_called()

    def test_export_source_table_data_delegates_to_copy_table(self):
        """export_source_table_data builds the export path and forwards the boundary."""
        args = Mock()
        args.table = 'public.my_table'
        args.temp_dir = '/tmp/exports'
        args.target = {
            'split_large_files': False,
            'split_file_chunk_size_mb': 1000,
            'split_file_max_chunks': 20,
        }
        boundary = PartialSyncBoundary('id', 1)

        with patch.object(
            tap_yugabyte.utils, 'gen_export_filename', return_value='export_file.csv'
        ), patch.object(self.yugabyte, 'copy_table') as copy_table_mock, patch.object(
            tap_yugabyte.glob, 'glob', return_value=['/tmp/exports/export_file.csv']
        ) as glob_mock:
            file_parts = self.yugabyte.export_source_table_data(
                args, 'test_tap', boundary=boundary
            )

        copy_table_mock.assert_called_once_with(
            'public.my_table',
            '/tmp/exports/export_file.csv',
            split_large_files=False,
            split_file_chunk_size_mb=1000,
            split_file_max_chunks=20,
            boundary=boundary,
        )
        glob_mock.assert_called_once_with('/tmp/exports/export_file.csv*')
        self.assertEqual(['/tmp/exports/export_file.csv'], file_parts)

    def test_get_table_columns_hstore_projection_depends_on_hstore_as_json(self):
        """hstore_as_json toggles the hstore_to_json projection in generated SQL"""
        self.yugabyte.hstore_as_json = True
        with patch.object(self.yugabyte, 'query', return_value=[]) as query_mock:
            self.yugabyte.get_table_columns('public.hstore_table', max_num='1')

        query = query_mock.call_args.args[0]
        self.assertIn("WHEN udt_name = 'hstore' THEN 'hstore'", query)
        self.assertIn("WHEN udt_name = 'hstore' THEN 'hstore_to_json", query)

    def test_get_table_columns_without_hstore_as_json(self):
        """Without hstore_as_json the hstore_to_json projection must not be emitted"""
        with patch.object(self.yugabyte, 'query', return_value=[]) as query_mock:
            self.yugabyte.get_table_columns('public.hstore_table', max_num='1')

        query = query_mock.call_args.args[0]
        self.assertNotIn('hstore_to_json', query)

    def test_get_primary_keys_preserve_declared_order(self):
        """Composite keys follow index order rather than physical column order"""
        with patch.object(
            self.yugabyte,
            'query',
            return_value=[('second_key',), ('first_key',)],
        ) as query_mock:
            keys = self.yugabyte.get_primary_keys('public.composite_key')

        self.assertEqual(keys, ['"SECOND_KEY"', '"FIRST_KEY"'])
        query_mock.assert_called_once()
        self.assertEqual(query_mock.call_args.args[1], ('public', 'composite_key'))
        self.assertIn('WITH ORDINALITY', query_mock.call_args.args[0])
        self.assertIn('ORDER BY key_column.key_ordinality', query_mock.call_args.args[0])

    def test_get_primary_keys_returns_none_if_no_pk(self):
        """No matching rows means the table has no primary key"""
        with patch.object(self.yugabyte, 'query', return_value=[]):
            keys = self.yugabyte.get_primary_keys('public.no_pk_table')

        self.assertIsNone(keys)


class _InsufficientPrivilege(psycopg2.Error):
    """psycopg2's pgcode is read-only, so a permanent SQLSTATE is shadowed on a subclass."""
    pgcode = '42501'


def _psycopg2_error(message):
    """Build a psycopg2.Error carrying the given message for retry-path tests."""
    return psycopg2.Error(message)
