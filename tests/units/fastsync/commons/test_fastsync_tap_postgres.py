import datetime
import io

from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, Mock, PropertyMock, call, patch

from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres
from pipelinewise.fastsync.commons import tap_postgres
from pipelinewise.fastsync.commons.partial_sync_boundary import (
    PartialSyncBoundary,
)


class TestFastSyncTapPostgres(TestCase):  # pylint: disable=too-many-public-methods
    """
    Unit tests for fastsync tap postgres
    """

    def setUp(self) -> None:
        """Initialise test FastSyncTapPostgres object"""
        self.postgres = FastSyncTapPostgres(
            connection_config={'dbname': 'test_database', 'tap_id': 'test_tap'},
            tap_type_to_target_type={},
        )
        self.postgres.executed_queries_primary_host = []
        self.postgres.executed_queries = []

        def primary_host_query_mock(query, _=None):
            self.postgres.executed_queries_primary_host.append(query)

        self.postgres.primary_host_query = primary_host_query_mock

    def test_copy_table_mogrifies_only_the_structured_boundary(self):
        """COPY keeps projection percent signs outside placeholder parsing."""
        table_columns = [{
            0: 'rate%s',
            1: 'text',
            2: 'to_char("event_date", \'%Y-%m-01\')',
            3: None,
            'safe_sql_value': 'to_char("event_date", \'%Y-%m-01\')',
        }]
        self.postgres.curr = MagicMock()
        self.postgres.curr.connection.encoding = 'UTF8'
        self.postgres.curr.mogrify.return_value = (
            b' WHERE "rate%s" >= \'x\\\'\' OR 1=1 --\''
        )
        boundary = PartialSyncBoundary('rate%s', "x' OR 1=1 --")

        with patch.object(
            self.postgres, 'get_table_columns', return_value=table_columns
        ), patch.object(
            tap_postgres.split_gzip, 'open', return_value=io.BytesIO()
        ):
            self.postgres.copy_table(
                'public.my_table', 'unused.csv', boundary=boundary
            )

        self.postgres.curr.mogrify.assert_called_once_with(
            ' WHERE "rate%%s" >= %s',
            ("x' OR 1=1 --",),
        )
        export_sql = self.postgres.curr.copy_expert.call_args.args[0]
        self.assertIn('to_char("event_date", \'%Y-%m-01\')', export_sql)
        self.assertIn(
            ' WHERE "rate%s" >= \'x\\\'\' OR 1=1 --\'', export_sql
        )

    def test_generate_repl_slot_name(self):
        """Validate if the replication slot name generated correctly"""
        # Provide only database name
        assert (
            self.postgres.generate_replication_slot_name('some_db')
            == 'pipelinewise_some_db'
        )

        # Provide database name and tap_id
        assert (
            self.postgres.generate_replication_slot_name('some_db', 'some_tap')
            == 'pipelinewise_some_db_some_tap'
        )

        # Provide database name, tap_id and prefix
        assert (
            self.postgres.generate_replication_slot_name(
                'some_db', 'some_tap', prefix='custom_prefix'
            )
            == 'custom_prefix_some_db_some_tap'
        )

        # Replication slot name should be lowercase
        assert (
            self.postgres.generate_replication_slot_name('SoMe_DB', 'SoMe_TaP')
            == 'pipelinewise_some_db_some_tap'
        )

        # Invalid characters should be replaced by underscores
        assert (
            self.postgres.generate_replication_slot_name('some-db', 'some-tap')
            == 'pipelinewise_some_db_some_tap'
        )

        assert (
            self.postgres.generate_replication_slot_name('some.db', 'some.tap')
            == 'pipelinewise_some_db_some_tap'
        )

    def test_close_connection_is_idempotent(self):
        """Close each opened connection once and clear its cursor reference."""
        connection = Mock()
        primary_connection = Mock()
        self.postgres.conn = connection
        self.postgres.curr = Mock()
        self.postgres.primary_host_conn = primary_connection

        self.postgres.close_connection()
        self.postgres.close_connection()

        connection.close.assert_called_once_with()
        primary_connection.close.assert_called_once_with()
        self.assertIsNone(self.postgres.conn)
        self.assertIsNone(self.postgres.curr)
        self.assertIsNone(self.postgres.primary_host_conn)

    def test_close_connection_silences_driver_failure(self):
        """Cleanup failures must not replace the original sync exception."""
        connection = Mock()
        connection.close.side_effect = RuntimeError('close failed')
        self.postgres.conn = connection

        self.postgres.close_connection(silent=True)

        connection.close.assert_called_once_with()
        self.assertIsNone(self.postgres.conn)

    def test_fetch_current_log_pos_closes_primary_connection_on_success(self):
        """The dedicated primary connection is released before reading the source LSN."""
        primary_connection = Mock()

        with patch.object(
            self.postgres, 'get_connection', return_value=primary_connection
        ), patch.object(
            self.postgres, 'create_replication_slot'
        ) as create_replication_slot, patch.object(
            self.postgres, 'query', return_value=[{'current_lsn': '0/2A'}]
        ) as query:
            bookmark = self.postgres.fetch_current_log_pos()

        self.assertEqual({'lsn': 42, 'version': 1}, bookmark)
        create_replication_slot.assert_called_once_with()
        query.assert_called_once_with('SELECT pg_current_wal_lsn() AS current_lsn')
        primary_connection.close.assert_called_once_with()
        self.assertIsNone(self.postgres.primary_host_conn)

    def test_fetch_current_log_pos_uses_current_wal_function_on_replica(self):
        """Supported replicas use the current WAL function name."""
        self.postgres.connection_config['replica_host'] = 'replica'
        primary_connection = Mock()

        with patch.object(
            self.postgres, 'get_connection', return_value=primary_connection
        ), patch.object(
            self.postgres, 'create_replication_slot'
        ), patch.object(
            self.postgres, 'query', return_value=[{'current_lsn': '0/2A'}]
        ) as query:
            bookmark = self.postgres.fetch_current_log_pos()

        self.assertEqual({'lsn': 42, 'version': 1}, bookmark)
        query.assert_called_once_with('SELECT pg_last_wal_replay_lsn() AS current_lsn')

    def test_fetch_current_log_pos_closes_primary_connection_on_failure(self):
        """A replication-slot failure cannot leak the primary connection."""
        primary_connection = Mock()

        with patch.object(
            self.postgres, 'get_connection', return_value=primary_connection
        ), patch.object(
            self.postgres,
            'create_replication_slot',
            side_effect=RuntimeError('replication slot failed'),
        ), self.assertRaisesRegex(RuntimeError, 'replication slot failed'):
            self.postgres.fetch_current_log_pos()

        primary_connection.close.assert_called_once_with()
        self.assertIsNone(self.postgres.primary_host_conn)

    def test_create_replication_slot_1(self):
        """
        Validate if replication slot creation SQL commands generated correctly in case no v15 slots exists
        """

        def execute_mock(query):
            print('Mocked execute called')
            self.postgres.executed_queries_primary_host.append(query)

        # mock cursor with execute method
        cursor_mock = MagicMock().return_value
        cursor_mock.__enter__.return_value.execute.side_effect = execute_mock
        type(cursor_mock.__enter__.return_value).rowcount = PropertyMock(return_value=0)

        # mock PG connection instance with ability to open cursor
        pg_con = Mock()
        pg_con.cursor.return_value = cursor_mock

        self.postgres.primary_host_conn = pg_con

        self.postgres.create_replication_slot()
        assert self.postgres.executed_queries_primary_host == [
            "SELECT * FROM pg_replication_slots WHERE slot_name = 'pipelinewise_test_database';",
            "SELECT * FROM pg_create_logical_replication_slot('pipelinewise_test_database_test_tap', 'wal2json')",
        ]

    def test_create_replication_slot_2(self):
        """
        Validate if replication slot creation SQL commands generated correctly in case a v15 slots exists
        """

        def execute_mock(query):
            print('Mocked execute called')
            self.postgres.executed_queries_primary_host.append(query)

        # mock cursor with execute method
        cursor_mock = MagicMock().return_value
        cursor_mock.__enter__.return_value.execute.side_effect = execute_mock
        type(cursor_mock.__enter__.return_value).rowcount = PropertyMock(return_value=1)

        # mock PG connection instance with ability to open cursor
        pg_con = Mock()
        pg_con.cursor.return_value = cursor_mock

        self.postgres.primary_host_conn = pg_con

        self.postgres.create_replication_slot()
        assert self.postgres.executed_queries_primary_host == [
            "SELECT * FROM pg_replication_slots WHERE slot_name = 'pipelinewise_test_database';",
            "SELECT * FROM pg_create_logical_replication_slot('pipelinewise_test_database', 'wal2json')",
        ]

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_to_primary(self, connect_mock):
        """
        Check that get connection uses the right credentials to connect to primary
        """
        connect_mock.return_value.server_version = 110002
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
        }

        self.assertEqual(
            FastSyncTapPostgres.get_connection(creds, prioritize_primary=True),
            connect_mock.return_value,
        )

        connect_mock.assert_called_once_with(
            f"host='{creds['host']}' port='{creds['port']}' user='{creds['user']}' password='{creds['password']}' "
            f"dbname='{creds['dbname']}'"
        )

        self.assertTrue(connect_mock.return_value.autocommit)

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_rejects_postgres_before_11_2(self, connect_mock):
        """Every FastSync source connection enforces the PostgreSQL floor."""
        connect_mock.return_value.server_version = 110001
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
        }

        with self.assertRaisesRegex(
            RuntimeError,
            'PostgreSQL 11.2 or later.*server_version_num 110001',
        ):
            FastSyncTapPostgres.get_connection(creds, prioritize_primary=True)

        connect_mock.return_value.close.assert_called_once_with()

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_allows_unsupported_version_for_config_removal(
        self, connect_mock
    ):
        """Removed-tap cleanup can still connect to an obsolete source."""
        connect_mock.return_value.server_version = 110001
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
        }

        connection = FastSyncTapPostgres.get_connection(
            creds,
            prioritize_primary=True,
            allow_unsupported_version_for_config_removal=True,
        )

        self.assertIs(connect_mock.return_value, connection)
        connect_mock.return_value.close.assert_not_called()
        self.assertTrue(connection.autocommit)

    def test_drop_slot_forwards_only_the_config_removal_bypass(self):
        """Slot cleanup forwards the explicit removed-config exception."""
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value.rowcount = 0
        creds = {
            'dbname': 'my_db',
            'tap_id': 'my_tap',
        }

        with patch.object(
            FastSyncTapPostgres, 'get_connection', return_value=connection
        ) as get_connection:
            FastSyncTapPostgres.drop_slot(
                creds,
                allow_unsupported_version_for_config_removal=True,
            )

        get_connection.assert_called_once_with(
            creds,
            prioritize_primary=True,
            allow_unsupported_version_for_config_removal=True,
        )
        connection.close.assert_called_once_with()

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_drop_slot_validates_version_by_default(self, connect_mock):
        """FastSync resync slot cleanup must retain source-version validation."""
        connect_mock.return_value.server_version = 110001
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'tap_id': 'my_tap',
        }

        with self.assertRaisesRegex(RuntimeError, 'PostgreSQL 11.2 or later'):
            FastSyncTapPostgres.drop_slot(creds)

        connect_mock.return_value.close.assert_called_once_with()
        connect_mock.return_value.cursor.assert_not_called()

    def test_reset_slot_drops_only_current_slot_after_state_invalidation(self):
        """Only the preflighted tap-specific slot is dropped, after state is durable."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False)]
        creds = {'dbname': 'my_db', 'tap_id': 'my_tap'}
        before_reset = MagicMock(return_value='state.backup')
        calls = MagicMock()
        calls.attach_mock(cursor.execute, 'execute')
        calls.attach_mock(before_reset, 'before_reset')

        with patch.object(
            FastSyncTapPostgres, 'get_connection', return_value=connection
        ) as get_connection:
            FastSyncTapPostgres.reset_slot(creds, before_reset=before_reset)

        get_connection.assert_called_once_with(creds, prioritize_primary=True)
        assert calls.mock_calls == [
            call.execute(
                'SELECT slot_name, database, plugin, active FROM pg_replication_slots '
                'WHERE slot_name IN (%s, %s)',
                ('pipelinewise_my_db', 'pipelinewise_my_db_my_tap'),
            ),
            call.before_reset(),
            call.execute('SELECT pg_drop_replication_slot(%s)', ('pipelinewise_my_db_my_tap',)),
            call.execute(
                'SELECT * FROM pg_create_logical_replication_slot(%s, %s)',
                ('pipelinewise_my_db_my_tap', 'wal2json'),
            ),
        ]
        connection.close.assert_called_once_with()

    def test_reset_slot_rejects_legacy_active_and_incompatible_slots_before_state_changes(self):
        """Legacy lookup takes precedence even when an inactive tap-specific slot also exists."""
        current = ('pipelinewise_my_db_my_tap', 'my_db', 'wal2json', False)
        cases = [
            ([('pipelinewise_my_db', 'my_db', 'wal2json', active)] + modern, 'legacy')
            for active in (False, True) for modern in ([], [current])
        ] + [
            ([(current[0], database, plugin, active)], 'must be inactive')
            for database, plugin, active in (
                ('my_db', 'wal2json', True), ('other_db', 'wal2json', False), ('my_db', 'pgoutput', False),
            )
        ]
        for rows, message in cases:
            with self.subTest(rows=rows):
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = rows
                before_reset = MagicMock()
                with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
                    with self.assertRaisesRegex(RuntimeError, message):
                        FastSyncTapPostgres.reset_slot(
                            {'dbname': 'my_db', 'tap_id': 'my_tap'}, before_reset=before_reset,
                        )
                before_reset.assert_not_called()
                self.assertEqual(cursor.execute.call_count, 1)
                connection.close.assert_called_once_with()

    def test_reset_slot_creates_missing_slot_without_dropping_any_slot(self):
        """A missing slot still requires state invalidation before creation."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []
        before_reset = MagicMock(return_value=None)
        with patch.object(FastSyncTapPostgres, 'get_connection', return_value=connection):
            FastSyncTapPostgres.reset_slot({'dbname': 'my_db', 'tap_id': 'my_tap'}, before_reset=before_reset)
        before_reset.assert_called_once_with()
        self.assertEqual(cursor.execute.call_count, 2)
        cursor.execute.assert_called_with(
            'SELECT * FROM pg_create_logical_replication_slot(%s, %s)', ('pipelinewise_my_db_my_tap', 'wal2json'),
        )
        connection.close.assert_called_once_with()

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_to_sec(self, connect_mock):
        """
        Check that get connection uses the right credentials to connect to secondary if present
        """
        connect_mock.return_value.server_version = 110002
        creds = {
            'host': 'my_primary_host',
            'replica_host': 'my_replica_host',
            'user': 'my_primary_user',
            'replica_user': 'my_replica_user',
            'password': 'my_primary_user',
            'replica_password': 'my_replica_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'replica_port': 'my_replica_port',
        }

        self.assertEqual(
            FastSyncTapPostgres.get_connection(creds, prioritize_primary=False),
            connect_mock.return_value,
        )

        connect_mock.assert_called_once_with(
            f"host='{creds['replica_host']}' port='{creds['replica_port']}' user='{creds['replica_user']}' password"
            f"='{creds['replica_password']}' "
            f"dbname='{creds['dbname']}'"
        )

        self.assertTrue(connect_mock.return_value.autocommit)

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_fallback(self, connect_mock):
        """
        Check that get connection uses the primary server credentials as a fallback
        """
        connect_mock.return_value.server_version = 110002
        creds = {
            'host': 'my_primary_host',
            'replica_host': 'my_replica_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
        }

        self.assertEqual(
            FastSyncTapPostgres.get_connection(creds, prioritize_primary=False),
            connect_mock.return_value,
        )

        connect_mock.assert_called_once_with(
            f"host='{creds['replica_host']}' port='{creds['port']}' user='{creds['user']}' password"
            f"='{creds['password']}' dbname='{creds['dbname']}'"
        )

        self.assertTrue(connect_mock.return_value.autocommit)

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_ssl(self, connect_mock):
        """
        Check that get connection uses ssl when present
        """
        connect_mock.return_value.server_version = 110002
        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'ssl': 'true',
        }

        self.assertEqual(
            FastSyncTapPostgres.get_connection(creds, prioritize_primary=False),
            connect_mock.return_value,
        )

        connect_mock.assert_called_once_with(
            f"host='{creds['host']}' port='{creds['port']}' user='{creds['user']}' password"
            f"='{creds['password']}' dbname='{creds['dbname']}' sslmode='require'"
        )

        self.assertTrue(connect_mock.return_value.autocommit)

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_drop_slot_v15(self, connect_mock):
        """
        Check that dropping slots works fine for v15 slots
        """

        def execute_mock(query):
            print('Mocked execute called')
            self.postgres.executed_queries_primary_host.append(query)

        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'ssl': 'true',
            'tap_id': 'tap_test',
        }

        # mock cursor with execute method
        cursor_mock = MagicMock().return_value
        cursor_mock.__enter__.return_value.execute.side_effect = execute_mock
        type(cursor_mock.__enter__.return_value).rowcount = PropertyMock(
            side_effect=[1, 2]
        )

        # mock PG connection instance with ability to open cursor
        pg_con = Mock()
        pg_con.server_version = 110002
        pg_con.cursor.return_value = cursor_mock

        connect_mock.return_value = pg_con

        self.postgres.drop_slot(creds)

        assert self.postgres.executed_queries_primary_host == [
            "SELECT * FROM pg_replication_slots WHERE slot_name = 'pipelinewise_my_db';",
            'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE '
            "slot_name = 'pipelinewise_my_db';",
        ]

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_drop_slot_v16(self, connect_mock):
        """
        Check that dropping slots works fine for v16 slots
        """

        def execute_mock(query):
            print('Mocked execute called')
            self.postgres.executed_queries_primary_host.append(query)

        creds = {
            'host': 'my_primary_host',
            'user': 'my_primary_user',
            'password': 'my_primary_user',
            'dbname': 'my_db',
            'port': 'my_primary_port',
            'ssl': 'true',
            'tap_id': 'tap_test',
        }

        # mock cursor with execute method
        cursor_mock = MagicMock().return_value
        cursor_mock.__enter__.return_value.execute.side_effect = execute_mock
        type(cursor_mock.__enter__.return_value).rowcount = PropertyMock(
            side_effect=[0, 1]
        )

        # mock PG connection instance with ability to open cursor
        pg_con = Mock()
        pg_con.server_version = 110002
        pg_con.cursor.return_value = cursor_mock

        connect_mock.return_value = pg_con

        self.postgres.drop_slot(creds)

        assert self.postgres.executed_queries_primary_host == [
            "SELECT * FROM pg_replication_slots WHERE slot_name = 'pipelinewise_my_db';",
            'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE '
            "slot_name = 'pipelinewise_my_db_tap_test';",
        ]

    def test_fetch_current_incremental_key_pos_empty_result_expect_exception(self):
        """
        test fetch_current_incremental_key_pos where result is empty, it should raise an exception
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = None

            with self.assertRaises(Exception) as context:
                self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertEqual('Cannot get replication key value for table: schema.table1', str(context.exception))

    def test_primary_keys_preserve_declared_order(self):
        """Composite keys follow index order rather than physical column order."""
        with patch.object(
            self.postgres,
            'query',
            return_value=[('second_key',), ('first_key',)],
        ) as query_mock:
            keys = self.postgres.get_primary_keys('public.composite_key')

        self.assertEqual(keys, ['"SECOND_KEY"', '"FIRST_KEY"'])
        query_mock.assert_called_once()
        self.assertEqual(query_mock.call_args.args[1], ('public', 'composite_key'))
        self.assertIn('WITH ORDINALITY', query_mock.call_args.args[0])
        self.assertIn(
            'ORDER BY key_column.key_ordinality', query_mock.call_args.args[0]
        )

    def test_hstore_is_exported_as_json(self):
        """FastSync and Singer must share object semantics for hstore."""
        self.postgres.hstore_as_json = True
        with patch.object(self.postgres, 'query', return_value=[]) as query_mock:
            self.postgres.get_table_columns('public.hstore_table', max_num='1')

        query = query_mock.call_args.args[0]
        self.assertIn("WHEN udt_name = 'hstore' THEN 'hstore'", query)
        self.assertIn(
            "WHEN udt_name = 'hstore' THEN 'hstore_to_json(\"'",
            query,
        )

    def test_hstore_export_is_unchanged_for_native_routes(self):
        """Native routes retain the existing textual hstore export."""
        with patch.object(self.postgres, 'query', return_value=[]) as query_mock:
            self.postgres.get_table_columns('public.hstore_table', max_num='1')

        query = query_mock.call_args.args[0]
        self.assertNotIn('hstore_to_json', query)

    def test_fetch_current_incremental_key_pos_empty_key_value_return_empty_state(self):
        """
        test fetch_current_incremental_key_pos where result has empty value is empty, it should return an empty state
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = [{}]

            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertFalse(state)

    def test_fetch_current_incremental_key_pos_non_empty_key_value_return_state(self):
        """
        test fetch_current_incremental_key_pos where result exists, it should return a non empty state with key value
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = [{'key_value': 123}]

            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual({
                'replication_key': 'id',
                'replication_key_value': 123,
                'version': 1,
            }, state)

    def test_fetch_current_incremental_key_pos_datetime_key_value_return_state(self):
        """
        test fetch_current_incremental_key_pos where result is datetime, it should return a state with iso formatted
         datetime key value
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = [{'key_value': datetime.datetime(2020, 1, 24, 7, 12, 6)}]

            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual({
                'replication_key': 'id',
                'replication_key_value': '2020-01-24T07:12:06',
                'version': 1,
            }, state)

    def test_fetch_current_incremental_key_pos_date_key_value_return_state(self):
        """
        test fetch_current_incremental_key_pos where result is date, it should return a state with iso formatted
         datetime key value
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = [{'key_value': datetime.date(2020, 1, 24)}]

            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual({
                'replication_key': 'id',
                'replication_key_value': '2020-01-24T00:00:00',
                'version': 1,
            }, state)

    def test_fetch_current_incremental_key_pos_decimal_key_value_return_state(self):
        """
        test fetch_current_incremental_key_pos where result is decimal, it should return a state with float key value
        """
        with patch.object(self.postgres, 'query') as query_mock:
            query_mock.return_value = [{'key_value': Decimal(4.222222222)}]

            state = self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual({
                'replication_key': 'id',
                'replication_key_value': 4.222222222,
                'version': 1,
            }, state)
