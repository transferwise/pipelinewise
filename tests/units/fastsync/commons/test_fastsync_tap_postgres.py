import datetime

from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, Mock, PropertyMock, patch

from pipelinewise.fastsync.commons.tap_postgres import FastSyncTapPostgres


class TestFastSyncTapPostgres(TestCase):
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

        self.assertTrue(connect_mock.autocommit)

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_to_sec(self, connect_mock):
        """
        Check that get connection uses the right credentials to connect to secondary if present
        """
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

        self.assertTrue(connect_mock.autocommit)

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_fallback(self, connect_mock):
        """
        Check that get connection uses the primary server credentials as a fallback
        """
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

        self.assertTrue(connect_mock.autocommit)

    @patch('pipelinewise.fastsync.commons.tap_postgres.psycopg2.connect')
    def test_get_connection_ssl(self, connect_mock):
        """
        Check that get connection uses ssl when present
        """
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

        self.assertTrue(connect_mock.autocommit)

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

            with self.assertRaises(Exception) as cm:
                self.postgres.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertEqual('Cannot get replication key value for table: schema.table1', str(cm.exception))

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
