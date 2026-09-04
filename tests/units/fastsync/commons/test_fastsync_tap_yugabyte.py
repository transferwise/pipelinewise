import datetime
import io

import psycopg2

from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

from pipelinewise.fastsync.commons.tap_yugabyte import FastSyncTapYugabyte
from pipelinewise.fastsync.commons import tap_yugabyte


class TestFastSyncTapYugabyte(TestCase):  # pylint: disable=too-many-public-methods
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

    def test_fetch_current_log_pos_returns_hybrid_time_boundary(self):
        """fetch_current_log_pos must read yb_restart_commit_ht, not the placeholder lsn"""
        with patch.object(
            self.yugabyte, 'create_replication_slot'
        ) as create_replication_slot, patch.object(
            self.yugabyte, 'query', return_value=[{'yb_restart_commit_ht': 123456789}]
        ) as query_mock:
            bookmark = self.yugabyte.fetch_current_log_pos()

        self.assertEqual({'lsn': 123456789, 'version': 1}, bookmark)
        create_replication_slot.assert_called_once_with()
        query_mock.assert_called_once_with(
            "SELECT yb_restart_commit_ht FROM pg_replication_slots WHERE "
            "slot_name = 'pipelinewise_test_database_test_tap'"
        )
        self.assertEqual(123456789, self.yugabyte._snapshot_ht)  # pylint: disable=protected-access

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
        """test fetch_current_incremental_key_pos where result is decimal, it should return a float key value"""
        with patch.object(self.yugabyte, 'query') as query_mock:
            query_mock.return_value = [{'key_value': Decimal(4.222222222)}]

            state = self.yugabyte.fetch_current_incremental_key_pos('schema.table1', 'id')

            self.assertDictEqual(
                {
                    'replication_key': 'id',
                    'replication_key_value': 4.222222222,
                    'version': 1,
                },
                state,
            )

    def test_get_connection_builds_conn_string(self):
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
            "host='my_host' port='my_port' user='my_user' password='my_password' dbname='my_db'"
        )
        self.assertTrue(connection.autocommit)

    def test_get_connection_with_ssl(self):
        """get_connection must append sslmode=require when ssl is requested"""
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
            "host='my_host' port='my_port' user='my_user' password='my_password' dbname='my_db' "
            "sslmode='require'"
        )

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

    def test_copy_table_pins_snapshot_when_hybrid_time_is_known(self):
        """copy_table must pin yb_read_time to the captured snapshot boundary before exporting"""
        table_columns = [{'safe_sql_value': '"id"'}]
        self.yugabyte.curr = MagicMock()
        self.yugabyte._snapshot_ht = 123456789  # pylint: disable=protected-access

        with patch.object(
            self.yugabyte, 'get_table_columns', return_value=table_columns
        ), patch.object(tap_yugabyte.split_gzip, 'open', return_value=io.BytesIO()):
            self.yugabyte.copy_table('public.my_table', 'unused.csv')

        self.yugabyte.curr.execute.assert_called_once_with(
            "SET yb_read_time TO '123456789 ht'"
        )
        export_sql = self.yugabyte.curr.copy_expert.call_args.args[0]
        self.assertIn('FROM public."my_table"', export_sql)

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


def _psycopg2_error(message):
    """Build a psycopg2.Error carrying the given message for retry-path tests."""
    return psycopg2.Error(message)
