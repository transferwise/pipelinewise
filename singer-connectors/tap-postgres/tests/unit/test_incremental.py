from unittest import TestCase
from unittest.mock import patch

import singer

from tests.utils import MockedConnect

from tap_postgres.sync_strategies import incremental


class TestIncremental(TestCase):
    """Test Cases for Incremental"""

    @classmethod
    def setUpClass(cls) -> None:
        super(TestIncremental, cls).setUpClass()
        cls.patcher = patch('psycopg2.connect')
        mocked_connect = cls.patcher.start()
        mocked_connect.return_value.__enter__.return_value = MockedConnect()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.patcher.stop()

    def setUp(self) -> None:
        self.conn_config = {
            'host': 'foo',
            'dbname': 'foo_db',
            'user': 'foo_user',
            'password': 'foo_pass',
            'port': 12345,
            'use_secondary': False,
            'limit': None
        }
        self.stream = {'tap_stream_id': 5, 'stream': 'bar', 'table_name': 'pg_tbl'}
        self.md_map = {
            (): {'schema-name': 'pg_catalog', 'replication-key': 'foo_key'},
            ('properties', 'foo_key'): {'sql-datatype': 'foo'},
            ('properties', 'bar_column'): {'sql-datatype': 'foo'}
        }
        self.state = {'bookmarks': {self.stream['tap_stream_id']: {'version': 1, 'replication_key_value': 'foo'}}}

    def test_fetch_max_replication_key(self):
        """Test if fetch_max_replication works correctly"""
        expected_max_key = MockedConnect.cursor.fetchone_return_value[0]
        replication_key = 'foo_key'
        schema_name = 'foo_schema'
        table_name = 'foo_table'

        actual_max_key = incremental.fetch_max_replication_key(self.conn_config,
                                                               replication_key, schema_name, table_name)
        self.assertEqual(expected_max_key, actual_max_key)

    @patch("psycopg2.extras.register_hstore")
    def test_sync_table(self, mocked_register_hstore):
        """Test for sync_table if it works correctly"""
        desired_columns = ['foo_key']
        self.state['bookmarks'] = {}
        expected_state_replication_key_value = MockedConnect.cursor.return_value
        actual_state = incremental.sync_table(self.conn_config, self.stream, self.state, desired_columns, self.md_map)
        mocked_register_hstore.assert_called()

        self.assertEqual(expected_state_replication_key_value,
                         actual_state['bookmarks'][self.stream['tap_stream_id']]['replication_key_value'],
                         )

    @patch('tap_postgres.sync_strategies.incremental.post_db.hstore_available')
    @patch('psycopg2.extras.register_hstore')
    def test_sync_table_if_not_hstore_available(self, _, mocked_hstore_available):
        """Test for sync_table_ if hstore is unavailable"""
        desired_columns = ['foo_key']
        expected_state_replication_key_value = MockedConnect.cursor.return_value
        mocked_hstore_available.return_value = False
        actual_state = incremental.sync_table(self.conn_config, self.stream, self.state, desired_columns, self.md_map)

        self.assertEqual(expected_state_replication_key_value,
                         actual_state['bookmarks'][self.stream['tap_stream_id']]['replication_key_value'])

    @patch("tap_postgres.sync_strategies.incremental.singer.write_message")
    @patch("psycopg2.extras.register_hstore")
    def test_sync_table_if_rows_saved_is_a_multiply_of_update_bookmark_period(self,
                                                                              mocked_register_hstore,
                                                                              mocked_singer_write):
        """Test for sync_table if rows_saved is a multiply of UPDATE_BOOKMARK_PERION"""
        original_update_bookmark_period = incremental.UPDATE_BOOKMARK_PERIOD
        incremental.UPDATE_BOOKMARK_PERIOD = MockedConnect.cursor.counter_limit - 1
        desired_columns = ['foo_key']
        expected_state_replication_key_value = MockedConnect.cursor.return_value
        actual_state = incremental.sync_table(self.conn_config,
                                              self.stream,
                                              self.state,
                                              desired_columns,
                                              self.md_map)
        mocked_register_hstore.assert_called()
        self.assertEqual(expected_state_replication_key_value,
                         actual_state['bookmarks'][self.stream['tap_stream_id']]['replication_key_value'],
                         )
        incremental.UPDATE_BOOKMARK_PERIOD = original_update_bookmark_period
        mocked_singer_write.assert_called_with(singer.StateMessage(value=self.state))
