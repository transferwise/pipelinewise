from unittest import TestCase
from unittest.mock import MagicMock, patch

from tap_postgres.sync_strategies.full_table import sync_view
from tap_postgres.sync_strategies import full_table

from tests.utils import MockedConnect


class TestFullTable(TestCase):
    """Test Cases for full_table"""

    @classmethod
    def setUpClass(cls) -> None:
        super(TestFullTable, cls).setUpClass()
        cls.patcher = patch('psycopg2.connect')
        mocked_connect = cls.patcher.start()
        mocked_connect.return_value.server_version = 140018
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
            'use_secondary': False
        }

    def test_sync_view(self):
        """Test for assuring sync_view works as expected"""
        stream = {
            'tap_stream_id': 'foo-bar',
            'schema': {'properties': {'foo_desired': 'b'}},
            'stream': 'test',
            'table_name': 'table_name_value',
            'metadata': [{
                'metadata': {'sql-datatype': 'test', 'schema-name': 'schema_name_value'},
                'breadcrumb': ["properties", "foo_desired"],
            }]
        }
        state = {'bookmarks': {'foo-bar': {'foo': 'bar', 'lsn': 4}}}
        desired_columns = ['foo', 'bar']
        md_map = {(): {'schema-name': 'pg_catalog', 'replication-key': 'oid'},
                  ('properties', 'foo'): {'sql-datatype': 'foo'},
                  ('properties', 'bar'): {'sql-datatype': 'foo'}}

        mocked_time_value = 1234
        expected_output_without_version = {
            'bookmarks': {'foo-bar': {'foo': 'bar', 'lsn': 4, 'version': mocked_time_value * 1000}}
        }
        with patch('time.time') as mocked_time:
            mocked_time.return_value = mocked_time_value
            actual_output = sync_view(self.conn_config, stream, state, desired_columns, md_map)
            self.assertEqual(expected_output_without_version, actual_output)

    @patch('tap_postgres.sync_strategies.full_table.post_db.open_connection')
    @patch('tap_postgres.sync_strategies.logical_replication.wait_for_replica_replay')
    @patch('tap_postgres.sync_strategies.full_table._sync_table')
    def test_replica_wait_uses_the_snapshot_connection(self, snapshot, wait, connect):
        connection = connect.return_value
        calls = MagicMock()
        calls.attach_mock(wait, 'wait')
        calls.attach_mock(snapshot, 'snapshot')

        stream = {'tap_stream_id': 'test'}
        state = {}
        full_table.sync_table(self.conn_config, stream, state, [], {}, snapshot_lsn=123)

        wait.assert_called_once_with(self.conn_config, 123, connection=connection)
        snapshot.assert_called_once_with(self.conn_config, connection, stream, state, [], {})
        self.assertEqual(state, {'bookmarks': {'test': {'lsn': 123}}})
        self.assertEqual(['wait', 'snapshot'], [call[0] for call in calls.mock_calls])
        connection.close.assert_called_once_with()

    @patch('tap_postgres.sync_strategies.full_table.post_db.open_connection')
    @patch('tap_postgres.sync_strategies.logical_replication.wait_for_replica_replay')
    @patch('tap_postgres.sync_strategies.full_table._sync_table')
    def test_replica_timeout_does_not_emit_snapshot_or_state(self, snapshot, wait, connect):
        wait.side_effect = TimeoutError('replica has not caught up')
        with self.assertRaises(TimeoutError):
            full_table.sync_table(self.conn_config, {}, {}, [], {}, snapshot_lsn=123)
        snapshot.assert_not_called()
        connect.return_value.close.assert_called_once_with()
