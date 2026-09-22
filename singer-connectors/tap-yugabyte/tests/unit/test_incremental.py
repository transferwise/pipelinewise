from unittest import TestCase
from unittest.mock import patch

import singer

from tests.utils import MockedConnect

from tap_yugabyte.sync_strategies import incremental


class TestIncremental(TestCase):
    """Test Cases for Incremental, ported from tap-postgres"""

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
            'limit': None
        }
        self.stream = {'tap_stream_id': 'yb_catalog-yb_tbl', 'stream': 'bar', 'table_name': 'yb_tbl'}
        self.md_map = {
            (): {'schema-name': 'yb_catalog', 'replication-key': 'foo_key'},
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

    @patch('tap_yugabyte.sync_strategies.incremental.yb_db.hstore_available')
    @patch('psycopg2.extras.register_hstore')
    def test_sync_table_if_not_hstore_available(self, _, mocked_hstore_available):
        """Test for sync_table_ if hstore is unavailable"""
        desired_columns = ['foo_key']
        expected_state_replication_key_value = MockedConnect.cursor.return_value
        mocked_hstore_available.return_value = False
        actual_state = incremental.sync_table(self.conn_config, self.stream, self.state, desired_columns, self.md_map)

        self.assertEqual(expected_state_replication_key_value,
                         actual_state['bookmarks'][self.stream['tap_stream_id']]['replication_key_value'])

    @patch("tap_yugabyte.sync_strategies.incremental.singer.write_message")
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


class TestIncrementalSelectSql(TestCase):
    """The statement INCREMENTAL emits, against the shape-3/4 index.

    The bucket predicate is what makes it stream: bounding a range does not need
    it, but ORDER BY does -- without it a full ordered drain plans as a sequential
    scan and an external merge sort.
    """

    BASE = {
        'escaped_columns': ['"id"', '"created_at"'],
        'replication_key': 'created_at',
        'replication_key_sql_datatype': 'timestamp with time zone',
        'replication_key_value': '2026-09-22 09:00:00+00',
        'schema_name': 'svc',
        'table_name': 'event',
        'limit': None,
        'keyset_buckets': 3,
        'pk_columns': ['id'],
    }

    def _sql(self, **overrides):
        return incremental._get_select_sql({**self.BASE, **overrides})

    def test_buckets_on_the_primary_key_not_the_replication_key(self):
        sql = self._sql()
        self.assertIn('(yb_hash_code("id") % 3) IN (0, 1, 2)', sql)
        self.assertNotIn('yb_hash_code("created_at")', sql)

    def test_orders_by_the_key_then_the_primary_key(self):
        # the index ends in the primary key, so the tie order is free and stable
        self.assertIn('ORDER BY "created_at" ASC, "id" ASC', self._sql())

    def test_hints_the_replication_key_index(self):
        self.assertIn('IndexScan(event event_created_at_pw_keyset)', self._sql())

    def test_hints_the_primary_key_index_when_the_key_is_the_primary_key(self):
        sql = self._sql(replication_key='id', escaped_columns=['"id"'],
                        replication_key_sql_datatype='bigint',
                        replication_key_value=25000)
        self.assertIn('IndexScan(event event_pw_keyset)', sql)
        self.assertNotIn('event_id_pw_keyset', sql)
        # and the ordering columns are deduped, not `ORDER BY "id" ASC, "id" ASC`
        self.assertIn('ORDER BY "id" ASC', sql)
        self.assertNotIn('"id" ASC, "id" ASC', sql)

    def test_bookmark_is_cast_to_the_discovered_column_type(self):
        # a timestamptz literal against a timestamp column is accepted as an index
        # condition and then rechecked against every row, which the plan does not
        # distinguish from a real bound
        self.assertIn("::timestamp with time zone", self._sql())

    def test_no_bucket_predicate_without_a_primary_key(self):
        sql = self._sql(pk_columns=[])
        self.assertNotIn('yb_hash_code', sql)
        self.assertNotIn('IndexScan', sql)
        self.assertIn('ORDER BY "created_at" ASC', sql)


class TestBookmarkStalledWarning(TestCase):
    """A tie group larger than one run can drain never advances the bookmark, so
    the sync makes no progress ever -- while every run still looks healthy,
    emitting a full batch of records on schedule.

    Singer INCREMENTAL state carries one scalar and the resume predicate is >=,
    so this is detected and reported, not fixed: fixing it means a composite
    bookmark, which is not a shape this state has. See INDEXES.md, "Resuming
    inside a tie group".
    """

    STREAM = {'tap_stream_id': 'rt-bulkload'}
    STAMP = '2026-09-22T14:21:22.858584+00:00'
    LATER = '2026-09-22T14:30:00.000000+00:00'

    def _warning(self, ended_at, started_at, rows_saved, run_limit):
        state = {'bookmarks': {'rt-bulkload': {'replication_key_value': ended_at}}}
        with patch.object(incremental.LOGGER, 'warning') as warning:
            incremental._warn_if_bookmark_stalled(
                self.STREAM, state, 'updated_at', started_at, rows_saved, run_limit)
        return warning

    def test_a_full_batch_that_moved_nothing_is_reported(self):
        warning = self._warning(self.STAMP, self.STAMP, 10000, 10000)
        assert warning.called
        message = warning.call_args[0][0] % warning.call_args[0][1:]
        assert 'MADE NO PROGRESS' in message
        assert self.STAMP in message
        assert 'LOG_BASED' in message

    def test_an_empty_run_is_not_a_livelock(self):
        # no rows emitted is "no new data", which moves no bookmark by design
        assert not self._warning(self.STAMP, self.STAMP, 0, 10000).called

    def test_a_first_run_has_no_bookmark_to_fail_to_advance(self):
        assert not self._warning(self.STAMP, None, 10000, 10000).called

    def test_a_run_that_advanced_the_bookmark_is_fine(self):
        # a run ending inside a tie having made progress is exactly the
        # at-least-once behaviour the design accepts
        assert not self._warning(self.LATER, self.STAMP, 10000, 10000).called

    def test_a_single_row_table_does_not_warn_every_run(self):
        # it re-reads its one row forever and the bookmark cannot move, because
        # nothing lies beyond it -- the sync is caught up, not stuck
        assert not self._warning(self.STAMP, self.STAMP, 1, 10000).called

    def test_a_short_run_drained_everything_there_was(self):
        assert not self._warning(self.STAMP, self.STAMP, 9999, 10000).called

    def test_no_limit_configured_means_a_run_always_drains_to_the_end(self):
        assert not self._warning(self.STAMP, self.STAMP, 500000, None).called
