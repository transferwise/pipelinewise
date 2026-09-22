import unittest
from unittest.mock import patch

from tap_yugabyte.sync_strategies.full_table import sync_view, sync_table

from tests.utils import MockedConnect


class TestFullTable(unittest.TestCase):
    """Test cases for full_table.sync_view, ported from tap-postgres"""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
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
        }

    def test_sync_view(self):
        """sync_view stamps a fresh version bookmark and always re-scans the whole view"""
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


class TestSyncTableWithPk(unittest.TestCase):
    """sync_table's resumable primary-key keyset pagination path"""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.patcher = patch('psycopg2.connect')
        mocked_connect = cls.patcher.start()
        mocked_connect.return_value.__enter__.return_value = MockedConnect()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.patcher.stop()

    @patch('tap_yugabyte.keyset.validate_index', return_value=(True, None))
    @patch('tap_yugabyte.db.hstore_available', return_value=False)
    def test_fresh_sync_snapshots_max_pk_and_clears_bookmarks_on_completion(
            self, _hstore_available, _validate_index):
        """A fresh sync (no prior bookmark) snapshots max_pk_values, then clears both
        resume bookmarks once the bounded scan completes."""
        stream = {'tap_stream_id': 'public-country', 'stream': 'country', 'table_name': 'country'}
        md_map = {
            (): {'schema-name': 'public', 'table-key-properties': ['id']},
            ('properties', 'id'): {'sql-datatype': 'integer'},
        }

        result = sync_table(self.conn_config, stream, {}, ['id'], md_map)

        bookmarks = result['bookmarks']['public-country']
        self.assertIsNone(bookmarks['max_pk_values'])
        self.assertIsNone(bookmarks['last_pk_fetched'])
        self.assertIsNone(bookmarks['completed_buckets'])
        self.assertIn('version', bookmarks)

    @patch('tap_yugabyte.keyset.validate_index', return_value=(True, None))
    @patch('tap_yugabyte.db.hstore_available', return_value=False)
    def test_resumed_sync_reuses_stream_version(self, _hstore_available, _validate_index):
        """A run that resumes from an existing max_pk_values bookmark reuses that
        stream's version instead of minting a new one."""
        stream = {'tap_stream_id': 'public-country', 'stream': 'country', 'table_name': 'country'}
        md_map = {
            (): {'schema-name': 'public', 'table-key-properties': ['id']},
            ('properties', 'id'): {'sql-datatype': 'integer'},
        }
        state = {'bookmarks': {'public-country': {
            'version': 999,
            'max_pk_values': [1234],
            'last_pk_fetched': [1000],
        }}}

        result = sync_table(self.conn_config, stream, state, ['id'], md_map)

        self.assertEqual(999, result['bookmarks']['public-country']['version'])

    def setUp(self) -> None:
        self.conn_config = {
            'host': 'foo',
            'dbname': 'foo_db',
            'user': 'foo_user',
            'password': 'foo_pass',
            'port': 12345,
        }


class TestSyncTableResumeQuery(unittest.TestCase):
    """Asserts the exact parameterized WHERE clause built for a resumed keyset scan"""

    class _RecordingCursor:
        def __init__(self, *_args, **_kwargs):
            self.executed = []

        def __enter__(self):
            return self

        def __exit__(self, *_args, **_kwargs):
            pass

        def __iter__(self):
            return iter([])

        def execute(self, sql, params=None):
            self.executed.append((sql, params))

        def fetchone(self):
            # the only fetchone on this path is the merge-scan availability probe,
            # which expects a count
            return (1,)

    class _RecordingConnect:
        def __init__(self, cursor):
            self._cursor = cursor

        def __enter__(self):
            return self

        def __exit__(self, *_args, **_kwargs):
            pass

        def cursor(self, *_args, **_kwargs):
            return self._cursor

    def test_resume_builds_keyset_comparison_with_bound_params(self):
        """A bucket resuming from its own last_pk_fetched builds a parameterized
        `pk > %s AND pk <= %s` clause, with the bookmarked values bound rather
        than interpolated into the SQL text."""
        recording_cursor = self._RecordingCursor()
        recording_connect = self._RecordingConnect(recording_cursor)

        stream = {'tap_stream_id': 'public-country', 'stream': 'country', 'table_name': 'country'}
        md_map = {
            (): {'schema-name': 'public', 'table-key-properties': ['code']},
            ('properties', 'code'): {'sql-datatype': 'character'},
        }
        state = {'bookmarks': {'public-country': {
            'version': 999,
            'max_pk_values': ['ZZZ'],
            # only bucket 1 was interrupted; the others have not started
            'last_pk_fetched': {'1': ['AAA']},
            'completed_buckets': [0, 2],
        }}}

        conn_config = {'host': 'foo', 'dbname': 'foo_db', 'user': 'foo_user',
                       'password': 'foo_pass', 'port': 12345, 'keyset_buckets': 3}

        with patch('psycopg2.connect') as mocked_connect, \
                patch('tap_yugabyte.keyset.validate_index', return_value=(True, None)), \
                patch('tap_yugabyte.db.hstore_available', return_value=False):
            mocked_connect.return_value.__enter__.return_value = recording_connect

            sync_table(conn_config, stream, state, ['code'], md_map)

        bucket_scans = [call for call in recording_cursor.executed
                        if call[1] and 'yb_hash_code' in call[0] and 'bucket_maxima' not in call[0]]
        self.assertEqual(1, len(bucket_scans))
        sql, params = bucket_scans[0]
        # a single-column key needs no expansion: `(code) > (%s)` already
        # collapses to `code > %s`, and a row constructor would only cost the
        # pushdown (see keyset.keyset_predicate)
        self.assertIn('"code" > %s', sql)
        self.assertIn('"code" <= %s', sql)
        self.assertNotIn('("code") >', sql)
        # doubled so psycopg2's placeholder interpolation leaves a single operator
        self.assertIn('%%', sql)
        # the key columns are ordered individually; a ROW() here would force a sort
        self.assertIn('ORDER BY "code" ASC', sql)
        self.assertNotIn('AAA', sql)
        self.assertNotIn('ZZZ', sql)
        self.assertEqual(['AAA', 'ZZZ'], params)


class TestSyncTableNoPk(unittest.TestCase):
    """sync_table's fallback for tables without a usable primary key"""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.patcher = patch('psycopg2.connect')
        mocked_connect = cls.patcher.start()
        mocked_connect.return_value.__enter__.return_value = MockedConnect()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.patcher.stop()

    def test_no_primary_key_falls_back_to_plain_scan(self):
        """With no table-key-properties, sync_table never writes resume bookmarks"""
        stream = {'tap_stream_id': 'public-no_pk_table', 'stream': 'no_pk_table', 'table_name': 'no_pk_table'}
        md_map = {
            (): {'schema-name': 'public', 'table-key-properties': []},
            ('properties', 'id'): {'sql-datatype': 'integer'},
        }
        conn_config = {'host': 'foo', 'dbname': 'foo_db', 'user': 'foo_user', 'password': 'foo_pass', 'port': 12345}

        result = sync_table(conn_config, stream, {}, ['id'], md_map)

        bookmarks = result['bookmarks']['public-no_pk_table']
        self.assertNotIn('max_pk_values', bookmarks)
        self.assertNotIn('last_pk_fetched', bookmarks)
        self.assertIn('version', bookmarks)


class TestLegacyStateMigration(unittest.TestCase):
    """State written before bucketing must not crash or be misread"""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.patcher = patch('psycopg2.connect')
        mocked_connect = cls.patcher.start()
        mocked_connect.return_value.__enter__.return_value = MockedConnect()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.patcher.stop()

    @patch('tap_yugabyte.keyset.validate_index', return_value=(True, None))
    @patch('tap_yugabyte.db.hstore_available', return_value=False)
    def test_pre_bucketing_last_pk_fetched_list_is_discarded(
            self, _hstore_available, _validate_index):
        """An older tap stored one cursor position as a list. It says nothing about
        any individual bucket, so it is dropped rather than indexed into."""
        stream = {'tap_stream_id': 'public-country', 'stream': 'country',
                  'table_name': 'country'}
        md_map = {
            (): {'schema-name': 'public', 'table-key-properties': ['id']},
            ('properties', 'id'): {'sql-datatype': 'integer'},
        }
        state = {'bookmarks': {'public-country': {
            'version': 999,
            'max_pk_values': [1234],
            'last_pk_fetched': [1000],   # legacy shape: a bare list, not per bucket
        }}}

        result = sync_table(self.conn_config, stream, state, ['id'], md_map)

        self.assertEqual(999, result['bookmarks']['public-country']['version'])

    def setUp(self) -> None:
        self.conn_config = {
            'host': 'foo', 'dbname': 'foo_db', 'user': 'foo_user',
            'password': 'foo_pass', 'port': 12345,
        }
