import re
import unittest
from unittest.mock import patch

import psycopg2

from tap_yugabyte import keyset
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
        # a one-column key has a one-rung ladder, so the branch form issues the
        # same single statement it always did -- no equality prefix, nothing
        # extra to run
        self.assertNotIn('"code" = %s', sql)

    def test_composite_resume_issues_one_statement_per_equality_prefix(self):
        """A composite key resumes with a ladder: `a=X AND b=Y AND c>Z`, then
        `a=X AND b>Y`, then `a>X` -- separate statements, most specific first.

        One statement cannot do this. The conjunctive expansion bounds only the
        leading column, so the scan starts at the head of the leading-value
        group: measured 33,220 index rows to return 5 when that group is the
        whole bucket. A UNION ALL of these same branches seeks correctly and is
        still wrong, because nothing guarantees Append emits children in branch
        order -- see keyset.keyset_branches.
        """
        recording_cursor = TestSyncTableResumeQuery._RecordingCursor()
        recording_connect = TestSyncTableResumeQuery._RecordingConnect(recording_cursor)

        stream = {'tap_stream_id': 'public-events', 'stream': 'events',
                  'table_name': 'events'}
        md_map = {
            (): {'schema-name': 'public', 'table-key-properties': ['a', 'b', 'c']},
            ('properties', 'a'): {'sql-datatype': 'integer'},
            ('properties', 'b'): {'sql-datatype': 'integer'},
            ('properties', 'c'): {'sql-datatype': 'bigint'},
        }
        state = {'bookmarks': {'public-events': {
            'version': 999,
            'max_pk_values': [9, 9, 9],
            'last_pk_fetched': {'1': [1, 2, 3]},
            'completed_buckets': [0, 2],
        }}}
        conn_config = {'host': 'foo', 'dbname': 'foo_db', 'user': 'foo_user',
                       'password': 'foo_pass', 'port': 12345, 'keyset_buckets': 3}

        with patch('psycopg2.connect') as mocked_connect, \
                patch('tap_yugabyte.keyset.validate_index', return_value=(True, None)), \
                patch('tap_yugabyte.db.hstore_available', return_value=False):
            mocked_connect.return_value.__enter__.return_value = recording_connect
            sync_table(conn_config, stream, state, ['a', 'b', 'c'], md_map)

        scans = [call for call in recording_cursor.executed
                 if call[1] and 'yb_hash_code' in call[0]]
        self.assertEqual(3, len(scans))

        lower_bounds = [sql.split('WHERE', 1)[1].split(' AND "a" <=')[0] for sql, _ in scans]
        self.assertEqual([
            ' (yb_hash_code("a", "b", "c") %% 3) = 1 AND "a" = %s AND "b" = %s AND "c" > %s',
            ' (yb_hash_code("a", "b", "c") %% 3) = 1 AND "a" = %s AND "b" > %s',
            ' (yb_hash_code("a", "b", "c") %% 3) = 1 AND "a" > %s',
        ], lower_bounds)

        # bookmark values first, then the upper bound's expansion of max_pk
        upper = keyset.keyset_params([9, 9, 9],
                                     keyset.keyset_predicate(['a', 'b', 'c'],
                                                             after=False)[1])
        self.assertEqual([[1, 2, 3] + upper, [1, 2] + upper, [1] + upper],
                         [params for _, params in scans])

        for sql, _ in scans:
            # ordering inside a statement is the ORDER BY; ordering between
            # statements is the loop. Neither is an Append's child order.
            self.assertIn('ORDER BY "a" ASC, "b" ASC, "c" ASC', sql)
            self.assertNotIn('UNION', sql)
            self.assertNotIn('ROW(', sql)

    def test_fresh_bucket_is_still_a_single_statement(self):
        """With no bookmark there is nothing to resume past, so the ladder has
        no rungs and the scan is the one unbounded-below statement."""
        recording_cursor = TestSyncTableResumeQuery._RecordingCursor()
        recording_connect = TestSyncTableResumeQuery._RecordingConnect(recording_cursor)

        stream = {'tap_stream_id': 'public-events', 'stream': 'events',
                  'table_name': 'events'}
        md_map = {
            (): {'schema-name': 'public', 'table-key-properties': ['a', 'b']},
            ('properties', 'a'): {'sql-datatype': 'integer'},
            ('properties', 'b'): {'sql-datatype': 'bigint'},
        }
        state = {'bookmarks': {'public-events': {
            'version': 999,
            'max_pk_values': [9, 9],
            'last_pk_fetched': {},
            'completed_buckets': [0, 2],
        }}}
        conn_config = {'host': 'foo', 'dbname': 'foo_db', 'user': 'foo_user',
                       'password': 'foo_pass', 'port': 12345, 'keyset_buckets': 3}

        with patch('psycopg2.connect') as mocked_connect, \
                patch('tap_yugabyte.keyset.validate_index', return_value=(True, None)), \
                patch('tap_yugabyte.db.hstore_available', return_value=False):
            mocked_connect.return_value.__enter__.return_value = recording_connect
            sync_table(conn_config, stream, state, ['a', 'b'], md_map)

        scans = [call for call in recording_cursor.executed
                 if call[1] and 'yb_hash_code' in call[0]]
        self.assertEqual(1, len(scans))
        self.assertNotIn('"a" = %s', scans[0][0])


class _FakeKeysetTable:
    """An in-memory stand-in for one keyset-indexed table.

    It answers the statements _scan_bucket actually builds -- deriving the
    branch from the parameter count rather than from a hand-written list, so a
    change to the ladder shows up here as wrong rows rather than as a test that
    quietly stops exercising anything.
    """

    def __init__(self, keys, buckets=3, max_pk=None, fail_after=None):
        self.keys = sorted(tuple(k) for k in keys)
        self.width = len(self.keys[0])
        self.buckets = buckets
        self.max_pk = tuple(max_pk) if max_pk else max(self.keys)
        # number of placeholders the upper bound contributes, so the rest of the
        # parameter list is the branch's own prefix
        self.upper_params = len(
            keyset.keyset_predicate(['x'] * self.width, after=False)[1])
        self.fail_after = fail_after
        self.rows_served = 0
        self.statements = []

    def bucket_of(self, key):
        # any stable partition of the keys will do; the tap only requires that
        # the bucket is a function of the key
        return sum(int(part) for part in key) % self.buckets

    def rows_for(self, sql, params):
        self.statements.append((sql, list(params)))
        bucket = int(re.search(r'%% \d+\) = (\d+)', sql).group(1))
        prefix_len = len(params) - self.upper_params
        bound = tuple(params[:prefix_len])

        def after_bound(key):
            if prefix_len == 0:
                return True
            pinned = prefix_len - 1
            return key[:pinned] == bound[:pinned] and key[pinned] > bound[pinned]

        return [k for k in self.keys
                if self.bucket_of(k) == bucket and after_bound(k) and k <= self.max_pk]


class _ReadRestart(psycopg2.OperationalError):
    """The shape of a YugabyteDB read restart: transient, no fault in the
    statement, and the connection is not reusable afterwards. psycopg2's own
    pgcode is read-only, so it is shadowed on a subclass."""
    pgcode = '40001'


class _FakeCursor:
    def __init__(self, table, name=None):
        self.table = table
        self.name = name
        self.itersize = None
        self._rows = []
        self._last_sql = ''

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def execute(self, sql, params=None):
        self._last_sql = sql
        self._rows = self.table.rows_for(sql, params) if params else []

    def fetchone(self):
        if 'pg_settings' in self._last_sql:
            return (1,)
        return self.table.max_pk

    def __iter__(self):
        for row in self._rows:
            if (self.table.fail_after is not None
                    and self.table.rows_served == self.table.fail_after):
                self.table.fail_after = None
                raise _ReadRestart('read restart required')
            self.table.rows_served += 1
            yield row


class _FakeConnection:
    def __init__(self, table):
        self.table = table

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def cursor(self, *_args, **kwargs):
        return _FakeCursor(self.table, kwargs.get('name'))


class TestBranchSequenceDrainsCorrectly(unittest.TestCase):
    """The ladder has to drain a bucket exactly once, in order, across retries."""

    PK = ['a', 'b', 'c']
    MD_MAP = {
        (): {'schema-name': 'public', 'table-key-properties': PK},
        ('properties', 'a'): {'sql-datatype': 'integer'},
        ('properties', 'b'): {'sql-datatype': 'integer'},
        ('properties', 'c'): {'sql-datatype': 'bigint'},
    }
    STREAM = {'tap_stream_id': 'public-events', 'stream': 'events',
              'table_name': 'events'}
    CONN = {'host': 'foo', 'dbname': 'foo_db', 'user': 'foo_user',
            'password': 'foo_pass', 'port': 12345, 'keyset_buckets': 3}

    @staticmethod
    def _keys():
        # a deliberately low-cardinality leading column: three values over 125
        # keys, which is the shape the expansion degenerates on
        return [(a, b, c)
                for a in (1, 2, 3) for b in range(1, 6) for c in range(1, 9)]

    def _run(self, table, state):
        emitted = []

        def record(message):
            record_dict = getattr(message, 'record', None)
            if record_dict is not None:
                emitted.append(tuple(record_dict[col] for col in self.PK))

        with patch('psycopg2.connect') as mocked_connect, \
                patch('tap_yugabyte.keyset.validate_index', return_value=(True, None)), \
                patch('tap_yugabyte.db.hstore_available', return_value=False), \
                patch('tap_yugabyte.retry.time.sleep'), \
                patch('singer.write_message', side_effect=record):
            mocked_connect.return_value.__enter__.return_value = _FakeConnection(table)
            result = sync_table(self.CONN, self.STREAM, state, self.PK, self.MD_MAP)
        return emitted, result

    def _fresh_state(self, table):
        return {'bookmarks': {'public-events': {
            'version': 999,
            'max_pk_values': list(table.max_pk),
            'last_pk_fetched': {},
        }}}

    def test_full_drain_covers_every_row_exactly_once(self):
        keys = self._keys()
        table = _FakeKeysetTable(keys)
        emitted, _ = self._run(table, self._fresh_state(table))

        self.assertEqual(len(keys), len(emitted))
        self.assertEqual(sorted(keys), sorted(emitted))
        self.assertEqual(len(set(emitted)), len(emitted))

    def test_each_bucket_is_ascending_and_rows_belong_to_one_bucket(self):
        keys = self._keys()
        table = _FakeKeysetTable(keys)
        emitted, _ = self._run(table, self._fresh_state(table))

        per_bucket = {}
        for key in emitted:
            per_bucket.setdefault(table.bucket_of(key), []).append(key)
        for bucket, rows in per_bucket.items():
            self.assertEqual(sorted(rows), rows, f'bucket {bucket} out of order')
            for key in rows:
                self.assertEqual(bucket, table.bucket_of(key))
        self.assertEqual(sorted(keys),
                         sorted(k for rows in per_bucket.values() for k in rows))

    def test_resume_from_a_midpoint_is_the_exact_complement(self):
        """Split each bucket at its midpoint: the two halves are disjoint and
        together are the bucket. This is the ladder's whole job."""
        keys = self._keys()
        table = _FakeKeysetTable(keys)
        first_half, second_half = [], []
        bookmarks = {}
        for bucket in range(3):
            rows = sorted(k for k in keys if table.bucket_of(k) == bucket)
            midpoint = len(rows) // 2
            first_half.extend(rows[:midpoint])
            bookmarks[str(bucket)] = list(rows[midpoint - 1])
            second_half.extend(rows[midpoint:])

        state = {'bookmarks': {'public-events': {
            'version': 999,
            'max_pk_values': list(table.max_pk),
            'last_pk_fetched': bookmarks,
        }}}
        emitted, _ = self._run(table, state)

        self.assertEqual(sorted(second_half), sorted(emitted))
        self.assertFalse(set(emitted) & set(first_half))
        self.assertEqual(sorted(keys), sorted(set(emitted) | set(first_half)))

    def test_retry_partway_through_the_ladder_neither_repeats_nor_skips(self):
        """A read restart in the middle of the sequence re-runs the whole
        sequence from the bookmark. The bookmark is what makes that safe: the
        rungs are disjoint, consecutive and ascending, so the rows already
        emitted are exactly the keys up to the bookmark and the rebuilt ladder
        is exactly the rest."""
        keys = self._keys()
        # start every bucket just past its first row, so the ladder has all
        # three rungs, and fail deep enough to be past the first of them
        table = _FakeKeysetTable(keys, fail_after=40)
        bookmarks = {}
        already = []
        for bucket in range(3):
            rows = sorted(k for k in keys if table.bucket_of(k) == bucket)
            bookmarks[str(bucket)] = list(rows[0])
            already.append(rows[0])

        state = {'bookmarks': {'public-events': {
            'version': 999,
            'max_pk_values': list(table.max_pk),
            'last_pk_fetched': bookmarks,
        }}}
        emitted, _ = self._run(table, state)

        self.assertIsNone(table.fail_after, 'the injected failure never fired')
        expected = sorted(set(keys) - set(already))
        self.assertEqual(expected, sorted(emitted))
        self.assertEqual(len(emitted), len(set(emitted)), 'a row was emitted twice')

    def test_retry_rebuilds_the_ladder_at_the_last_row_emitted(self):
        """One bucket, so the failure lands on a known row. The replacement
        ladder must be bounded at exactly that row -- not at the position the
        attempt started from, which is what hoisting the build out of the retry
        would give."""
        keys = self._keys()
        table = _FakeKeysetTable(keys, buckets=1, fail_after=40)
        conn = dict(self.CONN, keyset_buckets=1)
        state = self._fresh_state(table)

        emitted = []

        def record(message):
            record_dict = getattr(message, 'record', None)
            if record_dict is not None:
                emitted.append(tuple(record_dict[col] for col in self.PK))

        with patch('psycopg2.connect') as mocked_connect, \
                patch('tap_yugabyte.keyset.validate_index', return_value=(True, None)), \
                patch('tap_yugabyte.db.hstore_available', return_value=False), \
                patch('tap_yugabyte.retry.time.sleep'), \
                patch('singer.write_message', side_effect=record):
            mocked_connect.return_value.__enter__.return_value = _FakeConnection(table)
            sync_table(conn, self.STREAM, state, self.PK, self.MD_MAP)

        self.assertIsNone(table.fail_after, 'the injected failure never fired')
        self.assertEqual(sorted(keys), sorted(emitted))
        self.assertEqual(len(keys), len(emitted), 'a row was emitted twice')

        # the failed attempt was the single unbounded statement; everything
        # after it is the ladder, three rungs pinned to the 40th key
        before, after = table.statements[:1], table.statements[1:]
        self.assertEqual(table.upper_params, len(before[0][1]))
        last_emitted = sorted(keys)[39]
        self.assertEqual(3, len(after))
        self.assertEqual(
            [list(last_emitted), list(last_emitted[:2]), [last_emitted[0]]],
            [params[:len(params) - table.upper_params] for _, params in after])


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
