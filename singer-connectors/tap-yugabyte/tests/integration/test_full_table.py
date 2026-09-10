import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from singer import metadata

import tap_yugabyte
from tap_yugabyte.sync_strategies import common as sync_common

from tests.utils import ensure_test_table, get_test_connection, get_test_connection_config


class _CapturingStdout:
    """Stand-in for sys.stdout: singer.write_message writes bytes to stdout.buffer,
    while write_schema_message writes str via stdout.write, so both must be captured."""

    class _Buffer:
        def __init__(self, chunks):
            self._chunks = chunks

        def write(self, b):
            self._chunks.append(b)

        def flush(self):
            pass

    def __init__(self):
        self._chunks = []
        self.buffer = self._Buffer(self._chunks)

    def write(self, s):
        self._chunks.append(s.encode())

    def flush(self):
        pass

    def getvalue(self):
        return b''.join(self._chunks).decode()


def _capture_singer_messages(func, *args, **kwargs):
    """Run func, capturing every newline-delimited Singer message it writes to stdout."""
    stdout = _CapturingStdout()
    with redirect_stdout(stdout):
        result = func(*args, **kwargs)
    messages = [json.loads(line) for line in stdout.getvalue().splitlines() if line]
    return result, messages


def _discover_stream(conn_config, table_name):
    with patch('tap_yugabyte.dump_catalog'):
        streams = tap_yugabyte.do_discovery(conn_config)
    matches = [s for s in streams if s['table_name'] == table_name]
    assert len(matches) == 1, f"expected exactly one discovered stream for {table_name}, got {len(matches)}"
    return matches[0]


def _desired_columns(stream, md_map):
    return sorted(c for c in stream['schema']['properties'].keys() if sync_common.should_sync_column(md_map, c))


class TestFullTableSyncTable(unittest.TestCase):
    maxDiff = None
    table_name = 'ft_basic'

    def setUp(self):
        ensure_test_table({
            'name': self.table_name,
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'name', 'type': 'text'},
            ],
        })
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO {self.table_name} (id, name) VALUES (1, %s), (2, %s), (3, %s)',
                            ('alice', 'bob', 'carol'))

    def test_full_table_sync_table_emits_expected_messages(self):
        """A fresh FULL_TABLE sync emits SCHEMA, one RECORD per row, then ACTIVATE_VERSION,
        and clears the resume bookmarks once the bounded scan completes."""
        conn_config = get_test_connection_config()
        stream = _discover_stream(conn_config, self.table_name)
        md_map = metadata.to_map(stream['metadata'])
        desired_columns = _desired_columns(stream, md_map)

        state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync_full_table, conn_config, stream, {}, desired_columns, md_map)

        self.assertEqual('SCHEMA', messages[0]['type'])
        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(3, len(record_messages))
        self.assertEqual([1, 2, 3], sorted(m['record']['id'] for m in record_messages))
        self.assertEqual('ACTIVATE_VERSION', messages[-1]['type'])

        bookmarks = state['bookmarks'][stream['tap_stream_id']]
        self.assertIsNone(bookmarks['max_pk_values'])
        self.assertIsNone(bookmarks['last_pk_fetched'])
        self.assertIn('version', bookmarks)


class TestFullTableSyncTableResume(unittest.TestCase):
    maxDiff = None
    table_name = 'ft_resume'

    def setUp(self):
        ensure_test_table({
            'name': self.table_name,
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'value', 'type': 'text'},
            ],
        })
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO {self.table_name} (id, value) VALUES (1, %s), (2, %s), (3, %s)',
                            ('a', 'b', 'c'))

    def test_full_table_sync_table_resumes_from_last_pk_fetched(self):
        """A resumed sync only re-fetches rows beyond last_pk_fetched, bounded by the
        original max_pk_values snapshot, and reuses the interrupted run's stream version."""
        conn_config = get_test_connection_config()
        stream = _discover_stream(conn_config, self.table_name)
        md_map = metadata.to_map(stream['metadata'])
        desired_columns = _desired_columns(stream, md_map)

        crafted_version = 1700000000000
        state = {'bookmarks': {stream['tap_stream_id']: {
            'version': crafted_version,
            'max_pk_values': [3],
            'last_pk_fetched': [1],
        }}}

        # rows inserted after the original max_pk_values snapshot must not be re-emitted
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO {self.table_name} (id, value) VALUES (4, %s), (5, %s)', ('d', 'e'))

        result_state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync_full_table, conn_config, stream, state, desired_columns, md_map)

        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual([2, 3], sorted(m['record']['id'] for m in record_messages))

        bookmarks = result_state['bookmarks'][stream['tap_stream_id']]
        self.assertEqual(crafted_version, bookmarks['version'])
        self.assertIsNone(bookmarks['max_pk_values'])
        self.assertIsNone(bookmarks['last_pk_fetched'])


class TestFullTableSyncTableNonIntegerPk(unittest.TestCase):
    maxDiff = None
    table_name = 'ft_country'

    def setUp(self):
        ensure_test_table({
            'name': self.table_name,
            'columns': [
                {'name': 'code', 'type': 'character(3)', 'primary_key': True},
                {'name': 'name', 'type': 'text'},
            ],
        })
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO {self.table_name} (code, name) VALUES (%s, %s), (%s, %s), (%s, %s)',
                            ('USA', 'United States', 'FRA', 'France', 'GBR', 'United Kingdom'))

    def test_full_table_sync_table_non_integer_primary_key(self):
        """A character primary key is bound as a query parameter (never interpolated)
        and rows are emitted in ascending primary-key order."""
        conn_config = get_test_connection_config()
        stream = _discover_stream(conn_config, self.table_name)
        md_map = metadata.to_map(stream['metadata'])
        desired_columns = _desired_columns(stream, md_map)

        _state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync_full_table, conn_config, stream, {}, desired_columns, md_map)

        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(['FRA', 'GBR', 'USA'], [m['record']['code'] for m in record_messages])


class TestFullTableSyncView(unittest.TestCase):
    maxDiff = None
    table_name = 'ft_view_src'
    view_name = 'ft_view_src_v'

    def setUp(self):
        ensure_test_table({
            'name': self.table_name,
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'name', 'type': 'text'},
            ],
        })
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO {self.table_name} (id, name) VALUES (1, %s), (2, %s)', ('x', 'y'))
                cur.execute(f'DROP VIEW IF EXISTS {self.view_name}')
                cur.execute(f'CREATE VIEW {self.view_name} AS SELECT * FROM {self.table_name}')

    def tearDown(self):
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP VIEW IF EXISTS {self.view_name}')

    def test_full_table_sync_view_emits_expected_messages(self):
        """Views always re-scan in full. On a first run ACTIVATE_VERSION is emitted both
        before and after the scan (switching consumers up front, then confirming
        completion); there's no stable resume point for a view."""
        conn_config = get_test_connection_config()
        stream = _discover_stream(conn_config, self.view_name)
        md_map = metadata.to_map(stream['metadata'])
        self.assertTrue(md_map.get((), {}).get('is-view'))
        desired_columns = _desired_columns(stream, md_map)

        _state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync_full_table, conn_config, stream, {}, desired_columns, md_map)

        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(2, len(record_messages))
        activate_messages = [m for m in messages if m['type'] == 'ACTIVATE_VERSION']
        self.assertEqual(2, len(activate_messages))
        self.assertEqual('RECORD', messages[-2]['type'])
        self.assertEqual('ACTIVATE_VERSION', messages[-1]['type'])


class TestFullTableSyncTableNoPrimaryKey(unittest.TestCase):
    maxDiff = None
    table_name = 'ft_no_pk'

    def setUp(self):
        ensure_test_table({
            'name': self.table_name,
            'columns': [
                {'name': 'id', 'type': 'integer'},
                {'name': 'note', 'type': 'text'},
            ],
        })
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO {self.table_name} (id, note) VALUES (1, %s), (2, %s)', ('one', 'two'))

    def test_full_table_sync_table_no_primary_key(self):
        """Without a primary key, sync falls back to a plain scan and doesn't error"""
        conn_config = get_test_connection_config()
        stream = _discover_stream(conn_config, self.table_name)
        md_map = metadata.to_map(stream['metadata'])
        self.assertEqual([], md_map.get((), {}).get('table-key-properties'))
        desired_columns = _desired_columns(stream, md_map)

        state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync_full_table, conn_config, stream, {}, desired_columns, md_map)

        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(2, len(record_messages))

        bookmarks = state['bookmarks'][stream['tap_stream_id']]
        self.assertNotIn('max_pk_values', bookmarks)
        self.assertNotIn('last_pk_fetched', bookmarks)
