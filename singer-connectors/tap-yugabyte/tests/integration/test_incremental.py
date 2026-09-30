import unittest

from singer import metadata

import tap_yugabyte

from tests.integration.test_full_table import _capture_singer_messages, _discover_stream, _desired_columns
from tests.utils import ensure_test_table, get_test_connection, get_test_connection_config


def _select_incremental(stream, replication_key):
    for entry in stream['metadata']:
        if not entry['breadcrumb']:
            entry['metadata']['selected'] = True
            entry['metadata']['replication-method'] = 'INCREMENTAL'
            entry['metadata']['replication-key'] = replication_key


class TestIncrementalSyncTable(unittest.TestCase):
    maxDiff = None
    table_name = 'incr_basic'

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

    def test_incremental_sync_table_emits_expected_messages(self):
        """A fresh INCREMENTAL sync emits SCHEMA, ACTIVATE_VERSION, one RECORD per row,
        and bookmarks the replication key value of the highest row emitted."""
        conn_config = get_test_connection_config()
        stream = _discover_stream(conn_config, self.table_name)
        _select_incremental(stream, 'id')
        md_map = metadata.to_map(stream['metadata'])
        desired_columns = _desired_columns(stream, md_map)
        state = {'bookmarks': {stream['tap_stream_id']: {}}}

        state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync_incremental, conn_config, stream, state, desired_columns, md_map)

        self.assertEqual('SCHEMA', messages[0]['type'])
        activate_messages = [m for m in messages if m['type'] == 'ACTIVATE_VERSION']
        self.assertEqual(1, len(activate_messages))
        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(3, len(record_messages))
        self.assertEqual([1, 2, 3], sorted(m['record']['id'] for m in record_messages))

        bookmarks = state['bookmarks'][stream['tap_stream_id']]
        self.assertEqual('id', bookmarks['replication_key'])
        self.assertEqual(3, bookmarks['replication_key_value'])
        self.assertIn('version', bookmarks)


class TestIncrementalSyncTableResume(unittest.TestCase):
    maxDiff = None
    table_name = 'incr_resume'

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

    def test_incremental_sync_table_resumes_from_replication_key_value(self):
        """A resumed sync re-fetches rows with replication key >= the bookmarked value
        (the boundary row is re-emitted, since the query is inclusive), and rows below
        the bookmark inserted afterwards are never fetched."""
        conn_config = get_test_connection_config()
        stream = _discover_stream(conn_config, self.table_name)
        _select_incremental(stream, 'id')
        md_map = metadata.to_map(stream['metadata'])
        desired_columns = _desired_columns(stream, md_map)

        crafted_version = 1700000000000
        state = {'bookmarks': {stream['tap_stream_id']: {
            'version': crafted_version,
            'replication_key_value': 2,
        }}}

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO {self.table_name} (id, value) VALUES (4, %s)', ('d',))

        result_state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync_incremental, conn_config, stream, state, desired_columns, md_map)

        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual([2, 3, 4], sorted(m['record']['id'] for m in record_messages))

        bookmarks = result_state['bookmarks'][stream['tap_stream_id']]
        self.assertEqual(crafted_version, bookmarks['version'])
        self.assertEqual(4, bookmarks['replication_key_value'])


class TestIncrementalSyncTableNullReplicationKeyValue(unittest.TestCase):
    maxDiff = None
    table_name = 'incr_null_rk'

    def setUp(self):
        ensure_test_table({
            'name': self.table_name,
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'updated_at', 'type': 'integer'},
            ],
        })
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO {self.table_name} (id, updated_at) VALUES (1, 10), (2, NULL)')

    def test_null_replication_key_value_is_never_bookmarked(self):
        """A row whose replication-key column is NULL must never be written into the
        replication_key_value bookmark: NULL sorts last, so a naive bookmark write would
        poison every subsequent resume with an unusable value."""
        conn_config = get_test_connection_config()
        stream = _discover_stream(conn_config, self.table_name)
        _select_incremental(stream, 'updated_at')
        md_map = metadata.to_map(stream['metadata'])
        desired_columns = _desired_columns(stream, md_map)
        state = {'bookmarks': {stream['tap_stream_id']: {}}}

        state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync_incremental, conn_config, stream, state, desired_columns, md_map)

        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(2, len(record_messages))

        bookmarks = state['bookmarks'][stream['tap_stream_id']]
        self.assertEqual(10, bookmarks['replication_key_value'])


class TestDoSyncDispatchesReplicationMethods(unittest.TestCase):
    maxDiff = None
    full_table_name = 'disp_full'
    incremental_table_name = 'disp_incr'

    def setUp(self):
        ensure_test_table({
            'name': self.full_table_name,
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'name', 'type': 'text'},
            ],
        })
        ensure_test_table({
            'name': self.incremental_table_name,
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'value', 'type': 'text'},
            ],
        })
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO {self.full_table_name} (id, name) VALUES (1, %s)', ('a',))
                cur.execute(f'INSERT INTO {self.incremental_table_name} (id, value) VALUES (1, %s), (2, %s)',
                            ('x', 'y'))

    def test_do_sync_dispatches_full_table_and_incremental_streams_in_one_run(self):
        """do_sync routes each selected stream to its own replication method via the
        sync_stream dispatcher, within a single catalog/state run."""
        conn_config = get_test_connection_config()
        full_stream = _discover_stream(conn_config, self.full_table_name)
        incr_stream = _discover_stream(conn_config, self.incremental_table_name)

        for entry in full_stream['metadata']:
            if not entry['breadcrumb']:
                entry['metadata']['selected'] = True
                entry['metadata']['replication-method'] = 'FULL_TABLE'

        _select_incremental(incr_stream, 'id')

        catalog = {'streams': [full_stream, incr_stream]}

        state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync, conn_config, catalog, None, {})

        record_messages = [m for m in messages if m['type'] == 'RECORD']
        full_records = [m for m in record_messages if 'name' in m['record']]
        incr_records = [m for m in record_messages if 'value' in m['record']]
        self.assertEqual(1, len(full_records))
        self.assertEqual(2, len(incr_records))

        full_bookmarks = state['bookmarks'][full_stream['tap_stream_id']]
        incr_bookmarks = state['bookmarks'][incr_stream['tap_stream_id']]
        self.assertIsNone(full_bookmarks['max_pk_values'])
        self.assertEqual(2, incr_bookmarks['replication_key_value'])
