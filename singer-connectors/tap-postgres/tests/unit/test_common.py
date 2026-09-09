import json
from unittest import TestCase
from unittest.mock import patch

import singer
import tap_postgres

from tap_postgres.sync_strategies import common


class TestSchemaMessage(TestCase):
    def setUp(self):
        self.stream = {
            'stream': 'table',
            'schema': {'type': 'object', 'properties': {'id': {'type': 'integer'}}},
            'metadata': [{
                'breadcrumb': [],
                'metadata': {
                    'schema-name': 'public',
                    'table-key-properties': ['id']
                }
            }]
        }

    @patch('tap_postgres.sync_strategies.common.write_schema_message')
    def test_default_schema_message_has_full_record_semantics(self, write_schema_message):
        common.send_schema_message(self.stream, [])

        schema_message = write_schema_message.call_args.args[0]
        self.assertEqual(self.stream['schema'], schema_message['schema'])
        self.assertNotIn(common.RECORD_UPDATE_MODE_SCHEMA_KEY, schema_message['schema'])

    @patch('tap_postgres.sync_strategies.common.write_schema_message')
    def test_patch_schema_message_marks_record_update_mode_without_mutating_catalog(self, write_schema_message):
        common.send_schema_message(
            self.stream,
            ['lsn'],
            record_update_mode=common.PATCH_RECORD_UPDATE_MODE)

        schema_message = write_schema_message.call_args.args[0]
        self.assertEqual(
            common.PATCH_RECORD_UPDATE_MODE,
            schema_message['schema'][common.RECORD_UPDATE_MODE_SCHEMA_KEY])
        parsed_message = singer.parse_message(json.dumps(schema_message))
        self.assertEqual(
            common.PATCH_RECORD_UPDATE_MODE,
            parsed_message.schema[common.RECORD_UPDATE_MODE_SCHEMA_KEY])
        self.assertNotIn(common.RECORD_UPDATE_MODE_SCHEMA_KEY, self.stream['schema'])


class TestTraditionalSchemaMessages(TestCase):
    def setUp(self):
        self.stream = {
            'tap_stream_id': 'public-table',
            'stream': 'table',
            'schema': {'type': 'object', 'properties': {'id': {'type': 'integer'}}}
        }

    @patch('tap_postgres.full_table.sync_table')
    @patch('tap_postgres.sync_common.send_schema_message')
    def test_full_table_does_not_mark_schema_as_patch(self, send_schema_message, sync_table):
        sync_table.return_value = {}

        tap_postgres.do_sync_full_table({}, self.stream, {}, ['id'], {(): {}})

        send_schema_message.assert_called_once_with(self.stream, [])

    @patch('tap_postgres.incremental.sync_table')
    @patch('tap_postgres.sync_common.send_schema_message')
    def test_incremental_does_not_mark_schema_as_patch(self, send_schema_message, sync_table):
        sync_table.return_value = {}
        state = {'bookmarks': {'public-table': {}}}
        metadata = {(): {'replication-key': 'id'}}

        tap_postgres.do_sync_incremental({}, self.stream, state, ['id'], metadata)

        send_schema_message.assert_called_once_with(self.stream, ['id'])

    @patch('tap_postgres.singer.write_message')
    @patch('tap_postgres.full_table.sync_table')
    @patch('tap_postgres.register_type_adapters')
    @patch('tap_postgres.sync_common.send_schema_message')
    def test_logical_initial_full_table_does_not_mark_schema_as_patch(
            self, send_schema_message, _register_type_adapters, sync_table, _write_message):
        self.stream['metadata'] = [{
            'breadcrumb': [],
            'metadata': {
                'database-name': 'postgres',
                'schema-name': 'public',
                'table-key-properties': ['id']
            }
        }]
        sync_table.side_effect = lambda _conn_config, _stream, state, _desired_columns, _md_map: state

        tap_postgres.sync_traditional_stream(
            {}, self.stream, {'bookmarks': {}}, 'logical_initial', 42)

        send_schema_message.assert_called_once_with(self.stream, [])


class TestLogicalProgressMarkers(TestCase):
    @staticmethod
    def _stream(stream_id, database_name):
        return {
            'tap_stream_id': stream_id,
            'stream': stream_id,
            'schema': {'type': 'object', 'properties': {'id': {'type': 'integer'}}},
            'metadata': [{
                'breadcrumb': [],
                'metadata': {
                    'database-name': database_name,
                    'schema-name': 'public',
                    'table-key-properties': ['id'],
                },
            }],
        }

    def test_each_database_gets_its_own_logical_sync(self):
        initial_stream = self._stream('initial', 'initial_db')
        logical_streams = [self._stream('logical_b', 'db_b'), self._stream('logical_a', 'db_a')]
        catalog = {'streams': [initial_stream, *logical_streams]}
        state = {'currently_syncing': None, 'bookmarks': {}}
        conn_config = {'dbname': 'configured_db'}
        fetched_databases = []
        logical_calls = []
        traditional_boundaries = []
        events = []

        def fetch_current_lsn(config):
            fetched_databases.append(config['dbname'])
            events.append(f"fetch:{config['dbname']}")
            return 100

        def sync_traditional_stream(_config, _stream, current_state, _method, end_lsn):
            traditional_boundaries.append(end_lsn)
            events.append(f'traditional:{end_lsn}')
            return current_state

        def sync_logical_streams(config, streams, current_state, end_lsn, _state_file):
            logical_calls.append((config['dbname'], [stream['tap_stream_id'] for stream in streams], end_lsn))
            events.append(f"sync:{config['dbname']}:{end_lsn}")
            return current_state

        with patch('tap_postgres.is_selected_via_metadata', return_value=True), \
                patch('tap_postgres.any_logical_streams', return_value=True), \
                patch('tap_postgres.refresh_streams_schema'), \
                patch('tap_postgres.sync_method_for_streams', return_value=(
                    {'initial': 'logical_initial'}, [initial_stream], logical_streams)), \
                patch('tap_postgres.logical_replication.fetch_current_lsn', side_effect=fetch_current_lsn), \
                patch('tap_postgres.sync_traditional_stream', side_effect=sync_traditional_stream), \
                patch('tap_postgres.sync_logical_streams', side_effect=sync_logical_streams):
            tap_postgres.do_sync(conn_config, catalog, 'LOG_BASED', state, 'state.json')

        self.assertEqual(['configured_db'], fetched_databases)
        self.assertEqual([100], traditional_boundaries)
        self.assertEqual([
            ('db_a', ['logical_a'], 100),
            ('db_b', ['logical_b'], 100),
        ], logical_calls)
        self.assertEqual([
            'fetch:configured_db',
            'traditional:100',
            'sync:db_a:100',
            'sync:db_b:100',
        ], events)
