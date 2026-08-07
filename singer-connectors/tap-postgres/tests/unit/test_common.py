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
