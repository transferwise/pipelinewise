import json
from unittest import TestCase
from unittest.mock import Mock, patch

import singer
import tap_postgres

from tap_postgres.sync_strategies import common


class TestConfigNumbers(TestCase):
    def test_publication_fence_timeout_accepts_positive_finite_values(self):
        self.assertEqual(
            tap_postgres._positive_finite_number(
                {'publication_fence_timeout_seconds': '0.5'},
                'publication_fence_timeout_seconds',
                300,
            ),
            0.5,
        )
        self.assertEqual(
            tap_postgres._positive_finite_number({}, 'publication_fence_timeout_seconds', 300),
            300.0,
        )

    def test_publication_fence_timeout_rejects_non_positive_or_non_finite_values(self):
        for value in (-1, 0, 'nan', 'inf', '-inf', 'invalid', None):
            with self.subTest(value=value), self.assertRaisesRegex(
                    ValueError, 'publication_fence_timeout_seconds must be a positive finite number'):
                tap_postgres._positive_finite_number(
                    {'publication_fence_timeout_seconds': value},
                    'publication_fence_timeout_seconds',
                    300,
                )


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
        sync_table.side_effect = lambda _conn_config, _stream, state, *_args, **_kwargs: state

        tap_postgres.sync_traditional_stream(
            {}, self.stream, {'bookmarks': {}}, 'logical_initial', 42)

        send_schema_message.assert_called_once_with(self.stream, [])


class TestLogicalSecondarySnapshots(TestCase):
    def test_initial_and_interrupted_snapshots_wait_before_reading_rows(self):
        stream = {
            'tap_stream_id': 'public-table',
            'stream': 'table',
            'schema': {'properties': {'id': {'type': 'integer'}}},
            'metadata': [{
                'breadcrumb': [],
                'metadata': {'database-name': 'source', 'schema-name': 'public'},
            }],
        }
        for method, expected_boundary in [('logical_initial', 200), ('logical_initial_interrupted', 100)]:
            with self.subTest(method=method):
                events = []
                config = {'dbname': 'source', 'use_secondary': True}
                state = {'bookmarks': {'public-table': {'lsn': 100, 'xmin': 1}}}

                def snapshot(snapshot_config, _stream, current_state, *_args, snapshot_lsn=None):
                    self.assertTrue(snapshot_config['use_secondary'])
                    self.assertEqual(expected_boundary, snapshot_lsn)
                    events.append('snapshot')
                    return current_state

                with patch('tap_postgres.register_type_adapters',
                           side_effect=lambda adapter_config: events.append(adapter_config['use_secondary'])), \
                        patch('tap_postgres.singer.write_message'), \
                        patch('tap_postgres.sync_common.send_schema_message'), \
                        patch('tap_postgres.logical_replication.wait_for_replica_replay',
                              side_effect=lambda _config, boundary: events.append(boundary)), \
                        patch('tap_postgres.full_table.sync_table', side_effect=snapshot):
                    tap_postgres.sync_traditional_stream(config, stream, state, method, 200)

                self.assertEqual([False, 'snapshot'], events)

    def test_replica_replay_wait_accepts_catchup_and_primary_fallback(self):
        for replay_positions in [[(True, None), (True, '0/64'), (True, '0/C8')], [(False, None)]]:
            with self.subTest(replay_positions=replay_positions):
                conn = Mock()
                cursor = Mock()
                conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
                conn.cursor.return_value.__exit__ = Mock(return_value=False)
                cursor.fetchone.side_effect = replay_positions
                with patch('tap_postgres.logical_replication.post_db.open_connection', return_value=conn), \
                        patch('tap_postgres.logical_replication.time.sleep'):
                    tap_postgres.logical_replication.wait_for_replica_replay({'use_secondary': True}, 200)
                self.assertEqual(len(replay_positions), cursor.fetchone.call_count)
                conn.close.assert_called_once()

    def test_replica_replay_timeout_prevents_snapshot_and_bookmark_emission(self):
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        cursor.fetchone.return_value = (True, '0/64')
        with patch('tap_postgres.logical_replication.post_db.open_connection', return_value=conn), \
                patch('tap_postgres.logical_replication.time.monotonic', side_effect=[0, 301]), \
                patch('tap_postgres.singer.write_message') as write_message, \
                self.assertRaisesRegex(tap_postgres.logical_replication.ReplicationSlotMigrationError,
                                       'Timed out waiting for the secondary'):
            tap_postgres.logical_replication.wait_for_replica_replay({'use_secondary': True}, 200)
        write_message.assert_not_called()
        conn.close.assert_called_once()


class TestLogicalProgressMarkers(TestCase):
    def test_incomplete_explicit_resync_is_rejected_before_source_mutation(self):
        state = {'_pipelinewise_pgoutput_fresh_start': {'version': 1}}
        with patch('tap_postgres.prepare_logical_replication') as prepare, \
                self.assertRaisesRegex(tap_postgres.logical_replication.ReplicationSlotMigrationError,
                                       'whole-tap PostgreSQL resync is incomplete'):
            tap_postgres.do_sync({}, {'streams': []}, 'LOG_BASED', state)
        prepare.assert_not_called()

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
                patch('tap_postgres.prepare_logical_replication', return_value=logical_streams), \
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
            ('initial_db', ['initial'], 100),
        ], logical_calls)
        self.assertEqual([
            'fetch:configured_db',
            'traditional:100',
            'sync:db_a:100',
            'sync:db_b:100',
            'sync:initial_db:100',
        ], events)

    def test_only_logical_schema_refresh_uses_primary_with_secondary_configured(self):
        logical_stream = self._stream('logical', 'source')
        traditional_stream = self._stream('traditional', 'source')
        config = {'dbname': 'source', 'use_secondary': True}
        with patch('tap_postgres.is_selected_via_metadata', return_value=True), \
                patch('tap_postgres.prepare_logical_replication', return_value=[logical_stream]), \
                patch('tap_postgres.logical_replication.fetch_current_lsn', return_value=100), \
                patch('tap_postgres.sync_method_for_streams', return_value=({}, [], [])), \
                patch('tap_postgres.refresh_streams_schema') as refresh:
            tap_postgres.do_sync(
                config, {'streams': [logical_stream, traditional_stream]}, 'LOG_BASED', {})

        self.assertEqual([
            (True, ['traditional']),
            (False, ['logical']),
        ], [
            (call.args[0]['use_secondary'], [stream['tap_stream_id'] for stream in call.args[1]])
            for call in refresh.call_args_list
        ])
        self.assertTrue(config['use_secondary'])

    def test_invalid_tap_id_fails_before_publication_preflight(self):
        stream = self._stream('logical', 'configured_db')
        conn_config = {'dbname': 'configured_db', 'tap_id': 'Invalid-Tap'}

        with patch('tap_postgres.sync_common.should_sync_column', return_value=True), \
                patch('tap_postgres.logical_replication.prepare_publication') as prepare_publication, \
                self.assertRaisesRegex(ValueError, r'tap_id must match \^\[a-z0-9_\]\+\$'):
            tap_postgres.prepare_logical_replication(conn_config, [stream], 'LOG_BASED')
        prepare_publication.assert_not_called()

    def test_logical_bookmark_cleanup_preserves_migration_state(self):
        stream = self._stream('selected', 'configured_db')
        migration = {
            'version': 1,
            'phase': 'pgoutput',
            'source_slot': 'pipelinewise_configured_db_tap',
            'destination_slot': 'pipelinewise_tap',
            'copy_lsn': 100,
            'bridge_lsn': 110,
        }
        state = {
            'currently_syncing': None,
            'bookmarks': {
                'selected': {'last_replication_method': 'LOG_BASED', 'lsn': 110},
                'deselected': {'last_replication_method': 'LOG_BASED', 'lsn': 90},
            },
            '_pipelinewise_pgoutput_migration': migration,
        }

        with patch(
                'tap_postgres.logical_replication.sync_tables',
                side_effect=lambda _config, _streams, current_state, *_args: current_state):
            result = tap_postgres.sync_logical_streams(
                {'debug_lsn': False}, [stream], state, 120, 'state.json')

        self.assertEqual(migration, result['_pipelinewise_pgoutput_migration'])
        self.assertIn('selected', result['bookmarks'])
        self.assertNotIn('deselected', result['bookmarks'])
