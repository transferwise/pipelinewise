import unittest
from unittest.mock import ANY, MagicMock, Mock, patch

from singer import Catalog, CatalogEntry, Schema

from tap_mysql import (
    _runtime_config,
    binlog_stream_requires_historical,
    do_discover,
    do_sync,
    sync_binlog_streams,
)


class TestTapMysql(unittest.TestCase):

    @patch('tap_mysql.discover_catalog')
    def test_discovery_enables_mariadb_json_aliases_only_for_iceberg(
        self, discover_catalog_mock
    ):
        discovered = Mock()
        discover_catalog_mock.return_value = discovered

        do_discover(
            Mock(),
            {
                'engine': 'mariadb',
                'target_table_format': 'iceberg',
                'iceberg_version': 3,
                'filter_dbs': 'source_db',
            },
        )

        discover_catalog_mock.assert_called_once_with(
            ANY,
            'source_db',
            detect_json_aliases=True,
        )
        discovered.dump.assert_called_once_with()

    @patch('tap_mysql.discover_catalog')
    def test_native_discovery_keeps_existing_catalog_call(
        self, discover_catalog_mock
    ):
        do_discover(
            Mock(),
            {'engine': 'mariadb', 'target_table_format': 'native'},
        )

        discover_catalog_mock.assert_called_once_with(ANY, None)

    @patch('tap_mysql.discover_catalog')
    def test_detected_mariadb_engine_enables_iceberg_json_aliases(
            self, discover_catalog_mock):
        discovered = Mock()
        discover_catalog_mock.return_value = discovered
        mysql_conn = Mock(configured_engine=None, resolved_engine='mariadb')
        config = {
            'target_table_format': 'iceberg',
            'iceberg_version': 3,
            'filter_dbs': 'source_db',
        }

        do_discover(mysql_conn, _runtime_config(mysql_conn, config))

        discover_catalog_mock.assert_called_once_with(
            mysql_conn,
            'source_db',
            detect_json_aliases=True,
        )
        discovered.dump.assert_called_once_with()
        self.assertNotIn('engine', config)

    @patch('tap_mysql.connect_with_backoff')
    def test_runtime_engine_resolver_opens_closed_connection_when_detection_is_needed(
            self, connect_with_backoff_mock):
        mysql_conn = Mock(configured_engine=None, resolved_engine=None, open=False)
        open_conn = Mock(resolved_engine='mariadb')
        connection_context = MagicMock()
        connection_context.__enter__.return_value = open_conn
        connect_with_backoff_mock.return_value = connection_context

        runtime_config = _runtime_config(mysql_conn, {})

        self.assertEqual(runtime_config['engine'], 'mariadb')
        connect_with_backoff_mock.assert_called_once_with(mysql_conn)

    @patch('tap_mysql.metrics.job_timer')
    @patch('tap_mysql.binlog.sync_binlog_stream')
    def test_sync_binlog_streams_emits_automatic_properties_in_schema(self, sync_binlog_mock, _):
        catalog_entry = CatalogEntry(
            stream='stream_1',
            tap_stream_id='stream_1',
            schema=Schema(properties={'id': Schema(type=['null', 'integer'])}),
        )
        emitted_schemas = []

        with patch(
                'tap_mysql.write_schema_message',
                side_effect=lambda stream: emitted_schemas.append(stream.schema.to_dict())):
            sync_binlog_streams(None, Catalog(streams=[catalog_entry]), {}, {})

        self.assertIn('_sdc_deleted_at', emitted_schemas[0]['properties'])
        sync_binlog_mock.assert_called_once()

    @patch('tap_mysql.sync_binlog_streams')
    @patch('tap_mysql.sync_non_binlog_streams')
    @patch('tap_mysql.get_binlog_streams', return_value=Catalog(streams=[]))
    @patch('tap_mysql.get_non_binlog_streams', return_value=Catalog(streams=[]))
    def test_sync_uses_detected_engine_without_mutating_config(
            self, _, __, sync_non_binlog_streams_mock, sync_binlog_streams_mock):
        mysql_conn = Mock(configured_engine=None, resolved_engine='mariadb')
        config = {'use_gtid': True}

        do_sync(mysql_conn, config, Catalog(streams=[]), {})

        self.assertEqual(config, {'use_gtid': True})
        sync_non_binlog_streams_mock.assert_called_once_with(
            mysql_conn, ANY, {}, True, 'mariadb')
        runtime_config = sync_binlog_streams_mock.call_args.args[2]
        self.assertIsNot(runtime_config, config)
        self.assertEqual(runtime_config['engine'], 'mariadb')
        self.assertIs(runtime_config['use_gtid'], True)

    @patch('tap_mysql.sync_binlog_streams')
    @patch('tap_mysql.sync_non_binlog_streams')
    @patch('tap_mysql.get_binlog_streams', return_value=Catalog(streams=[]))
    @patch('tap_mysql.get_non_binlog_streams', return_value=Catalog(streams=[]))
    def test_sync_explicit_engine_wins_over_detected_engine(
            self, _, __, sync_non_binlog_streams_mock, sync_binlog_streams_mock):
        mysql_conn = Mock(configured_engine=None, resolved_engine='mariadb')
        config = {'engine': 'MYSQL'}

        do_sync(mysql_conn, config, Catalog(streams=[]), {})

        sync_non_binlog_streams_mock.assert_called_once_with(
            mysql_conn, ANY, {}, False, 'mysql')
        self.assertEqual(sync_binlog_streams_mock.call_args.args[2]['engine'], 'mysql')
        self.assertEqual(config, {'engine': 'MYSQL'})

    def test_binlog_stream_requires_historical_with_log_coordinates_returns_false(self):

        catalog = CatalogEntry(tap_stream_id='stream_1', schema={})

        state = {
            'bookmarks': {
                'stream_1': {'log_file': 'binlog.0001', 'log_pos': 1123},
                'stream_2': {},
            }
        }

        self.assertFalse(binlog_stream_requires_historical(
            catalog,
            state
        ))

    def test_binlog_stream_requires_historical_with_partial_log_coordinates_returns_true(self):

        catalog = CatalogEntry(tap_stream_id='stream_1', schema={})

        state = {
            'bookmarks': {
                'stream_1': {'log_pos': 1123},
                'stream_2': {},
            }
        }

        self.assertTrue(binlog_stream_requires_historical(
            catalog,
            state
        ))

    def test_binlog_stream_requires_historical_with_gtid_returns_false(self):

        catalog = CatalogEntry(tap_stream_id='stream_1', schema={})

        state = {
            'bookmarks': {
                'stream_1': {'gtid': '0-3834-222'},
                'stream_2': {},
            }
        }

        self.assertFalse(binlog_stream_requires_historical(
            catalog,
            state
        ))

    def test_binlog_stream_requires_historical_with_no_log_coordinates_returns_true(self):

        catalog = CatalogEntry(tap_stream_id='stream_1', schema={})

        state = {
            'bookmarks': {
                'stream_1': {},
                'stream_2': {},
            }
        }

        self.assertTrue(binlog_stream_requires_historical(
            catalog,
            state
        ))

    def test_binlog_stream_requires_historical_with_log_coordinates_and_max_value_returns_true(self):

        catalog = CatalogEntry(tap_stream_id='stream_1', schema={})

        state = {
            'bookmarks': {
                'stream_1': {'log_file': 'binlog.0001', 'log_pos': 1123, 'max_pk_values': '111'},
                'stream_2': {},
            }
        }

        self.assertTrue(binlog_stream_requires_historical(
            catalog,
            state
        ))

    def test_binlog_stream_requires_historical_with_log_coordinates_and_last_pk_value_returns_true(self):

        catalog = CatalogEntry(tap_stream_id='stream_1', schema={})

        state = {
            'bookmarks': {
                'stream_1': {'log_file': 'binlog.0001', 'log_pos': 1123, 'last_pk_fetched': '111'},
                'stream_2': {},
            }
        }

        self.assertTrue(binlog_stream_requires_historical(
            catalog,
            state
        ))
