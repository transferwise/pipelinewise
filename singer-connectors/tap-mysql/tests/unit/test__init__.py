import unittest
from unittest.mock import patch

from singer import Catalog, CatalogEntry, Schema

from tap_mysql import binlog_stream_requires_historical, sync_binlog_streams


class TestTapMysql(unittest.TestCase):

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
