import unittest

from singer import CatalogEntry

from tap_mysql import binlog_stream_requires_historical


class TestTapMysql(unittest.TestCase):

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
