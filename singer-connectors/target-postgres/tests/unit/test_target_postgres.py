import unittest
import os
import gzip
import tempfile

from unittest.mock import patch

import target_postgres


def _mock_record_to_csv_line(record):
    return record


class TestTargetPostgres(unittest.TestCase):

    def setUp(self):
        self.config = {}

    @patch('target_postgres.flush_streams')
    @patch('target_postgres.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(self,
                                                                                     dbsync_mock,
                                                                                     flush_streams_mock):
        self.config['batch_size_rows'] = 20
        self.config['flush_all_streams'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbsync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_postgres.persist_lines(self.config, lines)

        flush_streams_mock.assert_called_once()
