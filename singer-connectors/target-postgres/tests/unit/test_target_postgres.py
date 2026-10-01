import unittest
import os

from unittest.mock import Mock, patch

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

    def test_store_record_coalesces_patch_events_for_same_primary_key(self):
        db_sync = Mock(record_update_mode=target_postgres.RECORD_UPDATE_MODE_PATCH)
        records = {}

        target_postgres.store_record(records, '1', {'id': 1, 'payload': 'value'}, db_sync)
        target_postgres.store_record(records, '1', {'id': 1, 'marker': 'updated'}, db_sync)

        self.assertEqual(records, {
            '1': {'id': 1, 'payload': 'value', 'marker': 'updated'},
        })

    def test_store_record_patch_explicit_null_overwrites_buffered_value(self):
        db_sync = Mock(record_update_mode=target_postgres.RECORD_UPDATE_MODE_PATCH)
        records = {}

        target_postgres.store_record(records, '1', {'id': 1, 'payload': 'value'}, db_sync)
        target_postgres.store_record(records, '1', {'id': 1, 'payload': None}, db_sync)

        self.assertEqual(records, {'1': {'id': 1, 'payload': None}})

    def test_store_record_replaces_non_patch_event_for_same_primary_key(self):
        db_sync = Mock(record_update_mode=None)
        records = {}

        target_postgres.store_record(records, '1', {'id': 1, 'payload': 'value'}, db_sync)
        target_postgres.store_record(records, '1', {'id': 1, 'marker': 'updated'}, db_sync)

        self.assertEqual(records, {'1': {'id': 1, 'marker': 'updated'}})

    def test_group_patch_records_distinguishes_absent_column_from_explicit_null(self):
        db_sync = Mock(record_update_mode=target_postgres.RECORD_UPDATE_MODE_PATCH)
        db_sync.present_column_names.side_effect = lambda record: tuple(record)
        records = {
            '1': {'id': 1},
            '2': {'id': 2, 'payload': None},
            '3': {'id': 3, 'payload': 'value'},
        }

        groups = target_postgres.group_records_by_update_columns(records, db_sync)

        self.assertEqual(groups, [
            (('id',), {'1': {'id': 1}}),
            (('id', 'payload'), {
                '2': {'id': 2, 'payload': None},
                '3': {'id': 3, 'payload': 'value'},
            }),
        ])

    def test_group_non_patch_records_keeps_one_unrestricted_batch(self):
        db_sync = Mock(record_update_mode=None)
        records = {'1': {'id': 1}, '2': {'id': 2, 'payload': None}}

        self.assertEqual(
            target_postgres.group_records_by_update_columns(records, db_sync),
            [(None, records)],
        )
