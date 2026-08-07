import io
import json
import unittest
import os
import itertools

from contextlib import redirect_stdout
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import target_snowflake


def _mock_record_to_csv_line(record):
    return record


class TestTargetSnowflake(unittest.TestCase):

    def setUp(self):
        self.config = {}
        self.maxDiff = None

    def test_store_record_coalesces_patch_events_for_same_primary_key(self):
        db_sync = Mock(record_update_mode=target_snowflake.RECORD_UPDATE_MODE_PATCH)
        records = {}

        target_snowflake.store_record(records, '1', {'id': 1, 'payload': 'value'}, db_sync)
        target_snowflake.store_record(records, '1', {'id': 1, 'marker': 'updated'}, db_sync)

        self.assertEqual(records, {
            '1': {'id': 1, 'payload': 'value', 'marker': 'updated'},
        })

    def test_store_record_patch_explicit_null_overwrites_buffered_value(self):
        db_sync = Mock(record_update_mode=target_snowflake.RECORD_UPDATE_MODE_PATCH)
        records = {}

        target_snowflake.store_record(records, '1', {'id': 1, 'payload': 'value'}, db_sync)
        target_snowflake.store_record(records, '1', {'id': 1, 'payload': None}, db_sync)

        self.assertEqual(records, {'1': {'id': 1, 'payload': None}})

    def test_store_record_replaces_non_patch_event_for_same_primary_key(self):
        db_sync = Mock(record_update_mode=None)
        records = {}

        target_snowflake.store_record(records, '1', {'id': 1, 'payload': 'value'}, db_sync)
        target_snowflake.store_record(records, '1', {'id': 1, 'marker': 'updated'}, db_sync)

        self.assertEqual(records, {'1': {'id': 1, 'marker': 'updated'}})

    def test_group_patch_records_distinguishes_absent_column_from_explicit_null(self):
        db_sync = Mock(record_update_mode=target_snowflake.RECORD_UPDATE_MODE_PATCH)
        db_sync.present_column_names.side_effect = lambda record: tuple(record)
        records = {
            '1': {'id': 1},
            '2': {'id': 2, 'payload': None},
            '3': {'id': 3, 'payload': 'value'},
        }

        groups = target_snowflake.group_records_by_update_columns(records, db_sync)

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
            target_snowflake.group_records_by_update_columns(records, db_sync),
            [(None, records)],
        )

    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_full_to_patch_schema_transition_flushes_and_rebuilds_stream(self, db_sync_mock,
                                                                         flush_streams_mock):
        schema = {
            'type': 'object',
            'properties': {
                'id': {'type': ['integer']},
                'payload': {'type': ['null', 'string']},
            },
        }
        patch_schema = dict(schema)
        patch_schema['x-pipelinewise-record-update-mode'] = 'PATCH'
        lines = [
            json.dumps({
                'type': 'SCHEMA',
                'stream': 'public-table',
                'schema': schema,
                'key_properties': ['id'],
            }),
            json.dumps({
                'type': 'RECORD',
                'stream': 'public-table',
                'record': {'id': 1, 'payload': 'value'},
            }),
            json.dumps({
                'type': 'SCHEMA',
                'stream': 'public-table',
                'schema': patch_schema,
                'key_properties': ['id'],
            }),
        ]
        db_sync_mock.return_value.record_primary_key_string.return_value = '1'
        flush_streams_mock.return_value = None

        target_snowflake.persist_lines({}, lines)

        flush_streams_mock.assert_called_once()
        self.assertEqual(db_sync_mock.call_count, 2)
        self.assertNotIn(
            'x-pipelinewise-record-update-mode',
            db_sync_mock.call_args_list[0].args[1]['schema'],
        )
        self.assertEqual(
            db_sync_mock.call_args_list[1].args[1]['schema']['x-pipelinewise-record-update-mode'],
            'PATCH',
        )

    @patch('target_snowflake.flush_records')
    def test_hard_delete_runs_only_after_patch_batch_loads(self, flush_records_mock):
        db_sync = Mock()
        row_count = {'public-table': 1}
        records = {'1': {'id': 1, '_sdc_deleted_at': '2026-08-08T12:00:00Z'}}

        target_snowflake.load_stream_batch(
            'public-table',
            records,
            row_count,
            db_sync,
            delete_rows=True,
        )

        flush_records_mock.assert_called_once_with(
            'public-table', records, db_sync, None, False, None
        )
        db_sync.delete_rows.assert_called_once_with('public-table')
        self.assertEqual(row_count['public-table'], 0)

    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(self, dbSync_mock,
                                                                                     flush_streams_mock):
        self.config['batch_size_rows'] = 20
        self.config['flush_all_streams'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        self.assertEqual(1, flush_streams_mock.call_count)

    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_lines_with_same_schema_expect_flushing_once(self, dbSync_mock,
                                                                 flush_streams_mock):
        self.config['batch_size_rows'] = 20

        with open(f'{os.path.dirname(__file__)}/resources/same-schemas-multiple-times.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        self.assertEqual(1, flush_streams_mock.call_count)

    @patch('target_snowflake.datetime')
    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_40_records_with_batch_wait_limit(self, dbSync_mock, flush_streams_mock, dateTime_mock):

        start_time = datetime(2021, 4, 6, 0, 0, 0)
        increment = 11
        counter = itertools.count()

        # Move time forward by {{increment}} seconds every time utcnow() is called
        dateTime_mock.utcnow.side_effect = lambda: start_time + timedelta(seconds=increment * next(counter))

        self.config['batch_size_rows'] = 100
        self.config['batch_wait_limit_seconds'] = 10
        self.config['flush_all_streams'] = True

        # Expecting 40 records
        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        # Expecting flush after every records + 1 at the end
        self.assertEqual(flush_streams_mock.call_count, 41)

    @patch('target_snowflake.DbSync')
    @patch('target_snowflake.os.remove')
    def test_archive_load_files_incremental_replication(self, os_remove_mock, dbSync_mock):
        self.config['tap_id'] = 'test_tap_id'
        self.config['archive_load_files'] = True
        self.config['s3_bucket'] = 'dummy_bucket'

        with open(f'{os.path.dirname(__file__)}/resources/messages-simple-table.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.put_to_stage.return_value = 'some-s3-folder/some-name_date_batch_hash.csg.gz'

        target_snowflake.persist_lines(self.config, lines)

        copy_to_archive_args = instance.copy_to_archive.call_args[0]
        self.assertEqual(copy_to_archive_args[0], 'some-s3-folder/some-name_date_batch_hash.csg.gz')
        self.assertEqual(copy_to_archive_args[1], 'test_tap_id/test_simple_table/some-name_date_batch_hash.csg.gz')
        self.assertDictEqual(copy_to_archive_args[2], {
            'tap': 'test_tap_id',
            'schema': 'tap_mysql_test',
            'table': 'test_simple_table',
            'archived-by': 'pipelinewise_target_snowflake',
            'incremental-key': 'id',
            'incremental-key-min': '1',
            'incremental-key-max': '5'
        })

    @patch('target_snowflake.DbSync')
    @patch('target_snowflake.os.remove')
    def test_archive_load_files_log_based_replication(self, os_remove_mock, dbSync_mock):
        self.config['tap_id'] = 'test_tap_id'
        self.config['archive_load_files'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.put_to_stage.return_value = 'some-s3-folder/some-name_date_batch_hash.csg.gz'

        target_snowflake.persist_lines(self.config, lines)

        copy_to_archive_args = instance.copy_to_archive.call_args[0]
        self.assertEqual(copy_to_archive_args[0], 'some-s3-folder/some-name_date_batch_hash.csg.gz')
        self.assertEqual(copy_to_archive_args[1], 'test_tap_id/logical1_table2/some-name_date_batch_hash.csg.gz')
        self.assertDictEqual(copy_to_archive_args[2], {
            'tap': 'test_tap_id',
            'schema': 'logical1',
            'table': 'logical1_table2',
            'archived-by': 'pipelinewise_target_snowflake'
        })

    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_lines_with_only_state_messages(self, dbSync_mock, flush_streams_mock):
        """
        Given only state messages, target should emit the last one
        """

        self.config['batch_size_rows'] = 5

        with open(f'{os.path.dirname(__file__)}/resources/streams_only_state.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        # catch stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            target_snowflake.persist_lines(self.config, lines)

        flush_streams_mock.assert_not_called()

        self.assertEqual(
            buf.getvalue().strip(),
            '{"bookmarks": {"tap_mysql_test-test_simple_table": {"replication_key": "id", '
            '"replication_key_value": 100, "version": 1}}}')
