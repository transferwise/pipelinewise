"""Check safe snapshot replay when transaction IDs change between runs."""

import copy
import unittest
from unittest.mock import patch

import singer

from tap_postgres.sync_strategies import full_table

from ..utils import get_test_connection, get_test_connection_config


class SnapshotInterrupted(Exception):
    """Stop the first snapshot after its first persisted row."""


class TestFullTableResume(unittest.TestCase):
    def test_first_checkpoint_keeps_bootstrap_incomplete_until_snapshot_finishes(self):
        view = 'full_table_initial_checkpoint_test'
        stream_id = f'public-{view}'
        stream = {'tap_stream_id': stream_id, 'stream': view, 'table_name': view}
        metadata = {(): {'schema-name': 'public'}, ('properties', 'id'): {'sql-datatype': 'integer'}}
        connection = get_test_connection()
        persisted_state = {}

        def interrupt_at_initial_state(message):
            if isinstance(message, singer.StateMessage):
                persisted_state.update(copy.deepcopy(message.value))
                raise SnapshotInterrupted()

        try:
            with connection.cursor() as cursor:
                cursor.execute(f'CREATE OR REPLACE VIEW public.{view} AS SELECT 42 AS id, 100::text::xid AS xmin')
            with patch.object(full_table.singer, 'write_message', side_effect=interrupt_at_initial_state), \
                    self.assertRaises(SnapshotInterrupted):
                full_table.sync_table(get_test_connection_config(), stream, {}, ['id'], metadata, snapshot_lsn=4000)

            self.assertEqual(persisted_state['bookmarks'][stream_id]['lsn'], 4000)
            self.assertTrue(persisted_state['bookmarks'][stream_id]['xmin'])
            with patch.object(full_table.singer, 'write_message') as output:
                full_table.sync_table(get_test_connection_config(), stream, persisted_state, ['id'], metadata,
                                      snapshot_lsn=4000)
            records = [call.args[0].record for call in output.call_args_list
                       if isinstance(call.args[0], singer.RecordMessage)]
            self.assertEqual(records, [{'id': 42}])
            self.assertIsNone(persisted_state['bookmarks'][stream_id]['xmin'])
        finally:
            with connection.cursor() as cursor:
                cursor.execute(f'DROP VIEW IF EXISTS public.{view}')
            connection.close()

    def test_restart_preserves_rows_and_cdc_boundary_when_xids_change(self):
        view = 'full_table_xmin_resume_test'
        stream = {'tap_stream_id': f'public-{view}', 'stream': view, 'table_name': view}
        metadata = {(): {'schema-name': 'public'}, ('properties', 'id'): {'sql-datatype': 'integer'}}
        config = get_test_connection_config()
        connection = get_test_connection()
        try:
            for older_xid, newer_xid, resumed_xid in [
                    ('99', '100', '100'), ('4294967290', '5', '5'), ('99', '100', '2')]:
                with self.subTest(older_xid=older_xid, newer_xid=newer_xid, resumed_xid=resumed_xid):
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f'CREATE OR REPLACE VIEW public.{view} AS '
                            'SELECT id, source_xmin AS xmin FROM '
                            '(VALUES (1, %s::xid), (2, %s::xid)) AS source_rows(id, source_xmin)',
                            (older_xid, newer_xid),
                        )
                    delivered_ids = set()
                    persisted_state = {}
                    resumed_ids = []
                    resumed_versions = []
                    records_seen = 0

                    def interrupt_after_checkpoint(message):
                        nonlocal records_seen
                        if isinstance(message, singer.RecordMessage):
                            records_seen += 1
                            if records_seen == 2:
                                raise SnapshotInterrupted()
                            delivered_ids.add(message.record['id'])
                        elif isinstance(message, singer.StateMessage):
                            persisted_state.clear()
                            persisted_state.update(copy.deepcopy(message.value))

                    def collect_resumed_row(message):
                        if isinstance(message, singer.RecordMessage):
                            delivered_ids.add(message.record['id'])
                            resumed_ids.append(message.record['id'])
                            resumed_versions.append(message.version)

                    with patch.object(full_table, 'UPDATE_BOOKMARK_PERIOD', 1), \
                            patch.object(full_table.singer, 'write_message', side_effect=interrupt_after_checkpoint), \
                            self.assertRaises(SnapshotInterrupted):
                        initial_state = {'bookmarks': {stream['tap_stream_id']: {'lsn': 4000}}}
                        full_table.sync_table(config, stream, initial_state, ['id'], metadata)

                    with connection.cursor() as cursor:
                        cursor.execute(
                            f'CREATE OR REPLACE VIEW public.{view} AS '
                            'SELECT id, source_xmin AS xmin FROM '
                            '(VALUES (1, %s::xid), (2, %s::xid)) AS source_rows(id, source_xmin)',
                            (older_xid, resumed_xid),
                        )
                    initial_version = persisted_state['bookmarks'][stream['tap_stream_id']]['version']

                    with patch.object(full_table.singer, 'write_message', side_effect=collect_resumed_row):
                        full_table.sync_table(config, stream, persisted_state, ['id'], metadata)

                    self.assertEqual(delivered_ids, {1, 2})
                    self.assertEqual(set(resumed_ids), {1, 2})
                    self.assertEqual(resumed_versions, [initial_version, initial_version])
                    self.assertEqual(persisted_state['bookmarks'][stream['tap_stream_id']]['lsn'], 4000)
        finally:
            with connection.cursor() as cursor:
                cursor.execute(f'DROP VIEW IF EXISTS public.{view}')
            connection.close()
