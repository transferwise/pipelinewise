"""Check snapshot restart ordering against PostgreSQL transaction-ID semantics."""

import copy
import unittest
from unittest.mock import patch

import singer

from tap_postgres.sync_strategies import full_table

from ..utils import get_test_connection, get_test_connection_config


class SnapshotInterrupted(Exception):
    """Stop the first snapshot after its first persisted row."""


class TestFullTableResume(unittest.TestCase):
    def test_resume_preserves_rows_across_xid_digit_changes_and_wraparound(self):
        view = 'full_table_xmin_resume_test'
        stream = {'tap_stream_id': f'public-{view}', 'stream': view, 'table_name': view}
        metadata = {(): {'schema-name': 'public'}, ('properties', 'id'): {'sql-datatype': 'integer'}}
        config = get_test_connection_config()
        connection = get_test_connection()
        try:
            for older_xid, newer_xid in [('99', '100'), ('4294967290', '5')]:
                with self.subTest(older_xid=older_xid, newer_xid=newer_xid):
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f'CREATE OR REPLACE VIEW public.{view} AS '
                            'SELECT id, source_xmin AS xmin FROM '
                            '(VALUES (1, %s::xid), (2, %s::xid)) AS source_rows(id, source_xmin)',
                            (older_xid, newer_xid),
                        )
                    delivered_ids = set()
                    persisted_state = {}
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

                    with patch.object(full_table, 'UPDATE_BOOKMARK_PERIOD', 1), \
                            patch.object(full_table.singer, 'write_message', side_effect=interrupt_after_checkpoint), \
                            self.assertRaises(SnapshotInterrupted):
                        full_table.sync_table(config, stream, {}, ['id'], metadata)

                    with patch.object(full_table.singer, 'write_message', side_effect=collect_resumed_row):
                        full_table.sync_table(config, stream, persisted_state, ['id'], metadata)

                    self.assertEqual(delivered_ids, {1, 2})
        finally:
            with connection.cursor() as cursor:
                cursor.execute(f'DROP VIEW IF EXISTS public.{view}')
            connection.close()
