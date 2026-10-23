import time
import unittest

import tap_yugabyte
from tap_yugabyte.sync_strategies import common as sync_common
from tap_yugabyte.sync_strategies import logical_replication

from tests.integration.test_full_table import _capture_singer_messages, _discover_stream
from tests.utils import (
    create_replication_slot,
    drop_replication_slot,
    drop_table,
    ensure_test_table,
    execute_retrying_serialization_failures,
    get_test_connection,
    get_test_connection_config,
    insert_record,
    lsn_to_int,
    set_replication_method_for_stream,
)


class TestLogicalReplication(unittest.TestCase):
    """End-to-end LOG_BASED lifecycle: bootstrap full-table scan, then a pure-logical run
    that observes an UPDATE, a mid-stream ADD COLUMN, an INSERT using the new column, and a DELETE."""

    maxDiff = None
    table_name = 'lr_awesome_table'
    tap_id = 'tap_logical_replication_test'

    @classmethod
    def setUpClass(cls):
        ensure_test_table({
            'name': cls.table_name,
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'name', 'type': 'character varying'},
                {'name': 'colour', 'type': 'character varying'},
                {'name': 'timestamp_ntz', 'type': 'timestamp without time zone'},
                {'name': 'timestamp_tz', 'type': 'timestamp with time zone'},
            ],
        })
        create_replication_slot(tap_id=cls.tap_id)

    @classmethod
    def tearDownClass(cls):
        drop_replication_slot(tap_id=cls.tap_id)
        drop_table(cls.table_name)

    def test_logical_replication(self):
        conn_config = get_test_connection_config()
        conn_config['tap_id'] = self.tap_id

        stream = _discover_stream(conn_config, self.table_name)
        set_replication_method_for_stream(stream, 'LOG_BASED')
        tap_stream_id = stream['tap_stream_id']
        catalog = {'streams': [stream]}

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                insert_record(cur, self.table_name, {
                    'id': 1, 'name': 'betty', 'colour': 'blue',
                    'timestamp_ntz': '2020-09-01 10:40:59', 'timestamp_tz': '2020-09-01 10:40:59+00',
                })
                insert_record(cur, self.table_name, {
                    'id': 2, 'name': 'smelly', 'colour': 'brown',
                    'timestamp_ntz': '2021-05-01 11:00:00', 'timestamp_tz': '2021-05-01 11:00:00+00',
                })
                insert_record(cur, self.table_name, {
                    'id': 3, 'name': 'pooper', 'colour': 'green',
                    'timestamp_ntz': '2022-01-01 00:00:00', 'timestamp_tz': '2022-01-01 00:00:00+00',
                })

        # First run: no lsn bookmark yet, so this is the full-table bootstrap stage.
        state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', {})

        schema_messages = [m for m in messages if m['type'] == 'SCHEMA']
        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(1, len(schema_messages))
        self.assertEqual(3, len(record_messages))
        self.assertEqual([1, 2, 3], sorted(m['record']['id'] for m in record_messages))

        bootstrap_lsn = state['bookmarks'][tap_stream_id]['lsn']
        self.assertIsNotNone(bootstrap_lsn)
        self.assertIsNone(state['bookmarks'][tap_stream_id].get('bootstrap_in_progress'))

        # Mutate the table: UPDATE, ADD COLUMN, INSERT using the new column, DELETE.
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE {self.table_name} SET colour = 'purple' WHERE id = 1")

        # A DDL statement executed immediately after a DML transaction on the SAME session,
        # with no intervening delay, can make YugabyteDB's wal2json CDC stream either raise
        # SerializationFailure on the DDL itself or silently drop the preceding DML's change
        # event - even though the DML committed - confirmed by direct probing outside this
        # suite. A brief pause plus a fresh session avoids the conflict.
        time.sleep(2)

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'ALTER TABLE {self.table_name} ADD COLUMN nice_flag boolean')
                # The ALTER TABLE just above bumps the cluster's catalog version; the very
                # next statement on this session can observe a transient, unretryable-by-YB
                # SerializationFailure until that bump propagates - retry it here.
                insert_record(cur, self.table_name, {
                    'id': 4, 'name': 'milky', 'colour': 'black', 'nice_flag': False,
                    'timestamp_ntz': '2022-06-01 00:00:00', 'timestamp_tz': '2022-06-01 00:00:00+00',
                }, retry_serialization_failures=True)
                execute_retrying_serialization_failures(
                    cur, f'DELETE FROM {self.table_name} WHERE id = 3')

        # Second run: an lsn bookmark now exists, so this is a pure-logical (streaming) run.
        state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', state)

        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(3, len(record_messages))

        updated = next(m['record'] for m in record_messages if m['record'].get('id') == 1)
        self.assertEqual('purple', updated['colour'])
        self.assertIsNone(updated.get('_sdc_deleted_at'))

        inserted = next(m['record'] for m in record_messages if m['record'].get('id') == 4)
        self.assertEqual('black', inserted['colour'])
        self.assertEqual(False, inserted['nice_flag'])

        deleted = next(m['record'] for m in record_messages if m['record'].get('id') == 3)
        self.assertIsNotNone(deleted.get('_sdc_deleted_at'))

        schema_messages = [m for m in messages if m['type'] == 'SCHEMA']
        self.assertGreaterEqual(len(schema_messages), 1)
        self.assertEqual(
            sync_common.PATCH_RECORD_UPDATE_MODE,
            schema_messages[0]['schema'][sync_common.RECORD_UPDATE_MODE_SCHEMA_KEY],
        )

        state_messages = [m for m in messages if m['type'] == 'STATE']
        self.assertGreaterEqual(len(state_messages), 1)
        self.assertEqual(state, state_messages[-1]['value'])
        self.assertGreater(state['bookmarks'][tap_stream_id]['lsn'], bootstrap_lsn)


class TestUnselectedTableSlotAdvancement(unittest.TestCase):
    """Changes to a table that is NOT selected must still advance the slot's LSN
    (wal2json still emits BEGIN/COMMIT markers for its transactions) while emitting
    zero RECORD messages."""

    maxDiff = None
    selected_table = 'lr_selected_table'
    unselected_table = 'lr_unselected_table'
    tap_id = 'tap_unselected_table_test'

    @classmethod
    def setUpClass(cls):
        for table in (cls.selected_table, cls.unselected_table):
            ensure_test_table({
                'name': table,
                'columns': [
                    {'name': 'id', 'type': 'integer', 'primary_key': True},
                    {'name': 'val', 'type': 'character varying'},
                ],
            })
        create_replication_slot(tap_id=cls.tap_id)

    @classmethod
    def tearDownClass(cls):
        drop_replication_slot(tap_id=cls.tap_id)
        drop_table(cls.selected_table)
        drop_table(cls.unselected_table)

    def test_unselected_table_slot_advancement(self):
        conn_config = get_test_connection_config()
        conn_config['tap_id'] = self.tap_id

        selected_stream = _discover_stream(conn_config, self.selected_table)
        unselected_stream = _discover_stream(conn_config, self.unselected_table)
        set_replication_method_for_stream(selected_stream, 'LOG_BASED')
        tap_stream_id = selected_stream['tap_stream_id']
        catalog = {'streams': [selected_stream, unselected_stream]}

        # Bootstrap the selected stream only (unselected_stream is never selected).
        state, _ = _capture_singer_messages(
            tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', {})
        initial_lsn = state['bookmarks'][tap_stream_id]['lsn']

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                insert_record(cur, self.unselected_table, {'id': 1, 'val': 'untracked'})

        state, messages = _capture_singer_messages(
            tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', state)

        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(0, len(record_messages))
        self.assertGreater(state['bookmarks'][tap_stream_id]['lsn'], initial_lsn)


class TestIdleRunAndSlotAcknowledgement(unittest.TestCase):
    """YugabyteDB never streams pg_logical_emit_message, so an idle run ends at the idle timeout and keeps
    its bookmark. The slot moves once a run acknowledges a streamed commit from its input state; the
    bootstrap boundary is not a streamed commit, so acknowledging it leaves the slot in place."""

    maxDiff = None
    table_name = 'lr_idle_ack_table'
    tap_id = 'tap_idle_ack_test'

    @classmethod
    def setUpClass(cls):
        ensure_test_table({
            'name': cls.table_name,
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'val', 'type': 'character varying'},
            ],
        })
        create_replication_slot(tap_id=cls.tap_id)

    @classmethod
    def tearDownClass(cls):
        drop_replication_slot(tap_id=cls.tap_id)
        drop_table(cls.table_name)

    @staticmethod
    def _confirmed_flush_lsn(slot_name):
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = %s', (slot_name,))
                return lsn_to_int(cur.fetchone()[0])

    def test_idle_run_stops_then_acknowledged_commit_advances_slot(self):
        conn_config = get_test_connection_config()
        conn_config['tap_id'] = self.tap_id
        slot_name = logical_replication.generate_replication_slot_name(conn_config['dbname'], self.tap_id)

        stream = _discover_stream(conn_config, self.table_name)
        set_replication_method_for_stream(stream, 'LOG_BASED')
        tap_stream_id = stream['tap_stream_id']
        catalog = {'streams': [stream]}

        state, _ = _capture_singer_messages(tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', {})
        initial_lsn = state['bookmarks'][tap_stream_id]['lsn']
        baseline_confirmed_flush_lsn = self._confirmed_flush_lsn(slot_name)

        started = time.monotonic()
        state, messages = _capture_singer_messages(tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', state)
        self.assertLess(time.monotonic() - started, conn_config['logical_poll_total_seconds'] + 30)
        self.assertEqual([], [m for m in messages if m['type'] == 'RECORD'])
        self.assertEqual(initial_lsn, state['bookmarks'][tap_stream_id]['lsn'])

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                insert_record(cur, self.table_name, {'id': 1, 'val': 'first-commit'})

        state, messages = _capture_singer_messages(tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', state)
        committed_lsn = state['bookmarks'][tap_stream_id]['lsn']
        self.assertGreater(committed_lsn, initial_lsn)
        self.assertEqual([1], [m['record']['id'] for m in messages if m['type'] == 'RECORD'])

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                insert_record(cur, self.table_name, {'id': 2, 'val': 'second-commit'})

        state, messages = _capture_singer_messages(tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', state)
        self.assertEqual([2], [m['record']['id'] for m in messages if m['type'] == 'RECORD'])
        self.assertGreater(state['bookmarks'][tap_stream_id]['lsn'], committed_lsn)
        self.assertGreater(committed_lsn, baseline_confirmed_flush_lsn)
        self.assertEqual(committed_lsn, self._confirmed_flush_lsn(slot_name))
