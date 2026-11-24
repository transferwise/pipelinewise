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
    its bookmark. A run acknowledges two below the streamed commit in its input state, so the slot trails
    the target by one transaction and the bootstrap boundary leaves it in place."""

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

        # The slot is acknowledged two below the bookmark, so the last acknowledged transaction replays once.
        state, messages = _capture_singer_messages(tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', state)
        self.assertEqual([1, 2], [m['record']['id'] for m in messages if m['type'] == 'RECORD'])
        second_lsn = state['bookmarks'][tap_stream_id]['lsn']
        self.assertGreater(second_lsn, committed_lsn)
        self.assertGreater(committed_lsn, baseline_confirmed_flush_lsn)
        offset = logical_replication.SLOT_FEEDBACK_LSN_OFFSET
        self.assertLessEqual(self._confirmed_flush_lsn(slot_name), committed_lsn - offset)

        state, messages = _capture_singer_messages(tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', state)
        self.assertEqual([2], [m['record']['id'] for m in messages if m['type'] == 'RECORD'])
        self.assertEqual(second_lsn, state['bookmarks'][tap_stream_id]['lsn'])
        confirmed_flush_lsn = self._confirmed_flush_lsn(slot_name)
        self.assertGreater(confirmed_flush_lsn, baseline_confirmed_flush_lsn)
        self.assertLessEqual(confirmed_flush_lsn, second_lsn - offset)


class TestReplicaIdentityNothing(unittest.TestCase):
    """REPLICA IDENTITY NOTHING streams no updates, so a LOG_BASED sync stops before reading the slot."""

    table_name = 'lr_identity_nothing_table'

    def setUp(self):
        ensure_test_table({'name': self.table_name, 'columns': [{'name': 'id', 'type': 'integer', 'primary_key': True}]})
        self.addCleanup(drop_table, self.table_name)
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'ALTER TABLE {self.table_name} REPLICA IDENTITY NOTHING')

    def test_log_based_sync_rejects_replica_identity_nothing(self):
        conn_config = get_test_connection_config()
        conn_config['tap_id'] = 'tap_identity_nothing_test'
        stream = _discover_stream(conn_config, self.table_name)
        set_replication_method_for_stream(stream, 'LOG_BASED')

        with self.assertRaisesRegex(Exception, f'REPLICA IDENTITY NOTHING: public.{self.table_name}'):
            tap_yugabyte.do_sync(conn_config, {'streams': [stream]}, 'LOG_BASED', {})


class TestIntraTransactionBeforeImage(unittest.TestCase):
    """Rows inserted and then changed in one transaction stream correctly on DEFAULT and FULL tables.

    Needs cdc_enable_intra_transactional_before_image=true on the YB-TServers, as set in this connector's
    docker-compose.yml. Without it, the second run fails with BeforeImageUnavailableError. The unselected FULL table
    covers the failure's database-wide reach: YugabyteDB fails before wal2json filters the selected tables.
    """

    maxDiff = None
    tap_id = 'tap_intra_txn_before_image_test'
    selected_tables = {'lr_intra_txn_default': 'DEFAULT', 'lr_intra_txn_full': 'FULL'}
    unselected_table = 'lr_intra_txn_unselected'
    transactions = [
        ["INSERT INTO {t} VALUES (1, 'a1', 1)", "UPDATE {t} SET a = 'a1x' WHERE id = 1"],
        ["INSERT INTO {t} VALUES (2, 'a2', 2)", 'DELETE FROM {t} WHERE id = 2'],
        ["INSERT INTO {t} VALUES (3, 'a3', 3)", "UPDATE {t} SET a = 'a3x' WHERE id = 3",
         'DELETE FROM {t} WHERE id = 3'],
        ["INSERT INTO {t} VALUES (4, 'a4', 4)", 'UPDATE {t} SET id = 40 WHERE id = 4'],
        ["INSERT INTO {t} VALUES (6, 'a6', 6)", "UPDATE {t} SET a = 'a6x' WHERE id = 6",
         'UPDATE {t} SET b = 66 WHERE id = 6'],
        ["UPDATE {t} SET a = 'a5x' WHERE id = 5", 'DELETE FROM {t} WHERE id = 5'],
        ["UPDATE {t} SET a = 'a7x' WHERE id = 7", 'UPDATE {t} SET b = 77 WHERE id = 7'],
    ]

    @classmethod
    def setUpClass(cls):
        identities = {**cls.selected_tables, cls.unselected_table: 'FULL'}
        for table_name, identity in identities.items():
            ensure_test_table({
                'name': table_name,
                'columns': [
                    {'name': 'id', 'type': 'integer', 'primary_key': True},
                    {'name': 'a', 'type': 'character varying'},
                    {'name': 'b', 'type': 'integer'},
                ],
            })
            with get_test_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f'ALTER TABLE {table_name} REPLICA IDENTITY {identity}')
                    cur.execute(f"INSERT INTO {table_name} VALUES (5, 'a5', 5), (7, 'a7', 7)")
        # The slot fixes each table's replica identity when it is created.
        create_replication_slot(tap_id=cls.tap_id)

    @classmethod
    def tearDownClass(cls):
        drop_replication_slot(tap_id=cls.tap_id)
        for table_name in [*cls.selected_tables, cls.unselected_table]:
            drop_table(table_name)

    @staticmethod
    def _replay(records):
        """Apply PATCH records and deletes in order, as a target would."""
        rows = {}
        for record in records:
            if record.get('_sdc_deleted_at'):
                rows.pop(record['id'], None)
            else:
                rows.setdefault(record['id'], {}).update(
                    {key: value for key, value in record.items() if not key.startswith('_sdc_')})
        return rows

    def test_inserted_then_changed_rows_replay_to_source(self):
        conn_config = get_test_connection_config()
        conn_config['tap_id'] = self.tap_id
        streams = []
        for table_name in self.selected_tables:
            stream = _discover_stream(conn_config, table_name)
            set_replication_method_for_stream(stream, 'LOG_BASED')
            streams.append(stream)
        catalog = {'streams': streams}

        state, bootstrap_messages = _capture_singer_messages(
            tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', {})
        bootstrap_lsns = {s['tap_stream_id']: state['bookmarks'][s['tap_stream_id']]['lsn'] for s in streams}

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                for table_name in [*self.selected_tables, self.unselected_table]:
                    for statements in self.transactions:
                        cur.execute('BEGIN')
                        for statement in statements:
                            cur.execute(statement.format(t=table_name))
                        cur.execute('COMMIT')

        state, messages = _capture_singer_messages(tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', state)

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                for stream in streams:
                    with self.subTest(table=stream['table_name']):
                        records = [m['record'] for m in [*bootstrap_messages, *messages]
                                   if m['type'] == 'RECORD' and m['stream'] == stream['tap_stream_id']]
                        cur.execute(f"SELECT id, a, b FROM {stream['table_name']} ORDER BY id")
                        source = {row[0]: {'id': row[0], 'a': row[1], 'b': row[2]} for row in cur.fetchall()}
                        self.assertEqual(source, self._replay(records))
                        self.assertGreater(
                            state['bookmarks'][stream['tap_stream_id']]['lsn'], bootstrap_lsns[stream['tap_stream_id']])
