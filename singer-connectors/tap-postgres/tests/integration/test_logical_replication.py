import contextlib
import json
import threading
import time
import unittest
import unittest.mock

import tap_postgres
from tap_postgres.sync_strategies import common, logical_replication

from ..utils import get_test_connection_config, ensure_test_table, create_replication_slot, drop_replication_slot, \
    set_replication_method_for_stream, get_test_connection, insert_record, drop_table, SingerOutput


class TestLogicalReplication(unittest.TestCase):
    table_name = None
    maxDiff = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.table_name = 'awesome_table'
        table_spec = {
            "columns": [
                {"name": "id", "type": "serial", "primary_key": True},
                {"name": 'name', "type": "character varying"},
                {"name": 'colour', "type": "character varying"},
                {"name": 'timestamp_ntz', "type": "timestamp without time zone"},
                {"name": 'timestamp_tz', "type": "timestamp with time zone"},
            ],
            "name": cls.table_name}

        ensure_test_table(table_spec)
        create_replication_slot()

        cls.config = get_test_connection_config()

        tap_postgres.dump_catalog = lambda catalog: True

    @classmethod
    def tearDownClass(cls) -> None:
        drop_replication_slot()
        drop_table(cls.table_name)

    def test_logical_replication(self):
        streams = tap_postgres.do_discovery(self.config)

        awesome_stream = [s for s in streams if s['tap_stream_id'] == f'public-{self.table_name}'][0]
        awesome_stream = set_replication_method_for_stream(awesome_stream, 'LOG_BASED')

        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                records = [
                    {
                        'name': 'betty',
                        'colour': 'blue',
                        'timestamp_ntz': '2020-09-01 10:40:59',
                        'timestamp_tz': '2020-09-01 00:50:59+02'
                    },
                    {
                        'name': 'smelly',
                        'colour': 'brown',
                        'timestamp_ntz': '2020-09-01 10:40:59 BC',
                        'timestamp_tz': '2020-09-01 00:50:59+02 BC'
                    },
                    {
                        'name': 'pooper',
                        'colour': 'green',
                        'timestamp_ntz': '30000-09-01 10:40:59',
                        'timestamp_tz': '10000-09-01 00:50:59+02'
                    }
                ]

                for rec in records:
                    insert_record(cur, self.table_name, rec)
        finally:
            conn.close()

        state = {}

        my_stdout = SingerOutput()

        # Would use full initial sync
        with contextlib.redirect_stdout(my_stdout):
            state = tap_postgres.do_sync(self.config, {'streams': [awesome_stream]}, 'LOG_BASED', state, None)

        print('stdout from full initial sync: ', my_stdout.getvalue())
        messages = [json.loads(msg) for msg in my_stdout.getvalue().splitlines()]
        messages = list(filter(lambda msg: msg['type'] != 'ACTIVATE_VERSION', messages))

        self.assertEqual(messages[0]['type'], 'SCHEMA')
        self.assertEqual(messages[0]['stream'], f'public-{self.table_name}')
        self.assertNotIn(common.RECORD_UPDATE_MODE_SCHEMA_KEY, messages[0]['schema'])

        self.assertEqual(messages[1]['type'], 'STATE')
        self.assertEqual(messages[0]['stream'], f'public-{self.table_name}')

        full_records = {
            message['record']['id']: message for message in messages if message['type'] == 'RECORD'
        }
        self.assertEqual(set(full_records), {1, 2, 3})
        self.assertDictEqual(full_records[1], {
            'type': 'RECORD',
            'stream': f'public-{self.table_name}',
            'record': {
                'colour': 'blue',
                'id': 1,
                'name': 'betty',
                'timestamp_ntz': '2020-09-01T10:40:59+00:00',
                'timestamp_tz': '2020-08-31T22:50:59+00:00',
            },
            'time_extracted': unittest.mock.ANY,
            'version': unittest.mock.ANY
        })
        self.assertDictEqual(full_records[2], {
            'type': 'RECORD',
            'stream': f'public-{self.table_name}',
            'record': {
                'colour': 'brown',
                'id': 2,
                'name': 'smelly',
                'timestamp_ntz': '9999-12-31T23:59:59.999000+00:00',
                'timestamp_tz': '9999-12-31T23:59:59.999000+00:00',
            },
            'time_extracted': unittest.mock.ANY,
            'version': unittest.mock.ANY
        })
        self.assertDictEqual(full_records[3], {
            'type': 'RECORD',
            'stream': f'public-{self.table_name}',
            'record': {
                'colour': 'green',
                'id': 3,
                'name': 'pooper',
                'timestamp_ntz': '9999-12-31T23:59:59.999000+00:00',
                'timestamp_tz': '9999-12-31T23:59:59.999000+00:00',
            },
            'time_extracted': unittest.mock.ANY,
            'version': unittest.mock.ANY
        })
        self.assertEqual(messages[5]['type'], 'STATE')

        self.assertDictEqual(state, next(message['value'] for message in reversed(messages)
                                         if message['type'] == 'STATE'))
        self.assertIsNotNone(state['bookmarks']['public-awesome_table']['lsn'])

        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"update {self.table_name} set id=10, colour='purple' where name='betty';")
        finally:
            conn.close()

        # clear the io
        my_stdout.seek(0)
        my_stdout.truncate()

        # Would use logical replication
        with contextlib.redirect_stdout(my_stdout):
            state = tap_postgres.do_sync(self.config, {'streams': [awesome_stream]}, 'LOG_BASED', state, None)

        messages = [json.loads(msg) for msg in my_stdout.getvalue().splitlines()]
        messages = list(filter(lambda msg: msg['type'] != 'ACTIVATE_VERSION', messages))

        self.assertEqual(messages[0]['type'], 'SCHEMA')
        self.assertEqual(
            messages[0]['schema'][common.RECORD_UPDATE_MODE_SCHEMA_KEY],
            common.PATCH_RECORD_UPDATE_MODE,
        )
        record_messages = [message for message in messages if message['type'] == 'RECORD']
        self.assertDictEqual(record_messages[0], {
            'type': 'RECORD',
            'stream': f'public-{self.table_name}',
            'record': {
                '_sdc_deleted_at': unittest.mock.ANY,
                'id': 1,
            },
            'time_extracted': unittest.mock.ANY,
            'version': unittest.mock.ANY,
        })
        self.assertDictEqual(record_messages[1], {
            'type': 'RECORD',
            'stream': f'public-{self.table_name}',
            'record': {
                '_sdc_deleted_at': None,
                'colour': 'purple',
                'id': 10,
                'name': 'betty',
                'timestamp_ntz': '2020-09-01T10:40:59+00:00',
                'timestamp_tz': '2020-08-31T22:50:59+00:00',
            },
            'time_extracted': unittest.mock.ANY,
            'version': unittest.mock.ANY,
        })
        self.assertEqual(messages[-1]['type'], 'STATE')
        self.assertDictEqual(state, messages[-1]['value'])

        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"alter table {self.table_name} add column nice_flag bool default true;")
                insert_record(cur, self.table_name, {
                    'name': 'milky',
                    'colour': 'black',
                    'timestamp_ntz': '2022-09-01 10:40:59',
                    'timestamp_tz': '10000-09-01 00:50:59+02',
                    'nice_flag': False
                })
                cur.execute(f"delete from {self.table_name} where name='pooper';")
                cur.execute(f"truncate {self.table_name};")
        finally:
            conn.close()

        my_stdout.seek(0)
        my_stdout.truncate()
        with contextlib.redirect_stdout(my_stdout):
            state = tap_postgres.do_sync(
                self.config, {'streams': [awesome_stream]}, 'LOG_BASED', state, None)

        messages = [json.loads(msg) for msg in my_stdout.getvalue().splitlines()]
        messages = list(filter(lambda msg: msg['type'] != 'ACTIVATE_VERSION', messages))
        record_messages = [message for message in messages if message['type'] == 'RECORD']
        self.assertDictEqual(record_messages[0], {
            'type': 'RECORD',
            'stream': f'public-{self.table_name}',
            'record': {
                '_sdc_deleted_at': None,
                'colour': 'black',
                'id': 4,
                'name': 'milky',
                'nice_flag': False,
                'timestamp_ntz': '2022-09-01T10:40:59+00:00',
                'timestamp_tz': '9999-12-31T23:59:59.999+00:00',
            },
            'time_extracted': unittest.mock.ANY,
            'version': unittest.mock.ANY,
        })
        self.assertDictEqual(record_messages[1], {
            'type': 'RECORD',
            'stream': f'public-{self.table_name}',
            'record': {
                '_sdc_deleted_at': unittest.mock.ANY,
                'id': 3,
            },
            'time_extracted': unittest.mock.ANY,
            'version': unittest.mock.ANY,
        })
        self.assertEqual(messages[-1]['type'], 'STATE')
        self.assertDictEqual(state, messages[-1]['value'])


class TestUnselectedTableSlotAdvancement(unittest.TestCase):
    """Test that WAL slot LSN advances even when only non-selected tables have activity.

    A decoded logical-message transaction advances the slot beyond unrelated WAL without
    emitting records for unselected tables.
    """
    selected_table = None
    unselected_table = None
    maxDiff = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.selected_table = 'selected_table'
        cls.unselected_table = 'unselected_table'

        selected_spec = {
            "columns": [
                {"name": "id", "type": "serial", "primary_key": True},
                {"name": "val", "type": "character varying"},
            ],
            "name": cls.selected_table,
        }
        unselected_spec = {
            "columns": [
                {"name": "id", "type": "serial", "primary_key": True},
                {"name": "val", "type": "character varying"},
            ],
            "name": cls.unselected_table,
        }

        ensure_test_table(selected_spec)
        ensure_test_table(unselected_spec)
        create_replication_slot()

        cls.config = get_test_connection_config()
        tap_postgres.dump_catalog = lambda catalog: True

    @classmethod
    def tearDownClass(cls) -> None:
        drop_replication_slot()
        drop_table(cls.selected_table)
        drop_table(cls.unselected_table)

    def test_unselected_table_activity_does_not_emit_records(self):
        """Only decoded commits advance state; PostgreSQL 15+ filters empty transactions."""

        # Discover streams, select only `selected_table` for LOG_BASED
        streams = tap_postgres.do_discovery(self.config)
        selected_stream = [s for s in streams if s['tap_stream_id'] == f'public-{self.selected_table}'][0]
        selected_stream = set_replication_method_for_stream(selected_stream, 'LOG_BASED')

        # Insert a row into the selected table so initial sync has something to process
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                insert_record(cur, self.selected_table, {'val': 'seed'})
        finally:
            conn.close()

        # Initial sync to establish bookmarks
        state = {}
        my_stdout = SingerOutput()
        with contextlib.redirect_stdout(my_stdout):
            state = tap_postgres.do_sync(self.config, {'streams': [selected_stream]}, 'LOG_BASED', state, None)

        # Capture LSN after initial sync
        initial_lsn = state['bookmarks'][f'public-{self.selected_table}']['lsn']
        self.assertIsNotNone(initial_lsn, "Initial sync should set an LSN bookmark")

        # Now insert rows ONLY into the unselected table
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                for i in range(5):
                    insert_record(cur, self.unselected_table, {'val': f'noise_{i}'})
        finally:
            conn.close()

        # Run sync again without a published row change.
        my_stdout.seek(0)
        my_stdout.truncate()
        with contextlib.redirect_stdout(my_stdout):
            state = tap_postgres.do_sync(self.config, {'streams': [selected_stream]}, 'LOG_BASED', state, None)

        # Assert that the LSN bookmark has advanced past the unselected-table activity
        new_lsn = state['bookmarks'][f'public-{self.selected_table}']['lsn']
        with get_test_connection() as connection:
            version = connection.server_version
        if version >= 150000:
            self.assertEqual(new_lsn, initial_lsn)
        else:
            self.assertGreaterEqual(new_lsn, initial_lsn)

        # Verify no RECORD messages were emitted (only the unselected table had activity)
        messages = [json.loads(msg) for msg in my_stdout.getvalue().splitlines()]
        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len(record_messages), 0,
                         "No RECORD messages should be emitted for unselected table activity")


class TestPgoutputPreflightSafety(unittest.TestCase):
    def test_deferrable_primary_key_is_rejected_before_publishing_or_breaking_source_updates(self):
        table = 'deferrable_identity_review'
        config = {**get_test_connection_config(), 'tap_id': 'deferrable_identity_review'}
        publication = logical_replication.generate_publication_name(config['tap_id'])
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE TABLE {table} (id integer PRIMARY KEY DEFERRABLE, value text)')
                cur.execute(f"INSERT INTO {table} VALUES (1, 'before')")
            stream = next(s for s in tap_postgres.do_discovery(config) if s['table_name'] == table)
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            with self.assertRaisesRegex(logical_replication.ReplicationSlotMigrationError, 'non-deferrable'):
                logical_replication.prepare_publication(config, [stream])
            with conn.cursor() as cur:
                cur.execute('SELECT 1 FROM pg_publication WHERE pubname = %s', (publication,))
                self.assertIsNone(cur.fetchone())
                cur.execute(f"UPDATE {table} SET value = 'after' WHERE id = 1")
                self.assertEqual(cur.rowcount, 1)
        finally:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS {table}')
            conn.close()

    def test_publication_membership_is_additive_until_explicit_reconcile(self):
        managed_tables = ['publication_existing_review', 'publication_snapshot_review']
        external_table = 'publication_dba_review'
        tables = [*managed_tables, external_table]
        config = {**get_test_connection_config(), 'tap_id': 'publication_subset_review'}
        publication = logical_replication.generate_publication_name(config['tap_id'])
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                for table in tables:
                    cur.execute(f'CREATE TABLE {table} (id integer PRIMARY KEY)')
            streams = {
                stream['table_name']: set_replication_method_for_stream(stream, 'LOG_BASED')
                for stream in tap_postgres.do_discovery(config)
                if stream['table_name'] in managed_tables
            }

            logical_replication.prepare_publication(config, [streams[managed_tables[0]]])
            with conn.cursor() as cur:
                cur.execute(
                    f'ALTER PUBLICATION {publication} ADD TABLE public.{external_table}')

            logical_replication.prepare_publication(config, [streams[managed_tables[1]]])
            with conn.cursor() as cur:
                cur.execute('SELECT tablename FROM pg_publication_tables WHERE pubname = %s', (publication,))
                self.assertEqual({row[0] for row in cur.fetchall()}, set(tables))
                cur.execute(
                    "SELECT pg_catalog.obj_description(oid, 'pg_publication') "
                    'FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (publication,),
                )
                _, _, tracked = logical_replication._decode_publication_fence_comment(
                    cur.fetchone()[0])
                self.assertEqual(
                    tracked,
                    {('public', table) for table in managed_tables},
                )

            logical_replication.prepare_publication(
                config, [streams[managed_tables[0]]], reconcile=True)
            with conn.cursor() as cur:
                cur.execute('SELECT tablename FROM pg_publication_tables WHERE pubname = %s', (publication,))
                self.assertEqual(
                    {row[0] for row in cur.fetchall()},
                    {managed_tables[0], external_table},
                )

            logical_replication.prepare_publication(config, [], reconcile=True)
            with conn.cursor() as cur:
                cur.execute('SELECT tablename FROM pg_publication_tables WHERE pubname = %s', (publication,))
                self.assertEqual({row[0] for row in cur.fetchall()}, {external_table})
                cur.execute(
                    "SELECT pg_catalog.obj_description(oid, 'pg_publication') "
                    'FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (publication,),
                )
                self.assertEqual(
                    logical_replication._decode_publication_fence_comment(cur.fetchone()[0]),
                    ('ready', None, set()),
                )
        finally:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                for table in tables:
                    cur.execute(f'DROP TABLE IF EXISTS {table}')
            conn.close()

    def test_reconcile_does_not_create_an_absent_publication(self):
        table = 'publication_absent_reconcile_review'
        config = {**get_test_connection_config(), 'tap_id': 'publication_absent_review'}
        publication = logical_replication.generate_publication_name(config['tap_id'])
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'CREATE TABLE {table} (id integer PRIMARY KEY)')
            stream = next(
                set_replication_method_for_stream(stream, 'LOG_BASED')
                for stream in tap_postgres.do_discovery(config)
                if stream['table_name'] == table
            )

            logical_replication.prepare_publication(
                config, [stream], reconcile=True)

            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (publication,),
                )
                self.assertIsNone(cur.fetchone())
        finally:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS {table}')
            conn.close()

    def test_publication_lock_wait_is_bounded(self):
        table = 'publication_lock_review'
        config = {**get_test_connection_config(), 'tap_id': 'publication_lock_review',
                  'publication_fence_timeout_seconds': 0.1}
        publication = logical_replication.generate_publication_name(config['tap_id'])
        conn = get_test_connection()
        blocker = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE TABLE {table} (id integer PRIMARY KEY)')
            stream = next(s for s in tap_postgres.do_discovery(config) if s['table_name'] == table)
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            blocker.autocommit = False
            with blocker.cursor() as cur:
                cur.execute(f'LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE')
            started = time.monotonic()
            with self.assertRaisesRegex(Exception, 'timeout'):
                logical_replication.prepare_publication(config, [stream])
            self.assertLess(time.monotonic() - started, 5)
        finally:
            blocker.close()
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS {table}')
            conn.close()

    def test_publication_fence_waits_for_existing_writer(self):
        config = {**get_test_connection_config(), 'publication_fence_timeout_seconds': 5}
        transaction = get_test_connection()
        errors = []
        try:
            transaction.autocommit = False
            with transaction.cursor() as cur:
                cur.execute('SELECT txid_current()')
            waiter = threading.Thread(target=lambda: self._run_fence(config, errors), daemon=True)
            waiter.start()
            time.sleep(0.25)
            self.assertTrue(waiter.is_alive())
            transaction.commit()
            waiter.join(timeout=5)
            self.assertFalse(waiter.is_alive())
            self.assertEqual(errors, [])
        finally:
            transaction.close()

    def test_read_only_transaction_can_write_after_publication_fence_without_losing_row(self):
        table = 'publication_reader_upgrade'
        config = {**get_test_connection_config(), 'tap_id': 'publication_reader_upgrade'}
        publication = logical_replication.generate_publication_name(config['tap_id'])
        slot = logical_replication.generate_replication_slot_name(config['tap_id'])
        conn = get_test_connection()
        reader = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE TABLE {table} (id integer PRIMARY KEY)')
                cur.execute(f'INSERT INTO {table} VALUES (7)')
                cur.execute('SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s)', (slot, 'pgoutput'))
                start_lsn = logical_replication.lsn_to_int(cur.fetchone()[0])
            stream = next(s for s in tap_postgres.do_discovery(config) if s['table_name'] == table)
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            reader.autocommit = False
            with reader.cursor() as cur:
                cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                cur.execute(f'SELECT * FROM {table}')
            logical_replication.prepare_publication(config, [stream], fresh_start=True)
            with reader.cursor() as cur:
                cur.execute(f'UPDATE {table} SET id = 8 WHERE id = 7')
                cur.execute(f'INSERT INTO {table} VALUES (42)')
            reader.commit()
            output = SingerOutput()
            state = {'bookmarks': {stream['tap_stream_id']: {
                'lsn': start_lsn, 'version': 1, 'last_replication_method': 'LOG_BASED'}}}
            with contextlib.redirect_stdout(output):
                logical_replication.sync_tables(config, [stream], state, start_lsn, None)
            records = [json.loads(line)['record'] for line in output.getvalue().splitlines()
                       if json.loads(line)['type'] == 'RECORD']
            self.assertEqual([record['id'] for record in records], [7, 8, 42])
            self.assertIsNotNone(records[0]['_sdc_deleted_at'])
        finally:
            reader.close()
            with conn.cursor() as cur:
                cur.execute('SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots '
                            'WHERE slot_name = %s', (slot,))
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS {table}')
            conn.close()

    def test_publication_fence_does_not_wait_for_read_only_virtual_xid(self):
        config = get_test_connection_config()
        config['publication_fence_timeout_seconds'] = 5
        transaction = get_test_connection()
        observer = get_test_connection()
        errors = []
        try:
            transaction.autocommit = False
            with transaction.cursor() as cur:
                cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                cur.execute('SELECT 1')
                cur.execute('SELECT pg_backend_pid()')
                backend_pid = cur.fetchone()[0]
            with observer.cursor() as cur:
                cur.execute(
                    'SELECT backend_xid FROM pg_catalog.pg_stat_activity WHERE pid = %s',
                    (backend_pid,),
                )
                self.assertIsNone(cur.fetchone()[0])

            waiter = threading.Thread(
                target=lambda: self._run_fence(config, errors),
                daemon=True,
            )
            waiter.start()
            waiter.join(timeout=2)
            self.assertFalse(waiter.is_alive(), 'publication fence waited for a read-only transaction')
            transaction.commit()
            self.assertEqual(errors, [])
        finally:
            transaction.close()
            observer.close()

    @staticmethod
    def _run_fence(config, errors):
        try:
            logical_replication._wait_for_prepublication_transactions(config)
        except Exception as ex:  # pragma: no cover - reported by the calling test
            errors.append(ex)

    def test_selected_generated_column_is_rejected_by_real_catalog(self):
        table_name = 'generated_column_logical_test'
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
                cur.execute(
                    f'CREATE TABLE public.{table_name} ('
                    'id integer PRIMARY KEY, '
                    'amount numeric, '
                    'total numeric GENERATED ALWAYS AS (amount * 2) STORED)'
                )

            streams = tap_postgres.do_discovery(get_test_connection_config())
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{table_name}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            with self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError,
                    'generated columns'):
                logical_replication.prepare_publication(
                    get_test_connection_config(), [stream])
        finally:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
            conn.close()

    def test_identityless_table_is_rejected_before_publication_mutation(self):
        table_name = 'identityless_logical_test'
        config = get_test_connection_config()
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
                cur.execute(f'CREATE TABLE public.{table_name} (id integer, value text)')
                cur.execute(
                    """
                    SELECT schemaname, tablename
                      FROM pg_catalog.pg_publication_tables
                     WHERE pubname = %s
                    """,
                    (logical_replication.generate_publication_name(config['tap_id']),),
                )
                membership_before = set(cur.fetchall())

            streams = tap_postgres.do_discovery(config)
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{table_name}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            with self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError,
                    'REPLICA IDENTITY DEFAULT backed by a valid non-deferrable primary key'):
                logical_replication.prepare_publication(config, [stream])

            with conn.cursor() as cur:
                cur.execute(f'ALTER TABLE public.{table_name} REPLICA IDENTITY FULL')
            with self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError,
                    'REPLICA IDENTITY DEFAULT backed by a valid non-deferrable primary key'):
                logical_replication.prepare_publication(config, [stream])

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT schemaname, tablename
                      FROM pg_catalog.pg_publication_tables
                     WHERE pubname = %s
                    """,
                    (logical_replication.generate_publication_name(config['tap_id']),),
                )
                self.assertEqual(set(cur.fetchall()), membership_before)
        finally:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
            conn.close()

    def test_replica_identity_index_different_from_primary_key_is_rejected(self):
        table_name = 'alternate_replica_identity_test'
        identity_index = f'{table_name}_alternate_id'
        config = {**get_test_connection_config(), 'tap_id': 'alternate_identity'}
        publication = logical_replication.generate_publication_name(config['tap_id'])
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
                cur.execute(
                    f'CREATE TABLE public.{table_name} ('
                    'id integer PRIMARY KEY, alternate_id integer NOT NULL, value text)')
                cur.execute(
                    f'CREATE UNIQUE INDEX {identity_index} '
                    f'ON public.{table_name} (alternate_id)')
                cur.execute(
                    f'ALTER TABLE public.{table_name} '
                    f'REPLICA IDENTITY USING INDEX {identity_index}')

            streams = tap_postgres.do_discovery(config)
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{table_name}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            with self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError,
                    'REPLICA IDENTITY DEFAULT backed by a valid non-deferrable primary key'):
                logical_replication.prepare_publication(config, [stream])

            with conn.cursor() as cur:
                cur.execute(f'ALTER TABLE public.{table_name} REPLICA IDENTITY DEFAULT')
            prepared = logical_replication.prepare_publication(config, [stream])
            self.assertEqual(
                str(prepared),
                logical_replication.generate_publication_name(config['tap_id']),
            )
        finally:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
            conn.close()

    def test_partition_root_identity_is_validated_before_publication_mutation(self):
        root_table = 'identityless_partition_root_test'
        leaf_table = f'{root_table}_leaf'
        config = get_test_connection_config()
        config['tap_id'] = 'partition_identity_test'
        publication = logical_replication.generate_publication_name(config['tap_id'])
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{root_table} CASCADE')
                cur.execute(
                    f'CREATE TABLE public.{root_table} (id integer, value text) '
                    'PARTITION BY RANGE (id)')
                cur.execute(
                    f'CREATE TABLE public.{leaf_table} PARTITION OF public.{root_table} '
                    'FOR VALUES FROM (0) TO (100)')

            streams = tap_postgres.do_discovery(config)
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{root_table}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            with self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError,
                    'REPLICA IDENTITY DEFAULT backed by a valid non-deferrable primary key'):
                logical_replication.prepare_publication(config, [stream])

            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (publication,),
                )
                self.assertIsNone(cur.fetchone())
                cur.execute(f'ALTER TABLE public.{root_table} ADD PRIMARY KEY (id)')

            streams = tap_postgres.do_discovery(config)
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{root_table}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            prepared = logical_replication.prepare_publication(config, [stream])
            self.assertEqual(str(prepared), publication)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT schemaname, tablename
                      FROM pg_catalog.pg_publication_tables
                     WHERE pubname = %s
                    """,
                    (publication,),
                )
                self.assertEqual(set(cur.fetchall()), {('public', root_table)})

                # A partition leaf has its own source-side identity requirement,
                # even though pgoutput publishes changes as the root relation.
                cur.execute(f'ALTER TABLE public.{leaf_table} REPLICA IDENTITY NOTHING')
                cur.execute(f'DROP PUBLICATION {publication}')
            with self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError,
                    'REPLICA IDENTITY DEFAULT backed by a valid non-deferrable primary key'):
                logical_replication.prepare_publication(config, [stream])
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (publication,),
                )
                self.assertIsNone(cur.fetchone())
                cur.execute(f'ALTER TABLE public.{leaf_table} REPLICA IDENTITY DEFAULT')

            prepared = logical_replication.prepare_publication(config, [stream])
            self.assertEqual(str(prepared), publication)
        finally:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{root_table} CASCADE')
            conn.close()

    def test_partition_root_rejects_live_wal2json_migration_before_slot_creation(self):
        root_table = 'partition_migration_root_test'
        leaf_table = f'{root_table}_leaf'
        config = get_test_connection_config()
        config['tap_id'] = 'partition_migration_test'
        publication = logical_replication.generate_publication_name(config['tap_id'])
        destination = logical_replication.generate_replication_slot_name(config['tap_id'])
        source = logical_replication.legacy_replication_slot_names(
            config['dbname'], config['tap_id'])[1]
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                for slot in (destination, source):
                    cur.execute(
                        'SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = %s',
                        (slot,),
                    )
                    if cur.fetchone() is not None:
                        cur.execute('SELECT pg_catalog.pg_drop_replication_slot(%s)', (slot,))
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{root_table} CASCADE')
                cur.execute(
                    f'CREATE TABLE public.{root_table} (id integer PRIMARY KEY, value text) '
                    'PARTITION BY RANGE (id)')
                cur.execute(
                    f'CREATE TABLE public.{leaf_table} PARTITION OF public.{root_table} '
                    'FOR VALUES FROM (0) TO (100)')
                cur.execute(
                    'SELECT * FROM pg_catalog.pg_create_logical_replication_slot(%s, %s)',
                    (source, 'wal2json'),
                )

            streams = tap_postgres.do_discovery(config)
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{root_table}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            with self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError,
                    'partition root.*full resync'):
                logical_replication.prepare_publication(config, [stream])

            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (publication,),
                )
                self.assertIsNone(cur.fetchone())
                cur.execute(
                    'SELECT slot_name FROM pg_catalog.pg_replication_slots '
                    'WHERE slot_name = ANY(%s)',
                    ([source, destination],),
                )
                self.assertEqual(cur.fetchall(), [(source,)])
        finally:
            with conn.cursor() as cur:
                for slot in (destination, source):
                    cur.execute(
                        'SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = %s',
                        (slot,),
                    )
                    if cur.fetchone() is not None:
                        cur.execute('SELECT pg_catalog.pg_drop_replication_slot(%s)', (slot,))
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{root_table} CASCADE')
            conn.close()

    def test_interrupted_wal2json_bridge_reuses_original_boundary(self):
        table_name = 'wal2json_bridge_retry_test'
        config = get_test_connection_config()
        config['tap_id'] = 'wal2json_bridge_retry'
        config['logical_poll_total_seconds'] = 10
        publication_name = logical_replication.generate_publication_name(config['tap_id'])
        destination = logical_replication.generate_replication_slot_name(config['tap_id'])
        source = logical_replication.legacy_replication_slot_names(
            config['dbname'], config['tap_id'])[1]
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                for slot_name in (destination, source):
                    cur.execute(
                        'SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = %s',
                        (slot_name,),
                    )
                    if cur.fetchone() is not None:
                        cur.execute(
                            'SELECT pg_catalog.pg_drop_replication_slot(%s)',
                            (slot_name,),
                        )
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication_name}')
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name}')
                cur.execute(
                    f'CREATE TABLE public.{table_name} '
                    '(id integer PRIMARY KEY, value text)')
                cur.execute(
                    'SELECT * FROM pg_catalog.pg_create_logical_replication_slot(%s, %s)',
                    (source, 'wal2json'),
                )

            streams = tap_postgres.do_discovery(config)
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{table_name}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            publication = logical_replication.prepare_publication(config, [stream])
            slot = logical_replication.locate_replication_slot(config)
            state = {
                'bookmarks': {
                    stream['tap_stream_id']: {
                        'last_replication_method': 'LOG_BASED',
                        'lsn': slot.source_confirmed_lsn,
                        'version': 1,
                    },
                },
            }
            boundary_positions = []
            emit_wal_progress_message = logical_replication.emit_wal_progress_message

            def record_boundary(connection_config):
                position = emit_wal_progress_message(connection_config)
                boundary_positions.append(position)
                return position

            output = SingerOutput()
            with unittest.mock.patch.object(
                    logical_replication,
                    'emit_wal_progress_message',
                    side_effect=record_boundary), contextlib.redirect_stdout(output):
                interrupted_config = {**config, 'max_run_seconds': 0}
                state = logical_replication._bridge_wal2json_slot(
                    interrupted_config,
                    [stream],
                    state,
                    '/tmp/unused-wal2json-bridge-retry-state.json',
                    publication,
                    source,
                    slot,
                    slot.confirmed_flush_lsn,
                    slot.source_confirmed_lsn,
                    None,
                )
                self.assertEqual(
                    state[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['phase'],
                    'bridge_pending',
                )
                state = logical_replication._bridge_wal2json_slot(
                    config,
                    [stream],
                    state,
                    '/tmp/unused-wal2json-bridge-retry-state.json',
                    publication,
                    source,
                    slot,
                    slot.confirmed_flush_lsn,
                    slot.source_confirmed_lsn,
                    None,
                )

            self.assertEqual(len(boundary_positions), 1)
            self.assertEqual(
                state[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['boundary_lsn'],
                boundary_positions[0],
            )
            self.assertEqual(
                state[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['phase'],
                'bridge',
            )
        finally:
            with conn.cursor() as cur:
                for slot_name in (destination, source):
                    cur.execute(
                        'SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = %s',
                        (slot_name,),
                    )
                    if cur.fetchone() is not None:
                        cur.execute(
                            'SELECT pg_catalog.pg_drop_replication_slot(%s)',
                            (slot_name,),
                        )
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication_name}')
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name}')
            conn.close()

    def test_ordinary_inheritance_parent_is_rejected_before_publication_mutation(self):
        parent_table = 'inheritance_parent_logical_test'
        child_table = f'{parent_table}_child'
        config = get_test_connection_config()
        config['tap_id'] = 'inheritance_parent_test'
        publication = logical_replication.generate_publication_name(config['tap_id'])
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{parent_table} CASCADE')
                cur.execute(
                    f'CREATE TABLE public.{parent_table} '
                    '(id integer PRIMARY KEY, value text)')
                cur.execute(
                    f'CREATE TABLE public.{child_table} (child_value text) '
                    f'INHERITS (public.{parent_table})')

            streams = tap_postgres.do_discovery(config)
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{parent_table}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            with self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError,
                    'ordinary tables with inheritance descendants'):
                logical_replication.prepare_publication(config, [stream])

            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (publication,),
                )
                self.assertIsNone(cur.fetchone())
        finally:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{parent_table} CASCADE')
            conn.close()

    def test_partition_root_rejects_foreign_descendant_before_publication_mutation(self):
        root_table = 'foreign_partition_logical_test'
        leaf_table = f'{root_table}_leaf'
        server = 'pipelinewise_partition_file_server'
        config = get_test_connection_config()
        config['tap_id'] = 'foreign_partition_test'
        publication = logical_replication.generate_publication_name(config['tap_id'])
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{root_table} CASCADE')
                cur.execute(f'DROP SERVER IF EXISTS {server} CASCADE')
                cur.execute('CREATE EXTENSION IF NOT EXISTS file_fdw')
                cur.execute(f'CREATE SERVER {server} FOREIGN DATA WRAPPER file_fdw')
                cur.execute(
                    f'CREATE TABLE public.{root_table} (id integer, value text) '
                    'PARTITION BY RANGE (id)')
                cur.execute(
                    f'CREATE FOREIGN TABLE public.{leaf_table} '
                    f'PARTITION OF public.{root_table} FOR VALUES FROM (0) TO (100) '
                    f'SERVER {server} OPTIONS (filename \'/tmp/pipelinewise-empty.csv\')')

            streams = tap_postgres.do_discovery(config)
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{root_table}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            with self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError,
                    'persistent and partitioned descendants'):
                logical_replication.prepare_publication(config, [stream])

            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (publication,),
                )
                self.assertIsNone(cur.fetchone())
        finally:
            with conn.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{root_table} CASCADE')
                cur.execute(f'DROP SERVER IF EXISTS {server} CASCADE')
            conn.close()

    def test_invalid_canonical_slot_is_rejected_before_publication_mutation(self):
        table_name = 'invalid_slot_preflight_test'
        config = get_test_connection_config()
        config['tap_id'] = 'invalid_slot_preflight'
        publication = logical_replication.generate_publication_name(config['tap_id'])
        slot = logical_replication.generate_replication_slot_name(config['tap_id'])
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = %s',
                    (slot,),
                )
                if cur.fetchone() is not None:
                    cur.execute('SELECT pg_catalog.pg_drop_replication_slot(%s)', (slot,))
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
                cur.execute(
                    f'CREATE TABLE public.{table_name} '
                    '(id integer PRIMARY KEY, value text)')
                cur.execute(
                    'SELECT * FROM pg_catalog.pg_create_logical_replication_slot(%s, %s)',
                    (slot, 'wal2json'),
                )

            streams = tap_postgres.do_discovery(config)
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{table_name}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            with self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError,
                    'uses wal2json, expected pgoutput'):
                logical_replication.prepare_publication(config, [stream])

            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (publication,),
                )
                self.assertIsNone(cur.fetchone())
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = %s',
                    (slot,),
                )
                if cur.fetchone() is not None:
                    cur.execute('SELECT pg_catalog.pg_drop_replication_slot(%s)', (slot,))
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
            conn.close()

    def test_sync_rejects_bookmark_behind_canonical_confirmed_lsn(self):
        """A restored bookmark cannot silently start at retained slot WAL."""
        table_name = 'stale_canonical_bookmark_test'
        config = get_test_connection_config()
        config['tap_id'] = 'stale_canonical_bookmark'
        publication = logical_replication.generate_publication_name(config['tap_id'])
        slot = logical_replication.generate_replication_slot_name(config['tap_id'])
        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = %s',
                    (slot,),
                )
                if cur.fetchone() is not None:
                    cur.execute(
                        'SELECT pg_catalog.pg_drop_replication_slot(%s)',
                        (slot,),
                    )
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
                cur.execute(
                    f'CREATE TABLE public.{table_name} '
                    '(id integer PRIMARY KEY, value text)')
                cur.execute(
                    'SELECT * FROM pg_catalog.pg_create_logical_replication_slot(%s, %s)',
                    (slot, 'pgoutput'),
                )
                cur.execute(
                    'SELECT confirmed_flush_lsn::text '
                    'FROM pg_catalog.pg_replication_slots WHERE slot_name = %s',
                    (slot,),
                )
                confirmed_lsn = logical_replication.lsn_to_int(cur.fetchone()[0])
                self.assertGreater(confirmed_lsn, 1)

            streams = tap_postgres.do_discovery(config)
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{table_name}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            logical_replication.prepare_publication(config, [stream], fresh_start=True)
            state = {
                'bookmarks': {
                    stream['tap_stream_id']: {'lsn': 1, 'version': 1},
                },
            }
            with self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError,
                    'predates canonical pgoutput slot.*unfiltered whole-tap FastSync'):
                logical_replication.sync_tables(
                    config,
                    [stream],
                    state,
                    logical_replication.fetch_current_lsn(config),
                    '/tmp/unused-stale-canonical-state.json',
                )
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = %s',
                    (slot,),
                )
                if cur.fetchone() is not None:
                    cur.execute(
                        'SELECT pg_catalog.pg_drop_replication_slot(%s)',
                        (slot,),
                    )
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
            conn.close()

    def test_exact_precreated_publication_is_fenced_on_first_use(self):
        table_name = 'precreated_publication_test'
        config = get_test_connection_config()
        config['tap_id'] = 'precreated_fence_test'
        config['publication_fence_timeout_seconds'] = 5
        publication = logical_replication.generate_publication_name(config['tap_id'])
        transaction = get_test_connection()
        control = get_test_connection()
        errors = []
        try:
            with control.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
                cur.execute(f'CREATE TABLE public.{table_name} (id integer PRIMARY KEY)')

            transaction.autocommit = False
            with transaction.cursor() as cur:
                cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                cur.execute('SELECT txid_current()')

            with control.cursor() as cur:
                cur.execute(
                    f"CREATE PUBLICATION {publication} FOR TABLE public.{table_name} "
                    "WITH (publish = 'insert, update, delete', publish_via_partition_root = true)"
                )
                cur.execute(f"COMMENT ON PUBLICATION {publication} IS 'DBA comment'")

            streams = tap_postgres.do_discovery(config)
            stream = next(
                stream for stream in streams
                if stream['tap_stream_id'] == f'public-{table_name}'
            )
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            waiter = threading.Thread(
                target=lambda: self._prepare_publication(config, stream, errors),
                daemon=True,
            )
            waiter.start()

            deadline = time.monotonic() + 3
            pending_comment = None
            while time.monotonic() < deadline:
                with control.cursor() as cur:
                    cur.execute(
                        "SELECT pg_catalog.obj_description(oid, 'pg_publication') "
                        'FROM pg_catalog.pg_publication WHERE pubname = %s',
                        (publication,),
                    )
                    pending_comment = cur.fetchone()[0]
                if (isinstance(pending_comment, str)
                        and pending_comment.startswith(
                            logical_replication.PUBLICATION_FENCE_COMMENT_PREFIX)):
                    break
                time.sleep(0.05)

            self.assertEqual(
                logical_replication._decode_publication_fence_comment(pending_comment),
                ('pending', 'DBA comment', {('public', table_name)}),
            )
            self.assertTrue(waiter.is_alive())
            transaction.commit()
            waiter.join(timeout=5)
            self.assertFalse(waiter.is_alive())
            self.assertEqual(errors, [])

            with control.cursor() as cur:
                cur.execute(
                    "SELECT pg_catalog.obj_description(oid, 'pg_publication') "
                    'FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (publication,),
                )
                ready_comment = cur.fetchone()[0]
            self.assertEqual(
                logical_replication._decode_publication_fence_comment(ready_comment),
                ('ready', 'DBA comment', {('public', table_name)}),
            )
        finally:
            transaction.close()
            with control.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
            control.close()

    @staticmethod
    def _prepare_publication(config, stream, errors):
        try:
            logical_replication.prepare_publication(config, [stream])
        except Exception as ex:  # pragma: no cover - reported by the calling test
            errors.append(ex)


class TestLogicalReplicaSnapshots(unittest.TestCase):
    def test_secondary_snapshot_waits_for_primary_bookmark(self):
        table_name = 'secondary_snapshot_boundary_test'
        config = get_test_connection_config(use_secondary=True)
        config.update({'tap_id': 'secondary_snapshot_test', 'publication_fence_timeout_seconds': 5})
        publication = logical_replication.generate_publication_name(config['tap_id'])
        primary = get_test_connection()
        secondary = tap_postgres.post_db.open_connection(config)
        secondary.autocommit = True
        try:
            with primary.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
                cur.execute(f'CREATE TABLE public.{table_name} (id integer PRIMARY KEY, value text)')
            with contextlib.redirect_stdout(SingerOutput()):
                streams = tap_postgres.do_discovery({**config, 'use_secondary': False})
            stream = next(item for item in streams if item['tap_stream_id'] == f'public-{table_name}')
            stream = set_replication_method_for_stream(stream, 'LOG_BASED')
            logical_replication.prepare_publication(config, [stream])
            logical_replication.wait_for_replica_replay(config, logical_replication.fetch_current_lsn(config))

            with secondary.cursor() as cur:
                cur.execute('SELECT pg_is_in_recovery()')
                self.assertTrue(cur.fetchone()[0], 'The secondary test endpoint must be a real standby')
                cur.execute('SELECT pg_wal_replay_pause()')
                deadline = time.monotonic() + 5
                while True:
                    cur.execute('SELECT pg_get_wal_replay_pause_state()')
                    if cur.fetchone()[0] == 'paused':
                        break
                    self.assertLess(time.monotonic(), deadline, 'Standby replay did not pause')
                    time.sleep(0.01)
            with primary.cursor() as cur:
                cur.execute(f"INSERT INTO public.{table_name} VALUES (1, 'before the primary bookmark')")
            boundary_lsn = logical_replication.fetch_current_lsn(config)
            with secondary.cursor() as cur:
                cur.execute(f'SELECT count(*) FROM public.{table_name}')
                self.assertEqual(cur.fetchone()[0], 0)

            output = SingerOutput()
            state = {'bookmarks': {}}
            config['publication_fence_timeout_seconds'] = 0.1
            with contextlib.redirect_stdout(output), self.assertRaisesRegex(
                    logical_replication.ReplicationSlotMigrationError, 'Timed out waiting for the secondary'):
                tap_postgres.sync_traditional_stream(config, stream, state, 'logical_initial', boundary_lsn)
            self.assertFalse(any(
                json.loads(line)['type'] in {'RECORD', 'STATE'} for line in output.getvalue().splitlines()))
            self.assertFalse(state.get('bookmarks', {}).get(stream['tap_stream_id'], {}).get('lsn'))

            with secondary.cursor() as cur:
                cur.execute('SELECT pg_wal_replay_resume()')
            config['publication_fence_timeout_seconds'] = 5
            with contextlib.redirect_stdout(output):
                tap_postgres.sync_traditional_stream(config, stream, state, 'logical_initial', boundary_lsn)
            records = [json.loads(line) for line in output.getvalue().splitlines()]
            self.assertEqual(
                [message['record'] for message in records if message['type'] == 'RECORD'],
                [{'id': 1, 'value': 'before the primary bookmark'}],
            )
            self.assertEqual(state['bookmarks'][stream['tap_stream_id']]['lsn'], boundary_lsn)
        finally:
            with secondary.cursor() as cur:
                cur.execute('SELECT pg_wal_replay_resume()')
            secondary.close()
            with primary.cursor() as cur:
                cur.execute(f'DROP PUBLICATION IF EXISTS {publication}')
                cur.execute(f'DROP TABLE IF EXISTS public.{table_name} CASCADE')
            primary.close()
