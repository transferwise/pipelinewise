import contextlib
import io
import json
import unittest
import unittest.mock

import tap_postgres

from ..utils import get_test_connection_config, ensure_test_table, create_replication_slot, drop_replication_slot, \
    set_replication_method_for_stream, get_test_connection, insert_record, drop_table


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

        my_stdout = io.StringIO()

        # Would use full initial sync
        with contextlib.redirect_stdout(my_stdout):
            state = tap_postgres.do_sync(self.config, {'streams': [awesome_stream]}, 'LOG_BASED', state, None)

        print('stdout from full initial sync: ', my_stdout.getvalue())
        messages = [json.loads(msg) for msg in my_stdout.getvalue().splitlines()]
        messages = list(filter(lambda msg: msg['type'] != 'ACTIVATE_VERSION', messages))

        self.assertEqual(messages[0]['type'], 'SCHEMA')
        self.assertEqual(messages[0]['stream'], f'public-{self.table_name}')

        self.assertEqual(messages[1]['type'], 'STATE')
        self.assertEqual(messages[0]['stream'], f'public-{self.table_name}')

        self.assertDictEqual(messages[2], {
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
        self.assertDictEqual(messages[3], {
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
        self.assertDictEqual(messages[4], {
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

        self.assertDictEqual(state, messages[5]['value'])
        self.assertIsNotNone(state['bookmarks']['public-awesome_table']['lsn'])

        conn = get_test_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"update {self.table_name} set colour='purple' where name='betty';")
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

        # clear the io
        my_stdout.seek(0)
        my_stdout.truncate()

        # Would use logical replication
        with contextlib.redirect_stdout(my_stdout):
            state = tap_postgres.do_sync(self.config, {'streams': [awesome_stream]}, 'LOG_BASED', state, None)

        messages = [json.loads(msg) for msg in my_stdout.getvalue().splitlines()]
        messages = list(filter(lambda msg: msg['type'] != 'ACTIVATE_VERSION', messages))

        self.assertEqual(messages[0]['type'], 'SCHEMA')
        self.assertDictEqual(messages[1], {
            'type': 'RECORD',
            'stream': f'public-{self.table_name}',
            'record': {
                '_sdc_deleted_at': None,
                'colour': 'purple',
                'id': 1,
                'name': 'betty',
                'timestamp_ntz': '2020-09-01T10:40:59+00:00',
                'timestamp_tz': '2020-08-31T22:50:59+00:00',
            },
            'time_extracted': unittest.mock.ANY,
            'version': unittest.mock.ANY,
        })
        self.assertDictEqual(messages[2], {
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
        self.assertDictEqual(messages[3], {
            'type': 'RECORD',
            'stream': f'public-{self.table_name}',
            'record': {
                '_sdc_deleted_at': unittest.mock.ANY,
                'id': 3,
            },
            'time_extracted': unittest.mock.ANY,
            'version': unittest.mock.ANY,
        })
        self.assertEqual(messages[4]['type'], 'STATE')
        self.assertDictEqual(state, messages[4]['value'])


class TestUnselectedTableSlotAdvancement(unittest.TestCase):
    """Test that WAL slot LSN advances even when only non-selected tables have activity.

    This verifies the include-transaction: true fix — B/C markers from transactions
    on unselected tables cause the slot to advance, preventing unbounded slot growth.
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

    def test_slot_advances_with_only_unselected_table_activity(self):
        """Slot LSN must advance when only the unselected table receives writes."""

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
        my_stdout = io.StringIO()
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

        # Run sync again — no rows from selected table, but B/C markers should advance the slot
        my_stdout.seek(0)
        my_stdout.truncate()
        with contextlib.redirect_stdout(my_stdout):
            state = tap_postgres.do_sync(self.config, {'streams': [selected_stream]}, 'LOG_BASED', state, None)

        # Assert that the LSN bookmark has advanced past the unselected-table activity
        new_lsn = state['bookmarks'][f'public-{self.selected_table}']['lsn']
        self.assertGreater(new_lsn, initial_lsn,
                           "LSN bookmark should advance when unselected tables have WAL activity "
                           "(B/C markers from include-transaction: true)")

        # Verify no RECORD messages were emitted (only the unselected table had activity)
        messages = [json.loads(msg) for msg in my_stdout.getvalue().splitlines()]
        record_messages = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len(record_messages), 0,
                         "No RECORD messages should be emitted for unselected table activity")
