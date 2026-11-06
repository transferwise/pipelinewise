"""Exact YSQL numeric extraction through discovery, SELECT, keyset resume and wal2json."""

import json
import math
import time
import unittest
import uuid
from decimal import Decimal
from unittest.mock import patch

import singer
from jsonschema import Draft7Validator
from singer import metadata

import tap_yugabyte
from tap_yugabyte import keyset
from tap_yugabyte.sync_strategies import full_table, incremental

from tests.integration.test_full_table import _capture_singer_messages, _discover_stream
from tests.utils import (
    drop_replication_slot,
    create_replication_slot,
    execute_retrying_serialization_failures,
    get_test_connection,
    get_test_connection_config,
    set_replication_method_for_stream,
)

EXACT = '12345678901234567890.123456789012345678'
CHANGED = '12345678901234567890.123456789012345679'
WIDE_KEYS = ['12345678901234567891', '12345678901234567892', '12345678901234567893']
FLOAT_ROWS = [
    ('NaN', 'NaN', 'NaN', '{NaN,1.5}'),
    ('Infinity', 'Infinity', 'Infinity', '{{Infinity,-Infinity},{NaN,2.5}}'),
    ('-Infinity', '-Infinity', '-Infinity', None),
    ('1.5', '-0', '1.25', '{-0,2.5}'),
]
FLOAT_RECORDS = [
    {'id': 'NaN', 'single': 'NaN', 'double': 'NaN', 'doubles': ['NaN', 1.5]},
    {'id': 'Infinity', 'single': 'Infinity', 'double': 'Infinity',
     'doubles': [['Infinity', '-Infinity'], ['NaN', 2.5]]},
    {'id': '-Infinity', 'single': '-Infinity', 'double': '-Infinity', 'doubles': None},
    {'id': 1.5, 'single': -0.0, 'double': 1.25, 'doubles': [-0.0, 2.5]},
]
ARRAY_DDL = 'id INT PRIMARY KEY, amounts NUMERIC[], scaled NUMERIC(10,2)[], ints INT[], doubles FLOAT8[]'
ARRAY_COLUMNS = ['amounts', 'doubles', 'id', 'ints', 'scaled']
ARRAY_ROWS = [
    (1, '{1,1.10,12345678901234567890.123,0.00000012,NaN,Infinity,-Infinity,NULL}', '{1.1,2,NaN,NULL}', None, None),
    (2, '{{1,2},{3,4}}', '{{1.5,2},{3,4}}', '{}', '{}'),
    (3, '[0:1]={5,6}', '[0:1]={5,6}', '{7,NULL}', '{NaN,-Infinity,2.5}'),
    (4, '{}', '{}', None, '{}'),
    (5, None, None, '{}', None),
]
# The server's own array_to_json(col)::text, which FastSync exports through COPY.
ARRAY_TEXT = {
    1: ('[1,1.10,12345678901234567890.123,0.00000012,"NaN","Infinity","-Infinity",null]', '[1.10,2.00,"NaN",null]'),
    2: ('[[1,2],[3,4]]', '[[1.50,2.00],[3.00,4.00]]'),
    3: ('[5,6]', '[5.00,6.00]'),
    4: ('[]', '[]'),
    5: (None, None),
}
ARRAY_LISTS = {1: (None, None), 2: ([], []), 3: ([7, None], ['NaN', '-Infinity', 2.5]), 4: (None, []), 5: ([], None)}
TARGETS = ('snowflake', 'postgres')


def _config():
    config = get_test_connection_config()
    config['decimal_target'] = 'snowflake'
    return config


def _create(table, ddl):
    with get_test_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public.{table}')
            cur.execute(f'CREATE TABLE public.{table} ({ddl})')


def _drop(table):
    with get_test_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public.{table}')


def _records(messages):
    return [message['record'] for message in messages if message['type'] == 'RECORD']


class TestDecimalDiscoveryAndSelect(unittest.TestCase):

    def setUp(self):
        self.table = 'decimal_select_' + uuid.uuid4().hex[:12]
        _create(self.table, 'id NUMERIC(20,0) PRIMARY KEY, amount NUMERIC(38,18), negative NUMERIC(10,-2), '
                            'tiny NUMERIC(3,5), unbounded NUMERIC, approximate FLOAT8')

    def tearDown(self):
        _drop(self.table)

    def test_full_table_keeps_source_digits_and_declared_dimensions(self):
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO public.{self.table} VALUES (%s, %s, 12300, 0.00123, %s, 1.25)',
                            (WIDE_KEYS[0], EXACT, '1' * 60 + '.' + '2' * 40))
        stream = _discover_stream(_config(), self.table)
        properties = stream['schema']['properties']
        self.assertEqual({'precision': 38, 'scale': 18}, properties['amount']['decimal'])
        self.assertEqual({'precision': 10, 'scale': -2}, properties['negative']['decimal'])
        self.assertEqual({'precision': 3, 'scale': 5}, properties['tiny']['decimal'])
        self.assertEqual({'precision': None, 'scale': None}, properties['unbounded']['decimal'])
        self.assertEqual('snowflake', metadata.to_map(stream['metadata'])[()]['decimal-target'])

        set_replication_method_for_stream(stream, 'FULL_TABLE')
        _, messages = _capture_singer_messages(tap_yugabyte.do_sync, _config(), {'streams': [stream]},
                                               'FULL_TABLE', {})

        [record] = _records(messages)
        self.assertEqual(WIDE_KEYS[0], record['id'])
        self.assertEqual(EXACT, record['amount'])
        self.assertEqual(Decimal('12300'), Decimal(record['negative']))
        self.assertEqual(Decimal('0.00123'), Decimal(record['tiny']))
        self.assertEqual('1' * 60 + '.' + '2' * 40, record['unbounded'])
        self.assertEqual(1.25, record['approximate'])

    def test_keyset_resume_from_exact_numeric_key_bookmark_skips_and_replays_nothing(self):
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(keyset.index_ddl(f'"public"."{self.table}"', self.table, ['id'], 2))
                for key in WIDE_KEYS:
                    cur.execute(f'INSERT INTO public.{self.table} (id, amount) VALUES (%s, 1)', (key,))
        stream = _discover_stream(_config(), self.table)
        md_map = metadata.to_map(stream['metadata'])
        tap_stream_id = stream['tap_stream_id']
        # These keys differ only past float precision; a rounded bound would replay or skip them.
        conn_config = dict(_config(), keyset_buckets=2)
        state = {'bookmarks': {tap_stream_id: {
            'version': 1, 'keyset_buckets': 2,
            'max_pk_values': [WIDE_KEYS[-1]], 'last_pk_fetched': {'0': [WIDE_KEYS[0]], '1': [WIDE_KEYS[0]]},
        }}}

        _, messages = _capture_singer_messages(full_table.sync_table, conn_config, stream, state,
                                               ['id', 'amount'], md_map)

        self.assertEqual(WIDE_KEYS[1:], sorted(record['id'] for record in _records(messages)))


class TestDecimalIncremental(unittest.TestCase):

    def setUp(self):
        self.table = 'decimal_resume_' + uuid.uuid4().hex[:12]
        self.rows = [(1, '9999999999999999.98'), (2, '9999999999999999.99'), (3, '10000000000000000.00')]
        _create(self.table, 'id INT PRIMARY KEY, amount NUMERIC(20,2)')
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(f'INSERT INTO public.{self.table} VALUES (%s, %s)', self.rows)

    def tearDown(self):
        _drop(self.table)

    def _sync(self, bookmark):
        stream = _discover_stream(_config(), self.table)
        md_map = metadata.to_map(stream['metadata'])
        md_map[()]['replication-key'] = 'amount'
        state = {'bookmarks': {stream['tap_stream_id']: {'replication_key_value': bookmark}}}
        result, messages = _capture_singer_messages(incremental.sync_table, _config(), stream, state,
                                                    ['id', 'amount'], md_map)
        return result['bookmarks'][stream['tap_stream_id']]['replication_key_value'], _records(messages)

    def test_exact_bookmark_resumes_at_the_bookmarked_value(self):
        bookmark, records = self._sync(self.rows[1][1])
        self.assertEqual(self.rows[1:], [(record['id'], record['amount']) for record in records])
        self.assertEqual(self.rows[-1][1], bookmark)


class TestDecimalLogicalReplication(unittest.TestCase):
    tap_id = 'tap_decimal_logical_replication_test'

    def setUp(self):
        self.table = 'decimal_wal_' + uuid.uuid4().hex[:12]
        _create(self.table, 'id INT PRIMARY KEY, amount NUMERIC(38,18), unbounded NUMERIC, approximate FLOAT8')
        create_replication_slot(tap_id=self.tap_id)

    def tearDown(self):
        drop_replication_slot(tap_id=self.tap_id)
        _drop(self.table)

    def test_wal_keeps_source_digits_for_every_action(self):
        conn_config = dict(_config(), tap_id=self.tap_id)
        stream = _discover_stream(conn_config, self.table)
        set_replication_method_for_stream(stream, 'LOG_BASED')
        catalog = {'streams': [stream]}
        state, _ = _capture_singer_messages(tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', {})

        wide = '9' * 70 + '.' + '1' * 30
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO public.{self.table} VALUES (1, %s, %s, 1.25)', (EXACT, wide))
                cur.execute(f'UPDATE public.{self.table} SET amount = %s WHERE id = 1', (CHANGED,))
                execute_retrying_serialization_failures(cur, f'DELETE FROM public.{self.table} WHERE id = 1')
        # Let the CDC stream observe the committed changes before polling.
        time.sleep(2)

        _, messages = _capture_singer_messages(tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', state)

        # YugabyteDB's default CHANGE identity sends only modified columns on UPDATE.
        inserted, updated, deleted = _records(messages)
        self.assertEqual((EXACT, wide, 1.25), (inserted['amount'], inserted['unbounded'], inserted['approximate']))
        self.assertEqual(CHANGED, updated['amount'])
        self.assertEqual(1, deleted['id'])
        self.assertIsNotNone(deleted['_sdc_deleted_at'])
        self.assertTrue(all(
            message['schema']['properties']['amount'].get('format') == 'singer.decimal'
            for message in messages if message['type'] == 'SCHEMA'
        ))
        self.assertIsInstance(singer.get_bookmark(state, stream['tap_stream_id'], 'lsn'), int)

    def test_wal_typmod_change_publishes_new_decimal_dimensions(self):
        conn_config = dict(_config(), tap_id=self.tap_id)
        stream = _discover_stream(conn_config, self.table)
        set_replication_method_for_stream(stream, 'LOG_BASED')
        catalog = {'streams': [stream]}
        state, _ = _capture_singer_messages(tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', {})

        widened = '1234567890123456789012.12345678901234567890'
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'ALTER TABLE public.{self.table} ALTER COLUMN amount TYPE NUMERIC(42,20)')
        # A fresh session avoids the transient catalog-version conflict after YSQL DDL.
        time.sleep(2)
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                execute_retrying_serialization_failures(
                    cur, f'INSERT INTO public.{self.table} (id, amount) VALUES (2, %s)', (widened,))
        time.sleep(2)

        _, messages = _capture_singer_messages(tap_yugabyte.do_sync, conn_config, catalog, 'LOG_BASED', state)

        [record] = _records(messages)
        # YugabyteDB's wal2json can drop typmod trailing zeros after ALTER TYPE; the value stays exact.
        self.assertEqual(Decimal(widened), Decimal(record['amount']))
        schemas = [message['schema']['properties']['amount'] for message in messages if message['type'] == 'SCHEMA']
        self.assertEqual({'precision': 42, 'scale': 20}, schemas[-1]['decimal'])


def _validated(records, stream):
    """Validate serialized records against the discovered schema, as the targets do."""
    validator = Draft7Validator(stream['schema'])
    for record in records:
        validator.validate(record)
    return records


def _insert(table, rows):
    with get_test_connection() as conn:
        with conn.cursor() as cur:
            placeholders = ', '.join(['%s'] * len(rows[0]))
            for row in rows:
                execute_retrying_serialization_failures(
                    cur, f'INSERT INTO public.{table} VALUES ({placeholders})', row)


class TestNonfiniteFloatSelect(unittest.TestCase):

    def setUp(self):
        self.table = 'float_nonfinite_' + uuid.uuid4().hex[:12]
        _create(self.table, 'id FLOAT8 PRIMARY KEY, single FLOAT4, double FLOAT8, doubles FLOAT8[]')
        _insert(self.table, FLOAT_ROWS)

    def tearDown(self):
        _drop(self.table)

    def test_full_table_carries_nonfinite_keys_scalars_and_array_elements(self):
        stream = _discover_stream(_config(), self.table)
        properties = stream['schema']['properties']
        self.assertEqual(['number', 'string'], properties['id']['type'])
        self.assertEqual(['null', 'number', 'string'], properties['single']['type'])
        self.assertEqual(['null', 'number', 'string'], properties['double']['type'])
        self.assertEqual(['null', 'number', 'string', 'array'],
                         stream['schema']['definitions']['sdc_recursive_number_array']['type'])

        set_replication_method_for_stream(stream, 'FULL_TABLE')
        _, messages = _capture_singer_messages(tap_yugabyte.do_sync, _config(), {'streams': [stream]},
                                               'FULL_TABLE', {})

        records = _validated(_records(messages), stream)
        self.assertEqual(sorted(FLOAT_RECORDS, key=lambda record: str(record['id'])),
                         sorted(records, key=lambda record: str(record['id'])))
        finite = next(record for record in records if record['id'] == 1.5)
        self.assertEqual(-1.0, math.copysign(1.0, finite['single']))
        self.assertEqual(-1.0, math.copysign(1.0, finite['doubles'][0]))


class TestNonfiniteFloatIncremental(unittest.TestCase):

    def setUp(self):
        self.table = 'float_resume_' + uuid.uuid4().hex[:12]
        _create(self.table, 'id INT PRIMARY KEY, key FLOAT8')
        _insert(self.table, [(1, '-Infinity'), (2, '1.0'), (3, 'Infinity'), (4, 'NaN'), (5, None)])

    def tearDown(self):
        _drop(self.table)

    def _sync(self, stream, md_map, bookmark):
        state = {'bookmarks': {stream['tap_stream_id']: bookmark}}
        result, messages = _capture_singer_messages(incremental.sync_table, _config(), stream, state,
                                                    ['id', 'key'], md_map)
        rows = [(record['id'], record['key']) for record in _validated(_records(messages), stream)]
        # The next run reads state back from JSON, as the tap does after a target acknowledgement.
        state = json.loads(singer.format_message(singer.StateMessage(value=result)))['value']
        return state['bookmarks'][stream['tap_stream_id']], rows

    def test_float_key_keeps_last_finite_bookmark_and_rereads_nonfinite_rows(self):
        stream = _discover_stream(_config(), self.table)
        md_map = metadata.to_map(stream['metadata'])
        md_map[()]['replication-key'] = 'key'

        bookmark, rows = self._sync(stream, md_map, {})
        self.assertEqual([(1, '-Infinity'), (2, 1.0), (3, 'Infinity'), (4, 'NaN'), (5, None)], rows)
        self.assertEqual(1.0, bookmark['replication_key_value'])

        bookmark, rows = self._sync(stream, md_map, bookmark)
        self.assertEqual([(2, 1.0), (3, 'Infinity'), (4, 'NaN')], rows)
        self.assertEqual(1.0, bookmark['replication_key_value'])

        _insert(self.table, [(6, '2.5')])
        bookmark, rows = self._sync(stream, md_map, bookmark)
        self.assertEqual([(2, 1.0), (6, 2.5), (3, 'Infinity'), (4, 'NaN')], rows)
        self.assertEqual(2.5, bookmark['replication_key_value'])


def _execute(sql, params=None):
    with get_test_connection() as conn:
        with conn.cursor() as cur:
            execute_retrying_serialization_failures(cur, sql, params)


def _discover_streams(config, *tables):
    """Discover once and pick several streams; every discovery walks the whole database."""
    with patch('tap_yugabyte.dump_catalog'):
        streams = {stream['table_name']: stream for stream in tap_yugabyte.do_discovery(config)}
    return [streams[table] for table in tables]


def _server_array_text(table):
    with get_test_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, array_to_json(amounts)::text, array_to_json(scaled)::text '
                        f'FROM public.{table} ORDER BY id')
            return {row[0]: row[1:] for row in cur.fetchall()}


def _array_record(key, texts, lists):
    return {'id': key, 'amounts': texts[0], 'scaled': texts[1], 'ints': lists[0], 'doubles': lists[1]}


def _without_deleted_at(records):
    return [{name: value for name, value in record.items() if name != '_sdc_deleted_at'} for record in records]


class TestArrayParitySelect(unittest.TestCase):

    def setUp(self):
        self.table = 'array_parity_' + uuid.uuid4().hex[:12]
        self.view = self.table + '_view'
        _create(self.table, ARRAY_DDL)
        self.addCleanup(_drop, self.table)
        _insert(self.table, ARRAY_ROWS)
        _execute(f'CREATE VIEW public.{self.view} AS SELECT * FROM public.{self.table}')
        self.addCleanup(_execute, f'DROP VIEW IF EXISTS public.{self.view}')

    def _sync(self, sync, config, stream, md_map):
        state, messages = _capture_singer_messages(sync, config, stream, {}, ARRAY_COLUMNS, md_map)
        return state['bookmarks'][stream['tap_stream_id']], sorted(
            _validated(_records(messages), stream), key=lambda record: record['id'])

    def test_select_paths_carry_server_json_text_and_null_arrays(self):
        self.assertEqual(ARRAY_TEXT, _server_array_text(self.table))
        expected = [_array_record(key, ARRAY_TEXT[key], ARRAY_LISTS[key]) for key in sorted(ARRAY_TEXT)]
        routes = {target: (dict(_config(), decimal_target=target),
                           *_discover_streams(dict(_config(), decimal_target=target), self.table, self.view))
                  for target in TARGETS}
        for target, (config, stream, view_stream) in routes.items():
            with self.subTest(target=target):
                for discovered in (stream, view_stream):
                    for name in ('amounts', 'scaled'):
                        self.assertEqual(['null', 'string', 'array'], discovered['schema']['properties'][name]['type'])
                        self.assertEqual('singer.json', discovered['schema']['properties'][name]['format'])
                md_map = metadata.to_map(stream['metadata'])
                bookmark, records = self._sync(full_table.sync_table, config, stream, md_map)
                self.assertEqual(expected, records)
                # Without the keyset index, FULL_TABLE falls back to the plain scan and keeps no bucket bookmarks.
                self.assertNotIn('keyset_buckets', bookmark)
                _, records = self._sync(full_table.sync_view, config, view_stream,
                                        metadata.to_map(view_stream['metadata']))
                self.assertEqual(expected, records)
                _, records = self._sync(incremental.sync_table, config, stream,
                                        {**md_map, (): {**md_map[()], 'replication-key': 'id'}})
                self.assertEqual(expected, records)

        _execute(keyset.index_ddl(f'"public"."{self.table}"', self.table, ['id'], keyset.BUCKETS_DEFAULT))
        for target, (config, stream, _) in routes.items():
            with self.subTest(target=target, keyset_index=True):
                bookmark, records = self._sync(full_table.sync_table, config, stream,
                                               metadata.to_map(stream['metadata']))
                self.assertEqual(expected, records)
                self.assertIn('keyset_buckets', bookmark)


class _TargetSlots:
    """One table and slot per target: Snowflake keeps YugabyteDB's CHANGE identity, PostgreSQL needs DEFAULT."""

    ddl = None
    prefix = None

    def setUp(self):
        base = f'{self.prefix}_{uuid.uuid4().hex[:12]}'
        self.tables = {target: f'{base}_{target}' for target in TARGETS}
        self.tap_ids = {target: f'tap_{self.prefix}_{target}' for target in TARGETS}
        for table in self.tables.values():
            _create(table, self.ddl)
            self.addCleanup(_drop, table)
        # tap-yugabyte rejects CHANGE for PostgreSQL targets, and a slot keeps the identity it was created with.
        _execute(f'ALTER TABLE public.{self.tables["postgres"]} REPLICA IDENTITY DEFAULT')
        for tap_id in self.tap_ids.values():
            create_replication_slot(tap_id=tap_id)
            self.addCleanup(drop_replication_slot, tap_id=tap_id)

    def _bootstrap(self):
        """Start every target before any change, so both CDC runs read the same events."""
        runs = {}
        for target in TARGETS:
            config = dict(_config(), decimal_target=target, tap_id=self.tap_ids[target])
            stream = set_replication_method_for_stream(_discover_stream(config, self.tables[target]), 'LOG_BASED')
            catalog = {'streams': [stream]}
            state, _ = _capture_singer_messages(tap_yugabyte.do_sync, config, catalog, 'LOG_BASED', {})
            runs[target] = (config, catalog, state)
        return runs

    def _replicate(self, runs):
        # Let the CDC stream observe the committed changes before polling.
        time.sleep(2)
        replicated = {}
        for target, (config, catalog, state) in runs.items():
            _, messages = _capture_singer_messages(tap_yugabyte.do_sync, config, catalog, 'LOG_BASED', state)
            replicated[target] = _validated(_records(messages), catalog['streams'][0])
        return replicated


class TestArrayParityLogicalReplication(_TargetSlots, unittest.TestCase):
    ddl = ARRAY_DDL
    prefix = 'array_wal'

    def test_cdc_carries_server_json_text_and_null_arrays_for_every_action(self):
        runs = self._bootstrap()
        for table in self.tables.values():
            _insert(table, ARRAY_ROWS)
            _execute(f"UPDATE public.{table} SET amounts = '{{7.70,NaN}}', ints = NULL WHERE id = 2")
            _execute(f'DELETE FROM public.{table} WHERE id = 3')
            self.assertEqual(('[7.70,"NaN"]', ARRAY_TEXT[2][1]), _server_array_text(table)[2])
        replicated = self._replicate(runs)

        inserted = [_array_record(key, ARRAY_TEXT[key], ARRAY_LISTS[key]) for key in sorted(ARRAY_TEXT)]
        # CHANGE sends only the key and the changed columns of an update; DEFAULT sends the whole new row.
        updated = {'snowflake': {'id': 2, 'amounts': '[7.70,"NaN"]', 'ints': None},
                   'postgres': _array_record(2, ('[7.70,"NaN"]', ARRAY_TEXT[2][1]), (None, []))}
        for target, records in replicated.items():
            with self.subTest(target=target):
                self.assertEqual([*inserted, updated[target], {'id': 3}], _without_deleted_at(records))
                self.assertEqual([False] * 6 + [True], [record['_sdc_deleted_at'] is not None for record in records])


class TestNonfiniteFloatLogicalReplication(_TargetSlots, unittest.TestCase):
    ddl = 'id FLOAT8 PRIMARY KEY, single FLOAT4, double FLOAT8, doubles FLOAT8[]'
    prefix = 'float_wal'

    def test_cdc_carries_nonfinite_keys_scalars_and_array_elements_for_every_action(self):
        runs = self._bootstrap()
        for table in self.tables.values():
            _insert(table, FLOAT_ROWS)
            _execute(f"UPDATE public.{table} SET single = 'NaN', double = '-Infinity' WHERE id = 'Infinity'")
            _execute(f"DELETE FROM public.{table} WHERE id = 'NaN'")
        replicated = self._replicate(runs)

        updated = {'snowflake': {'id': 'Infinity', 'single': 'NaN', 'double': '-Infinity'},
                   'postgres': {**FLOAT_RECORDS[1], 'single': 'NaN', 'double': '-Infinity'}}
        for target, records in replicated.items():
            with self.subTest(target=target):
                self.assertEqual([*FLOAT_RECORDS, updated[target], {'id': 'NaN'}], _without_deleted_at(records))
                self.assertEqual([False] * 5 + [True], [record['_sdc_deleted_at'] is not None for record in records])
                self.assertEqual(-1.0, math.copysign(1.0, records[3]['single']))
                self.assertEqual(-1.0, math.copysign(1.0, records[3]['doubles'][0]))
