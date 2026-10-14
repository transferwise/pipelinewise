"""Exact YSQL numeric extraction through discovery, SELECT, keyset resume and wal2json."""

import time
import unittest
import uuid
from decimal import Decimal

import singer
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
