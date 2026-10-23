import argparse
import unittest
from unittest.mock import MagicMock, Mock, call, patch

import psycopg2

import tap_yugabyte


def _fake_args(config):
    return argparse.Namespace(
        config=config,
        discover=True,
        properties=None,
        catalog=None,
        state={},
        state_file=None,
    )


class TestMainImplConnConfig(unittest.TestCase):
    """main_impl must forward optional connection keys from args.config into conn_config"""

    def test_load_balance_and_topology_keys_are_forwarded(self):
        config = {
            'host': 'my_host',
            'user': 'my_user',
            'password': 'my_password',
            'port': 5433,
            'dbname': 'my_db',
            'load_balance': 'any',
            'topology_keys': 'cloud1.region1.zone1,cloud1.region1.zone2',
        }

        with patch.object(tap_yugabyte, 'parse_args', return_value=_fake_args(config)), \
                patch.object(tap_yugabyte, 'do_discovery') as do_discovery_mock:
            tap_yugabyte.main_impl()

        (conn_config,), _ = do_discovery_mock.call_args
        self.assertEqual('any', conn_config['load_balance'])
        self.assertEqual('cloud1.region1.zone1,cloud1.region1.zone2', conn_config['topology_keys'])

    def test_load_balance_and_topology_keys_default_to_none(self):
        config = {
            'host': 'my_host',
            'user': 'my_user',
            'password': 'my_password',
            'port': 5433,
            'dbname': 'my_db',
        }

        with patch.object(tap_yugabyte, 'parse_args', return_value=_fake_args(config)), \
                patch.object(tap_yugabyte, 'do_discovery') as do_discovery_mock:
            tap_yugabyte.main_impl()

        (conn_config,), _ = do_discovery_mock.call_args
        self.assertIsNone(conn_config['load_balance'])
        self.assertIsNone(conn_config['topology_keys'])


class TestMainImplKeysetBuckets(unittest.TestCase):
    """main_impl must forward keyset_buckets so the scan matches the operator's bucket index"""

    base_config = {
        'host': 'my_host',
        'user': 'my_user',
        'password': 'my_password',
        'port': 5433,
        'dbname': 'my_db',
    }

    def _conn_config(self, **extra):
        with patch.object(tap_yugabyte, 'parse_args', return_value=_fake_args({**self.base_config, **extra})), \
                patch.object(tap_yugabyte, 'do_discovery') as do_discovery_mock:
            tap_yugabyte.main_impl()

        (conn_config,), _ = do_discovery_mock.call_args
        return conn_config

    def test_keyset_buckets_is_forwarded_as_int(self):
        self.assertEqual(8, self._conn_config(keyset_buckets=8)['keyset_buckets'])
        self.assertEqual(8, self._conn_config(keyset_buckets='8')['keyset_buckets'])

    def test_keyset_buckets_bounds_are_accepted(self):
        self.assertEqual(2, self._conn_config(keyset_buckets=2)['keyset_buckets'])
        self.assertEqual(1024, self._conn_config(keyset_buckets=1024)['keyset_buckets'])

    def test_absent_keyset_buckets_leaves_the_default_to_the_sync(self):
        self.assertNotIn('keyset_buckets', self._conn_config())

    def test_invalid_keyset_buckets_fail_before_discovery(self):
        for value in (0, 1, 1025, -3, 'abc', '8; SELECT 1', 8.5, True):
            with self.subTest(value=value):
                with patch.object(tap_yugabyte, 'parse_args',
                                  return_value=_fake_args({**self.base_config, 'keyset_buckets': value})), \
                        patch.object(tap_yugabyte, 'do_discovery') as do_discovery_mock:
                    with self.assertRaisesRegex(ValueError, 'keyset_buckets'):
                        tap_yugabyte.main_impl()
                do_discovery_mock.assert_not_called()


class TestRegisterTypeAdapters(unittest.TestCase):
    """register_type_adapters must give psycopg2 array casters for types it returns as raw text"""

    @staticmethod
    def _cursor(type_oids, enum_oids):
        cur = MagicMock()
        executed = []

        def execute(sql, params=None):
            executed.append(params[0] if params else sql)

        def fetchone():
            return (type_oids[executed[-1]],) if type_oids.get(executed[-1]) else None

        cur.execute.side_effect = execute
        cur.fetchone.side_effect = fetchone
        cur.fetchall.return_value = [(oid,) for oid in enum_oids]
        return cur

    def _register(self, cur):
        conn = MagicMock()
        conn.__enter__.return_value.cursor.return_value.__enter__.return_value = cur
        with patch('tap_yugabyte.post_db.open_connection', return_value=conn), \
                patch('psycopg2.extensions.register_type') as register_type, \
                patch('psycopg2.extensions.new_array_type', side_effect=lambda *args: args) as new_array_type, \
                patch('psycopg2.extras.register_default_json') as register_json, \
                patch('psycopg2.extras.register_default_jsonb') as register_jsonb:
            tap_yugabyte.register_type_adapters({'dbname': 'db'})
        register_json.assert_not_called()
        register_jsonb.assert_not_called()
        self.assertEqual(new_array_type.call_count, register_type.call_count)
        return [c.args[0] for c in register_type.call_args_list]

    def test_registers_string_array_casters_for_raw_text_types(self):
        cur = self._cursor({'citext': 1001, 'bit': 1561, 'uuid': 2951, 'money': 791}, [5001, 5002])

        self.assertEqual([
            ((1001,), 'CITEXT[]', psycopg2.STRING),
            ((1561,), 'BIT[]', psycopg2.STRING),
            ((2951,), 'UUID[]', psycopg2.STRING),
            ((791,), 'MONEY[]', psycopg2.STRING),
            ((5001,), 'ENUM_5001[]', psycopg2.STRING),
            ((5002,), 'ENUM_5002[]', psycopg2.STRING),
        ], self._register(cur))

    def test_skips_citext_when_extension_is_not_installed(self):
        cur = self._cursor({'bit': 1561, 'uuid': 2951, 'money': 791}, [])

        self.assertEqual(['BIT[]', 'UUID[]', 'MONEY[]'], [name for _, name, _ in self._register(cur)])


class TestSyncTraditionalStreamRegistersTypeAdapters(unittest.TestCase):
    """Every SELECT-based sync method must register the array casters before reading rows"""

    stream = {
        'tap_stream_id': 'public-t',
        'schema': {'properties': {'id': {}}},
        'metadata': [{'breadcrumb': ['properties', 'id'], 'metadata': {'selected': True}}],
    }

    def test_registers_before_each_sync_method(self):
        for sync_method, target in (('full', 'tap_yugabyte.do_sync_full_table'),
                                    ('incremental', 'tap_yugabyte.do_sync_incremental'),
                                    ('logical_initial', 'tap_yugabyte.full_table.sync_table'),
                                    ('logical_initial_interrupted', 'tap_yugabyte.full_table.sync_table')):
            with self.subTest(sync_method=sync_method):
                calls = Mock()

                def sync(*args):
                    calls.sync()
                    return args[2]

                with patch('tap_yugabyte.register_type_adapters', calls.register), \
                        patch(target, side_effect=sync), \
                        patch('tap_yugabyte.sync_common.send_schema_message'), \
                        patch('tap_yugabyte.singer.write_message'):
                    tap_yugabyte.sync_traditional_stream({'dbname': 'db'}, self.stream, {}, sync_method, 1)

                self.assertEqual([call.register({'dbname': 'db'}), call.sync()], calls.mock_calls)

    def test_skips_registration_when_no_columns_are_selected(self):
        stream = {**self.stream, 'metadata': [{'breadcrumb': ['properties', 'id'], 'metadata': {'selected': False}}]}
        with patch('tap_yugabyte.register_type_adapters') as register:
            tap_yugabyte.sync_traditional_stream({'dbname': 'db'}, stream, {}, 'full', None)

        register.assert_not_called()


class TestLogicalReplicationPort(unittest.TestCase):
    """LOG_BASED must fail before any work when the YugabyteDB driver would connect to the wrong port"""

    @staticmethod
    def _stream(replication_method):
        return {
            'tap_stream_id': 'public-t',
            'schema': {'properties': {'id': {}}},
            'metadata': [{'breadcrumb': [], 'metadata': {'selected': True, 'replication-method': replication_method}}],
        }

    def test_ensure_replication_port_accepts_5433(self):
        for port in (5433, '5433'):
            with self.subTest(port=port):
                tap_yugabyte.logical_replication.ensure_replication_port({'port': port})

    def test_ensure_replication_port_rejects_other_ports(self):
        for port in (15436, '5432'):
            with self.subTest(port=port):
                with self.assertRaises(tap_yugabyte.logical_replication.UnsupportedReplicationPortError) as context:
                    tap_yugabyte.logical_replication.ensure_replication_port({'port': port})

                self.assertIn(f'port {port} is configured', str(context.exception))
                self.assertIn('psycopg2-yugabytedb-binary 2.9.3.5 and 2.9.3.5.post1', str(context.exception))

    def test_do_sync_rejects_log_based_on_other_port_before_fetching_lsn(self):
        catalog = {'streams': [self._stream('LOG_BASED')]}
        with patch('tap_yugabyte.logical_replication.fetch_current_lsn') as fetch_current_lsn, \
                patch('tap_yugabyte.refresh_streams_schema') as refresh_streams_schema:
            with self.assertRaises(tap_yugabyte.logical_replication.UnsupportedReplicationPortError):
                tap_yugabyte.do_sync({'port': 15436}, catalog, 'FULL_TABLE', {'bookmarks': {}})

        fetch_current_lsn.assert_not_called()
        refresh_streams_schema.assert_not_called()

    def test_do_sync_allows_other_port_without_log_based_streams(self):
        catalog = {'streams': [self._stream('FULL_TABLE')]}
        with patch('tap_yugabyte.logical_replication.ensure_replication_port') as ensure_replication_port, \
                patch('tap_yugabyte.refresh_streams_schema'), \
                patch('tap_yugabyte.sync_traditional_stream', side_effect=lambda *args: args[2]):
            tap_yugabyte.do_sync({'port': 15436}, catalog, 'FULL_TABLE', {'bookmarks': {}})

        ensure_replication_port.assert_not_called()


class TestSyncLogicalStreamsBookmarks(unittest.TestCase):
    """sync_logical_streams prunes only deselected LOG_BASED bookmarks before streaming"""

    def test_keeps_deselected_fastsync_bookmark_and_drops_deselected_log_based_one(self):
        selected = {'tap_stream_id': 'public-selected', 'schema': {'properties': {}}}
        state = {
            'currently_syncing': None,
            'bookmarks': {
                'public-selected': {'lsn': 5, 'version': 1, 'last_replication_method': 'LOG_BASED'},
                # FastSync writes bookmarks without last_replication_method
                'public-fastsync_only': {'lsn': 1, 'version': 1},
                'public-deselected': {'lsn': 2, 'version': 1, 'last_replication_method': 'LOG_BASED'},
            },
        }
        with patch('tap_yugabyte.logical_replication.sync_tables',
                   side_effect=lambda _conn, _streams, state, *_args: state) as sync_tables:
            result = tap_yugabyte.sync_logical_streams({}, [selected], state, 10, 'state.json')

        self.assertEqual(
            {
                'public-selected': {'lsn': 5, 'version': 1, 'last_replication_method': 'LOG_BASED'},
                'public-fastsync_only': {'lsn': 1, 'version': 1},
            },
            result['bookmarks'],
        )
        sync_tables.assert_called_once()
