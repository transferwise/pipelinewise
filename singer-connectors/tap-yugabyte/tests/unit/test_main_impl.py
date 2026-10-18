import argparse
import unittest
from unittest.mock import patch

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
