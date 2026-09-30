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
