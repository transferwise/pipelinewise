"""MongoDB discovery reports seeds without repeating URI credentials."""

from argparse import Namespace
import unittest
from unittest.mock import patch

import singer.logger as singer_logger
import tap_mongodb


class TestSourceLogging(unittest.TestCase):
    def test_discovery_logs_seed_hosts_once_without_uri_credentials(self):
        for srv in ('true', 'false'):
            with self.subTest(srv=srv):
                host = 'seed.example' if srv == 'true' else 'seed-one.example,seed-two.example'
                config = {'host': host, 'port': 27017, 'user': 'private-login', 'password': 'private-password',
                          'database': 'analytics', 'auth_database': 'admin', 'srv': srv}
                args = Namespace(config=config, discover=True)
                with patch.object(singer_logger, '_REPORTED_SOURCE_HOSTS', set()), \
                        patch.object(tap_mongodb.utils, 'parse_args', return_value=args), \
                        patch.object(tap_mongodb, 'MongoClient') as connect, \
                        patch.object(tap_mongodb, 'do_discover'), \
                        self.assertLogs(tap_mongodb.LOGGER, level='INFO') as logs:
                    connect.return_value.server_info.return_value = {'version': '6.0'}
                    tap_mongodb.main_impl()
                    tap_mongodb.main_impl()
                self.assertIn(host, connect.call_args.args[0])
                self.assertEqual(logs.output, [
                    f'INFO:tap_mongodb:Connecting to MongoDB source host: {host}',
                    'INFO:tap_mongodb:Connected to MongoDB, version: 6.0',
                    'INFO:tap_mongodb:Connected to MongoDB, version: 6.0',
                ])
