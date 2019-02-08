import unittest
import json
import datetime
import target_snowflake
from target_snowflake.db_sync import DbSync

from nose.tools import assert_raises 


try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils


class TestLoad(unittest.TestCase):


    @classmethod
    def setUp(self):
        self.config = test_utils.get_test_config()
        snowflake = DbSync(self.config)
        snowflake.query("DROP SCHEMA {}".format(self.config['schema']))


    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-json.json')
        with assert_raises(json.decoder.JSONDecodeError):
            target_snowflake.persist_lines(self.config, tap_lines)


    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-message-order.json')
        with assert_raises(Exception):
            target_snowflake.persist_lines(self.config, tap_lines)


    def test_loading_tables(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        target_snowflake.persist_lines(self.config, tap_lines)

        snowflake = DbSync(self.config)
        table_one = snowflake.query("SELECT * FROM {}.test_table_one".format(self.config['schema']))
        table_two = snowflake.query("SELECT * FROM {}.test_table_two".format(self.config['schema']))
        table_three = snowflake.query("SELECT * FROM {}.test_table_three".format(self.config['schema']))

        self.assertEqual(
            table_one,
            [
                {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1'}
            ])

        self.assertEqual(
            table_two,
            [
                {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1', 'C_DATE': datetime.datetime(2019, 2, 1, 15, 12, 45)},
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2', 'C_DATE': datetime.datetime(2019, 2, 10, 2, 0, 0)}
            ])

        self.assertEqual(
            table_three,
            [
                {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1'},
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2'},
                {'C_INT': 3, 'C_PK': 3, 'C_VARCHAR': '3'}
            ])
