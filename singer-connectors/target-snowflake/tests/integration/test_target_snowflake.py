import unittest
import os
import json
import datetime
import target_snowflake
import snowflake

from nose.tools import assert_raises 
from target_snowflake.db_sync import DbSync
from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.remote_storage_util import SnowflakeFileEncryptionMaterial

try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils


class TestIntegration(unittest.TestCase):
    """
    Integration Tests
    """
    @classmethod
    def setUp(self):
        self.config = test_utils.get_test_config()
        snowflake = DbSync(self.config)
        if self.config['schema']:
            snowflake.query("DROP SCHEMA IF EXISTS {}".format(self.config['schema']))


    def assert_three_streams_are_into_snowflake(self):
        """
        This is a helper assertion that checks if every data from the message-with-three-streams.json
        file is available in Snowflake tables correctly.
        Useful to check different loading methods (unencrypted, Client-Side encryption, gzip, etc.)
        without duplicating assertions
        """
        snowflake = DbSync(self.config)
        config_schema = self.config.get('schema', '')
        config_dynamic_schema_name = self.config.get('dynamic_schema_name', '')
        config_dynamic_schema_name_postfix = self.config.get('dynamic_schema_name_postfix', '')

        # Identify target schema name
        target_schema = None
        if config_schema is not None and config_schema.strip():
            target_schema = config_schema
        elif config_dynamic_schema_name:
            target_schema = "tap_mysql_test"
            if config_dynamic_schema_name_postfix:
                target_schema = "{}{}".format(target_schema, config_dynamic_schema_name_postfix)

        table_one = snowflake.query("SELECT * FROM {}.test_table_one".format(target_schema))
        table_two = snowflake.query("SELECT * FROM {}.test_table_two".format(target_schema))
        table_three = snowflake.query("SELECT * FROM {}.test_table_three".format(target_schema))

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


    def test_loading_tables_with_no_encryption(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning off client-side encryption and load
        self.config['client_side_encryption_master_key'] = ''
        target_snowflake.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_into_snowflake()


    def test_loading_tables_with_client_side_encryption(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on client-side encryption and load
        self.config['client_side_encryption_master_key'] = os.environ.get('CLIENT_SIDE_ENCRYPTION_MASTER_KEY')
        target_snowflake.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_into_snowflake()


    def test_loading_tables_with_client_side_encryption_and_wrong_master_key(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on client-side encryption and load but using a well formatted but wrong master key
        self.config['client_side_encryption_master_key'] = "Wr0n6m45t3rKeY0123456789a0123456789a0123456="
        with assert_raises(snowflake.connector.errors.ProgrammingError):
            target_snowflake.persist_lines(self.config, tap_lines)

