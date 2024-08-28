import datetime
import gzip
import json
import tempfile
import unittest
import os
import botocore
import boto3
import target_snowflake

from target_snowflake import RecordValidationException
from target_snowflake.exceptions import PrimaryKeyNotFoundException
from target_snowflake.db_sync import DbSync
from target_snowflake.upload_clients.s3_upload_client import S3UploadClient

from unittest import mock
from pyarrow.lib import ArrowTypeError
from snowflake.connector.errors import ProgrammingError
from snowflake.connector.errors import DatabaseError

try:
    import tests.integration.utils as test_utils
except ImportError:
    import utils as test_utils

METADATA_COLUMNS = [
    '_SDC_EXTRACTED_AT',
    '_SDC_BATCHED_AT',
    '_SDC_DELETED_AT'
]


class TestIntegration(unittest.TestCase):
    """
    Integration Tests
    """
    maxDiff = None

    def setUp(self):
        self.config = test_utils.get_test_config()
        self.snowflake = DbSync(self.config)

        # Drop target schema
        if self.config['default_target_schema']:
            self.snowflake.query("DROP SCHEMA IF EXISTS {}".format(self.config['default_target_schema']))

        if self.config['schema_mapping']:
            for _, val in self.config['schema_mapping'].items():
                self.snowflake.query('drop schema if exists {}'.format(val['target_schema']))

        # Set up S3 client
        aws_access_key_id = self.config.get('aws_access_key_id')
        aws_secret_access_key = self.config.get('aws_secret_access_key')
        aws_session_token = self.config.get('aws_session_token')
        aws_session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )

        self.s3_client = aws_session.client('s3',
                                            region_name=self.config.get('s3_region_name'),
                                            endpoint_url=self.config.get('s3_endpoint_url'))

    def persist_lines(self, lines):
        """Loads singer messages into snowflake without table caching option"""
        target_snowflake.persist_lines(self.config, lines)

    def persist_lines_with_cache(self, lines):
        """Enables table caching option and loads singer messages into snowflake.

        Table caching mechanism is creating and maintaining an extra table in snowflake about
        the table structures. It's very similar to the INFORMATION_SCHEMA.COLUMNS system views
        but querying INFORMATION_SCHEMA is slow especially when lot of taps running
        in parallel.

        Selecting from a real table instead of INFORMATION_SCHEMA and keeping it
        in memory while the target-snowflake is running results better load performance.
        """
        table_cache, file_format_type = target_snowflake.get_snowflake_statics(self.config)
        target_snowflake.persist_lines(self.config, lines, table_cache, file_format_type)

    def remove_metadata_columns_from_rows(self, rows):
        """Removes metadata columns from a list of rows"""
        d_rows = []
        for r in rows:
            # Copy the original row to a new dict to keep the original dict
            # and remove metadata columns
            d_row = r.copy()
            for md_c in METADATA_COLUMNS:
                d_row.pop(md_c, None)

            # Add new row without metadata columns to the new list
            d_rows.append(d_row)

        return d_rows

    def assert_metadata_columns_exist(self, rows):
        """This is a helper assertion that checks if every row in a list has metadata columns"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                self.assertTrue(md_c in r)

    def assert_metadata_columns_not_exist(self, rows):
        """This is a helper assertion that checks metadata columns don't exist in any row"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                self.assertFalse(md_c in r)

    def assert_three_streams_are_into_snowflake(self, should_metadata_columns_exist=False,
                                                should_hard_deleted_rows=False):
        """
        This is a helper assertion that checks if every data from the message-with-three-streams.json
        file is available in Snowflake tables correctly.
        Useful to check different loading methods (unencrypted, Client-Side encryption, gzip, etc.)
        without duplicating assertions
        """
        snowflake = DbSync(self.config)
        default_target_schema = self.config.get('default_target_schema', '')
        schema_mapping = self.config.get('schema_mapping', {})

        # Identify target schema name
        target_schema = None
        if default_target_schema is not None and default_target_schema.strip():
            target_schema = default_target_schema
        elif schema_mapping:
            target_schema = "tap_mysql_test"

        # Get loaded rows from tables
        table_one = snowflake.query("SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema))
        table_two = snowflake.query("SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema))
        table_three = snowflake.query("SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1'}
        ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_one), expected_table_one)

        # ----------------------------------------------------------------------
        # Check rows in table_two
        # ----------------------------------------------------------------------
        expected_table_two = []
        if not should_hard_deleted_rows:
            expected_table_two = [
                {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1', 'C_DATE': datetime.datetime(2019, 2, 1, 15, 12, 45), 'C_ISO_DATE':datetime.date(2019, 2, 1)},
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2', 'C_DATE': datetime.datetime(2019, 2, 10, 2, 0, 0), 'C_ISO_DATE':datetime.date(2019, 2, 10)}
            ]
        else:
            expected_table_two = [
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2', 'C_DATE': datetime.datetime(2019, 2, 10, 2, 0, 0), 'C_ISO_DATE':datetime.date(2019, 2, 10)}
            ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_two), expected_table_two)

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = []
        if not should_hard_deleted_rows:
            expected_table_three = [
                {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1', 'C_TIME': datetime.time(4, 0, 0)},
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2', 'C_TIME': datetime.time(7, 15, 0)},
                {'C_INT': 3, 'C_PK': 3, 'C_VARCHAR': '3', 'C_TIME': datetime.time(23, 0, 3)}
            ]
        else:
            expected_table_three = [
                {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1', 'C_TIME': datetime.time(4, 0, 0)},
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2', 'C_TIME': datetime.time(7, 15, 0)}
            ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_three), expected_table_three)

        # ----------------------------------------------------------------------
        # Check if metadata columns exist or not
        # ----------------------------------------------------------------------
        if should_metadata_columns_exist:
            self.assert_metadata_columns_exist(table_one)
            self.assert_metadata_columns_exist(table_two)
            self.assert_metadata_columns_exist(table_three)
        else:
            self.assert_metadata_columns_not_exist(table_one)
            self.assert_metadata_columns_not_exist(table_two)
            self.assert_metadata_columns_not_exist(table_three)

    def assert_logical_streams_are_in_snowflake(self, should_metadata_columns_exist=False):
        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = snowflake.query("SELECT * FROM {}.logical1_table1 ORDER BY CID".format(target_schema))
        table_two = snowflake.query("SELECT * FROM {}.logical1_table2 ORDER BY CID".format(target_schema))
        table_three = snowflake.query("SELECT * FROM {}.logical2_table1 ORDER BY CID".format(target_schema))
        table_four = snowflake.query("SELECT CID, CTIMENTZ, CTIMETZ FROM {}.logical1_edgydata WHERE CID IN(1,2,3,4,5,6,8,9) ORDER BY CID".format(target_schema))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'CID': 1, 'CVARCHAR': "inserted row", 'CVARCHAR2': None},
            {'CID': 2, 'CVARCHAR': 'inserted row', "CVARCHAR2": "inserted row"},
            {'CID': 3, 'CVARCHAR': "inserted row", 'CVARCHAR2': "inserted row"},
            {'CID': 4, 'CVARCHAR': "inserted row", 'CVARCHAR2': "inserted row"}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_two
        # ----------------------------------------------------------------------
        expected_table_two = [
            {'CID': 1, 'CVARCHAR': "updated row"},
            {'CID': 2, 'CVARCHAR': 'updated row'},
            {'CID': 3, 'CVARCHAR': "updated row"},
            {'CID': 5, 'CVARCHAR': "updated row"},
            {'CID': 7, 'CVARCHAR': "updated row"},
            {'CID': 8, 'CVARCHAR': 'updated row'},
            {'CID': 9, 'CVARCHAR': "updated row"},
            {'CID': 10, 'CVARCHAR': 'updated row'}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = [
            {'CID': 1, 'CVARCHAR': "updated row"},
            {'CID': 2, 'CVARCHAR': 'updated row'},
            {'CID': 3, 'CVARCHAR': "updated row"},
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_four
        # ----------------------------------------------------------------------
        expected_table_four = [
            {'CID': 1, 'CTIMENTZ': None, 'CTIMETZ': None},
            {'CID': 2, 'CTIMENTZ': datetime.time(23, 0, 15), 'CTIMETZ': datetime.time(23, 0, 15)},
            {'CID': 3, 'CTIMENTZ': datetime.time(12, 0, 15), 'CTIMETZ': datetime.time(12, 0, 15)},
            {'CID': 4, 'CTIMENTZ': datetime.time(12, 0, 15), 'CTIMETZ': datetime.time(9, 0, 15)},
            {'CID': 5, 'CTIMENTZ': datetime.time(12, 0, 15), 'CTIMETZ': datetime.time(15, 0, 15)},
            {'CID': 6, 'CTIMENTZ': datetime.time(0, 0), 'CTIMETZ': datetime.time(0, 0)},
            {'CID': 8, 'CTIMENTZ': datetime.time(0, 0), 'CTIMETZ': datetime.time(1, 0)},
            {'CID': 9, 'CTIMENTZ': datetime.time(0, 0), 'CTIMETZ': datetime.time(0, 0)}
        ]

        if should_metadata_columns_exist:
            self.assertEqual(self.remove_metadata_columns_from_rows(table_one), expected_table_one)
            self.assertEqual(self.remove_metadata_columns_from_rows(table_two), expected_table_two)
            self.assertEqual(self.remove_metadata_columns_from_rows(table_three), expected_table_three)
            self.assertEqual(table_four, expected_table_four)
        else:
            self.assertEqual(table_one, expected_table_one)
            self.assertEqual(table_two, expected_table_two)
            self.assertEqual(table_three, expected_table_three)
            self.assertEqual(table_four, expected_table_four)

    def assert_logical_streams_are_in_snowflake_and_are_empty(self):
        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = snowflake.query("SELECT * FROM {}.logical1_table1 ORDER BY CID".format(target_schema))
        table_two = snowflake.query("SELECT * FROM {}.logical1_table2 ORDER BY CID".format(target_schema))
        table_three = snowflake.query("SELECT * FROM {}.logical2_table1 ORDER BY CID".format(target_schema))
        table_four = snowflake.query("SELECT CID, CTIMENTZ, CTIMETZ FROM {}.logical1_edgydata WHERE CID IN(1,2,3,4,5,6,8,9) ORDER BY CID".format(target_schema))

        self.assertEqual(table_one, [])
        self.assertEqual(table_two, [])
        self.assertEqual(table_three, [])
        self.assertEqual(table_four, [])

    def assert_binary_data_are_in_snowflake(self, table_name, should_metadata_columns_exist=False):
        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = snowflake.query("SELECT * FROM {}.{} ORDER BY ID".format(target_schema, table_name))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'ID': b'pk2', 'DATA': b'data2', 'CREATED_AT': datetime.datetime(2019, 12, 17, 16, 2, 55)},
            {'ID': b'pk4', 'DATA': b'data4', "CREATED_AT": datetime.datetime(2019, 12, 17, 16, 32, 22)},
        ]

        if should_metadata_columns_exist:
            self.assertEqual(self.remove_metadata_columns_from_rows(table_one), expected_table_one)
        else:
            self.assertEqual(table_one, expected_table_one)

    #################################
    #           TESTS               #
    #################################

    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-json.json')
        with self.assertRaises(json.decoder.JSONDecodeError):
            self.persist_lines_with_cache(tap_lines)

    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-message-order.json')
        with self.assertRaises(Exception):
            self.persist_lines_with_cache(tap_lines)

    def test_run_query(self):
        """Running SQLs"""
        snowflake = DbSync(self.config)

        # Running single SQL should return as array
        self.assertEqual(snowflake.query("SELECT 1 col1, 2 col2"),
                         [{'COL1': 1, 'COL2': 2}])

        # Running multiple SQLs should return the result of the last query
        self.assertEqual(snowflake.query(["SELECT 1 col1, 2 col2",
                                          "SELECT 3 col1, 4 col2",
                                          "SELECT 5 col1, 6 col2"]),
                         [{'COL1': 5, 'COL2': 6}])

        # Running multiple SQLs should return empty list if the last query returns zero record
        self.assertEqual(snowflake.query(["SELECT 1 col1, 2 col2",
                                          "SELECT 3 col1, 4 col2",
                                          "SELECT 5 col1, 6 col2 WHERE 1 = 2"]),
                         [])

        # Running multiple SQLs should return the result of the last query even if a previous query returns zero record
        self.assertEqual(snowflake.query(["SELECT 1 col1, 2 col2 WHERE 1 =2 ",
                                          "SELECT 3 col1, 4 col2",
                                          "SELECT 5 col1, 6 col2"]),
                         [{'COL1': 5, 'COL2': 6}])

        # Running multiple SQLs should return empty list if every query returns zero record
        self.assertEqual(snowflake.query(["SELECT 1 col1, 2 col2 WHERE 1 = 2 ",
                                          "SELECT 3 col1, 4 col2 WHERE 1 = 2",
                                          "SELECT 5 col1, 6 col2 WHERE 1 = 2"]),
                         [])

    def test_loading_tables_with_no_encryption(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning off client-side encryption and load
        self.config['client_side_encryption_master_key'] = ''
        self.persist_lines_with_cache(tap_lines)

        self.assert_three_streams_are_into_snowflake()

    def test_loading_tables_with_client_side_encryption(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on client-side encryption and load
        self.config['client_side_encryption_master_key'] = os.environ.get('CLIENT_SIDE_ENCRYPTION_MASTER_KEY')
        self.persist_lines_with_cache(tap_lines)

        self.assert_three_streams_are_into_snowflake()

    def test_loading_tables_with_client_side_encryption_and_wrong_master_key(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on client-side encryption and load but using a well formatted but wrong master key
        self.config['client_side_encryption_master_key'] = "Wr0n6m45t3rKeY0123456789a0123456789a0123456="
        with self.assertRaises(ProgrammingError):
            self.persist_lines_with_cache(tap_lines)

    def test_loading_tables_with_metadata_columns(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on adding metadata columns
        self.config['add_metadata_columns'] = True
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_snowflake(should_metadata_columns_exist=True)

    def test_loading_tables_with_defined_parallelism(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Using fixed 1 thread parallelism
        self.config['parallelism'] = 1
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_snowflake()

    def test_loading_tables_with_hard_delete(self):
        """Loading multiple tables from the same input tap with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_snowflake(
            should_metadata_columns_exist=True,
            should_hard_deleted_rows=True
        )

    def test_loading_with_multiple_schema(self):
        """Loading table with multiple SCHEMA messages"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multi-schemas.json')

        # Load with default settings
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly
        self.assert_three_streams_are_into_snowflake(
            should_metadata_columns_exist=False,
            should_hard_deleted_rows=False
        )

    def test_loading_tables_with_binary_columns_and_hard_delete(self):
        """Loading multiple tables from the same input tap with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-binary-columns.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_binary_data_are_in_snowflake(
            table_name='test_binary',
            should_metadata_columns_exist=True
        )

    def test_loading_table_with_reserved_word_as_name_and_hard_delete(self):
        """Loading a table where the name is a reserved word with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-reserved-name-as-table-name.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_binary_data_are_in_snowflake(
            table_name='"ORDER"',
            should_metadata_columns_exist=True
        )

    def test_loading_table_with_space(self):
        """Loading a table where the name has space"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-space-in-table-name.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_binary_data_are_in_snowflake(
            table_name='"TABLE WITH SPACE AND UPPERCASE"',
            should_metadata_columns_exist=True
        )

    def test_loading_unicode_characters(self):
        """Loading unicode encoded characters"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-unicode-characters.json')

        # Load with default settings
        self.persist_lines_with_cache(tap_lines)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_unicode = snowflake.query("SELECT * FROM {}.test_table_unicode ORDER BY C_INT".format(target_schema))

        self.assertEqual(
            table_unicode,
            [
                {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': 'Hello world, Καλημέρα κόσμε, コンニチハ'},
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': 'Chinese: 和毛泽东 <<重上井冈山>>. 严永欣, 一九八八年.'},
                {'C_INT': 3, 'C_PK': 3,
                 'C_VARCHAR': 'Russian: Зарегистрируйтесь сейчас на Десятую Международную Конференцию по'},
                {'C_INT': 4, 'C_PK': 4, 'C_VARCHAR': 'Thai: แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช'},
                {'C_INT': 5, 'C_PK': 5, 'C_VARCHAR': 'Arabic: لقد لعبت أنت وأصدقاؤك لمدة وحصلتم علي من إجمالي النقاط'},
                {'C_INT': 6, 'C_PK': 6, 'C_VARCHAR': 'Special Characters: [",\'!@£$%^&*()]'}
            ])

    def test_non_db_friendly_columns(self):
        """Loading non-db friendly columns like, camelcase, minus signs, etc."""
        tap_lines = test_utils.get_test_tap_lines('messages-with-non-db-friendly-columns.json')

        # Load with default settings
        self.persist_lines_with_cache(tap_lines)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_non_db_friendly_columns = snowflake.query(
            "SELECT * FROM {}.test_table_non_db_friendly_columns ORDER BY c_pk".format(target_schema))

        self.assertEqual(
            table_non_db_friendly_columns,
            [
                {'C_PK': 1, 'CAMELCASECOLUMN': 'Dummy row 1', 'MINUS-COLUMN': 'Dummy row 1'},
                {'C_PK': 2, 'CAMELCASECOLUMN': 'Dummy row 2', 'MINUS-COLUMN': 'Dummy row 2'},
                {'C_PK': 3, 'CAMELCASECOLUMN': 'Dummy row 3', 'MINUS-COLUMN': 'Dummy row 3'},
                {'C_PK': 4, 'CAMELCASECOLUMN': 'Dummy row 4', 'MINUS-COLUMN': 'Dummy row 4'},
                {'C_PK': 5, 'CAMELCASECOLUMN': 'Dummy row 5', 'MINUS-COLUMN': 'Dummy row 5'},
            ])

    def test_nested_schema_unflattening(self):
        """Loading nested JSON objects into VARIANT columns without flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Load with default settings - Flattening disabled
        self.persist_lines_with_cache(tap_lines)

        # Get loaded rows from tables - Transform JSON to string at query time
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        unflattened_table = snowflake.query("""
            SELECT c_pk
                  ,TO_CHAR(c_array) c_array
                  ,TO_CHAR(c_object) c_object
                  ,TO_CHAR(c_object) c_object_with_props
                  ,TO_CHAR(c_nested_object) c_nested_object
              FROM {}.test_table_nested_schema
             ORDER BY c_pk""".format(target_schema))

        # Should be valid nested JSON strings
        self.assertEqual(
            unflattened_table,
            [{
                'C_PK': 1,
                'C_ARRAY': '[1,2,3]',
                'C_OBJECT': '{"key_1":"value_1"}',
                'C_OBJECT_WITH_PROPS': '{"key_1":"value_1"}',
                'C_NESTED_OBJECT': '{"nested_prop_1":"nested_value_1","nested_prop_2":"nested_value_2","nested_prop_3":{"multi_nested_prop_1":"multi_value_1","multi_nested_prop_2":"multi_value_2"}}'
            }])

    def test_nested_schema_flattening(self):
        """Loading nested JSON objects with flattening and not not flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Turning on data flattening
        self.config['data_flattening_max_level'] = 10

        # Load with default settings - Flattening disabled
        self.persist_lines_with_cache(tap_lines)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        flattened_table = snowflake.query(
            "SELECT * FROM {}.test_table_nested_schema ORDER BY c_pk".format(target_schema))

        # Should be flattened columns
        self.assertEqual(
            flattened_table,
            [{
                'C_PK': 1,
                'C_ARRAY': '[\n  1,\n  2,\n  3\n]',
                'C_OBJECT': None,
                # Cannot map RECORD to SCHEMA. SCHEMA doesn't have properties that requires for flattening
                'C_OBJECT_WITH_PROPS__KEY_1': 'value_1',
                'C_NESTED_OBJECT__NESTED_PROP_1': 'nested_value_1',
                'C_NESTED_OBJECT__NESTED_PROP_2': 'nested_value_2',
                'C_NESTED_OBJECT__NESTED_PROP_3__MULTI_NESTED_PROP_1': 'multi_value_1',
                'C_NESTED_OBJECT__NESTED_PROP_3__MULTI_NESTED_PROP_2': 'multi_value_2',
            }])

    def test_column_name_change(self):
        """Tests correct renaming of snowflake columns after source change"""
        tap_lines_before_column_name_change = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        tap_lines_after_column_name_change = test_utils.get_test_tap_lines(
            'messages-with-three-streams-modified-column.json')

        # Load with default settings
        self.persist_lines_with_cache(tap_lines_before_column_name_change)
        self.persist_lines_with_cache(tap_lines_after_column_name_change)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = snowflake.query("SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema))
        table_two = snowflake.query("SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema))
        table_three = snowflake.query("SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema))

        # Get the previous column name from information schema in test_table_two
        previous_column_name = snowflake.query("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_catalog = '{}'
               AND table_schema = '{}'
               AND table_name = 'TEST_TABLE_TWO'
               AND ordinal_position = 1
            """.format(
            self.config.get('dbname', '').upper(),
            target_schema.upper()))[0]["COLUMN_NAME"]

        # Table one should have no changes
        self.assertEqual(
            table_one,
            [{'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1'}])

        # Table two should have a versioned column and a new column
        self.assertEqual(
            table_two,
            [
                {previous_column_name: datetime.datetime(2019, 2, 1, 15, 12, 45), 'C_INT': 1, 'C_PK': 1,
                 'C_VARCHAR': '1', 'C_DATE': None, 'C_ISO_DATE': datetime.date(2019, 2, 1), 'C_NEW_COLUMN': None},
                {previous_column_name: datetime.datetime(2019, 2, 10, 2), 'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2',
                 'C_DATE': '2019-02-12 02:00:00', 'C_ISO_DATE': datetime.date(2019, 2, 10), 'C_NEW_COLUMN': 'data 1'},
                {previous_column_name: None, 'C_INT': 3, 'C_PK': 3, 'C_VARCHAR': '2', 'C_DATE': '2019-02-15 02:00:00',
                 'C_ISO_DATE': datetime.date(2019, 2, 15), 'C_NEW_COLUMN': 'data 2'}
            ]
        )

        # Table three should have a renamed columns and a new column
        self.assertEqual(
            table_three,
            [
                {'C_INT': 1, 'C_PK': 1, 'C_TIME': datetime.time(4, 0), 'C_VARCHAR': '1', 'C_TIME_RENAMED': None,
                 'C_NEW_COLUMN': None},
                {'C_INT': 2, 'C_PK': 2, 'C_TIME': datetime.time(7, 15), 'C_VARCHAR': '2', 'C_TIME_RENAMED': None,
                 'C_NEW_COLUMN': None},
                {'C_INT': 3, 'C_PK': 3, 'C_TIME': datetime.time(23, 0, 3), 'C_VARCHAR': '3',
                 'C_TIME_RENAMED': datetime.time(8, 15), 'C_NEW_COLUMN': 'data 1'},
                {'C_INT': 4, 'C_PK': 4, 'C_TIME': None, 'C_VARCHAR': '4', 'C_TIME_RENAMED': datetime.time(23, 0, 3),
                 'C_NEW_COLUMN': 'data 2'}
            ])

    def test_column_name_change_without_table_cache(self):
        """Tests correct renaming of snowflake columns after source change with not using table caching"""
        tap_lines_before_column_name_change = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        tap_lines_after_column_name_change = test_utils.get_test_tap_lines(
            'messages-with-three-streams-modified-column.json')

        # Load with default settings
        self.persist_lines(tap_lines_before_column_name_change)
        self.persist_lines(tap_lines_after_column_name_change)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = snowflake.query("SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema))
        table_two = snowflake.query("SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema))
        table_three = snowflake.query("SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema))

        # Get the previous column name from information schema in test_table_two
        previous_column_name = snowflake.query("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_catalog = '{}'
               AND table_schema = '{}'
               AND table_name = 'TEST_TABLE_TWO'
               AND ordinal_position = 1
            """.format(
            self.config.get('dbname', '').upper(),
            target_schema.upper()))[0]["COLUMN_NAME"]

        # Table one should have no changes
        self.assertEqual(
            table_one,
            [{'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1'}])

        # Table two should have a versioned column and a new column
        self.assertEqual(
            table_two,
            [
                {previous_column_name: datetime.datetime(2019, 2, 1, 15, 12, 45), 'C_INT': 1, 'C_PK': 1,
                 'C_VARCHAR': '1', 'C_DATE': None, 'C_ISO_DATE': datetime.date(2019, 2, 1), 'C_NEW_COLUMN': None},
                {previous_column_name: datetime.datetime(2019, 2, 10, 2), 'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2',
                 'C_DATE': '2019-02-12 02:00:00', 'C_ISO_DATE': datetime.date(2019, 2, 10), 'C_NEW_COLUMN': 'data 1'},
                {previous_column_name: None, 'C_INT': 3, 'C_PK': 3, 'C_VARCHAR': '2', 'C_DATE': '2019-02-15 02:00:00',
                 'C_ISO_DATE': datetime.date(2019, 2, 15), 'C_NEW_COLUMN': 'data 2'}
            ]            
        )

        # Table three should have a renamed columns and a new column
        self.assertEqual(
            table_three,
            [
                {'C_INT': 1, 'C_PK': 1, 'C_TIME': datetime.time(4, 0), 'C_VARCHAR': '1', 'C_TIME_RENAMED': None,
                 'C_NEW_COLUMN': None},
                {'C_INT': 2, 'C_PK': 2, 'C_TIME': datetime.time(7, 15), 'C_VARCHAR': '2', 'C_TIME_RENAMED': None,
                 'C_NEW_COLUMN': None},
                {'C_INT': 3, 'C_PK': 3, 'C_TIME': datetime.time(23, 0, 3), 'C_VARCHAR': '3',
                 'C_TIME_RENAMED': datetime.time(8, 15), 'C_NEW_COLUMN': 'data 1'},
                {'C_INT': 4, 'C_PK': 4, 'C_TIME': None, 'C_VARCHAR': '4', 'C_TIME_RENAMED': datetime.time(23, 0, 3),
                 'C_NEW_COLUMN': 'data 2'}
            ])

    def test_logical_streams_from_pg_with_hard_delete_and_default_batch_size_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines_with_cache(tap_lines)

        self.assert_logical_streams_are_in_snowflake(True)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        self.persist_lines_with_cache(tap_lines)

        self.assert_logical_streams_are_in_snowflake(True)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_and_no_records_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams-no-records.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        self.persist_lines_with_cache(tap_lines)

        self.assert_logical_streams_are_in_snowflake_and_are_empty()

    @mock.patch('target_snowflake.emit_state')
    def test_flush_streams_with_no_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when no intermediate flush required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size big enough to never has to flush in the middle
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 1000
        self.persist_lines_with_cache(tap_lines)

        # State should be emitted only once with the latest received STATE message
        self.assertEqual(
            mock_emit_state.mock_calls,
            [
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}})
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_snowflake(True)

    @mock.patch('target_snowflake.emit_state')
    def test_flush_streams_with_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when intermediate flushes required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        self.persist_lines_with_cache(tap_lines)

        # State should be emitted multiple times, updating the positions only in the stream which got flushed
        self.assertEqual(
            mock_emit_state.call_args_list,
            [
                # Flush #1 - Flushed edgydata until lsn: 108197216
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108196176,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #2 - Flushed logical1-logical1_table2 until lsn: 108201336
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108201336,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #3 - Flushed logical1-logical1_table2 until lsn: 108237600
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108237600,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #4 - Flushed logical1-logical1_table2 until lsn: 108238768
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108238768,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #5 - Flushed logical1-logical1_table2 until lsn: 108239704,
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108239896,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #6 - Last flush, update every stream lsn: 108240872,
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_snowflake(True)

    @mock.patch('target_snowflake.emit_state')
    def test_flush_streams_with_intermediate_flushes_on_all_streams(self, mock_emit_state):
        """Test emitting states when intermediate flushes required and flush_all_streams is enabled"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        self.config['flush_all_streams'] = True
        self.persist_lines_with_cache(tap_lines)

        # State should be emitted 6 times, flushing every stream and updating every stream position
        self.assertEqual(
            mock_emit_state.call_args_list,
            [
                # Flush #1 - Flush every stream until lsn: 108197216
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108197216,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108197216,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108197216,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #2 - Flush every stream until lsn 108201336
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108201336,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108201336,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108201336,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108201336,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #3 - Flush every stream until lsn: 108237600
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108237600,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108237600,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108237600,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108237600,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #4 - Flush every stream until lsn: 108238768
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108238768,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108238768,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108238768,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108238768,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #5 - Flush every stream until lsn: 108239704,
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108239896,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108239896,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108239896,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108239896,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #6 - Last flush, update every stream until lsn: 108240872,
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872,
                                                   "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872,
                                                 "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872,
                                                 "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872,
                                                 "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id",
                                    "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_snowflake(True)

    @mock.patch('target_snowflake.emit_state')
    def test_flush_streams_based_on_batch_wait_limit(self, mock_emit_state):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        mock_emit_state.get.return_value = None

        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 1000
        self.config['batch_wait_limit_seconds'] = 0.1
        self.persist_lines_with_cache(tap_lines)

        self.assert_logical_streams_are_in_snowflake(True)
        self.assertGreater(mock_emit_state.call_count, 1, 'Expecting multiple flushes')

    def test_record_validation(self):
        """Test validating records"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-invalid-records.json')

        # Loading invalid records when record validation enabled should fail at ...
        self.config['validate_records'] = True
        with self.assertRaises(RecordValidationException):
            self.persist_lines_with_cache(tap_lines)

        # Loading invalid records when record validation disabled should fail at load time
        self.config['validate_records'] = False
        if self.config['file_format'] == os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT_CSV'):
            with self.assertRaises(ProgrammingError):
                self.persist_lines_with_cache(tap_lines)

        if self.config['file_format'] == os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT_PARQUET'):
            with self.assertRaises(ArrowTypeError):
                self.persist_lines_with_cache(tap_lines)

    def test_pg_records_validation(self):
        """Test validating records from postgres tap"""
        tap_lines_invalid_records = test_utils.get_test_tap_lines('messages-pg-with-invalid-records.json')

        # Loading invalid records when record validation enabled should fail at ...
        self.config['validate_records'] = True
        with self.assertRaises(RecordValidationException):
            self.persist_lines_with_cache(tap_lines_invalid_records)

        # Loading invalid records when record validation disabled, should pass without any exceptions
        self.config['validate_records'] = False
        self.persist_lines_with_cache(tap_lines_invalid_records)

        # Valid records should pass for both with and without validation
        tap_lines_valid_records = test_utils.get_test_tap_lines('messages-pg-with-valid-records.json')

        self.config['validate_records'] = True
        self.persist_lines_with_cache(tap_lines_valid_records)

    def test_loading_tables_with_custom_temp_dir(self):
        """Loading multiple tables from the same input tap using custom temp directory"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on client-side encryption and load
        self.config['temp_dir'] = ('~/.pipelinewise/tmp')
        self.persist_lines_with_cache(tap_lines)

        self.assert_three_streams_are_into_snowflake()

    def test_aws_env_vars(self):
        """Test loading data with credentials defined in AWS environment variables
        than explicitly provided access keys"""
        tap_lines = test_utils.get_test_tap_lines("messages-with-three-streams.json")

        try:
            # Save original config to restore later
            orig_config = self.config.copy()

            # Move aws access key and secret from config into environment variables
            os.environ['AWS_ACCESS_KEY_ID'] = os.environ.get('TARGET_SNOWFLAKE_AWS_ACCESS_KEY')
            os.environ['AWS_SECRET_ACCESS_KEY'] = os.environ.get('TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY')
            del self.config['aws_access_key_id']
            del self.config['aws_secret_access_key']

            # Create a new S3 client using env vars
            s3Client = S3UploadClient(self.config)
            s3Client._create_s3_client()

        # Restore the original state to not confuse other tests
        finally:
            del os.environ['AWS_ACCESS_KEY_ID']
            del os.environ['AWS_SECRET_ACCESS_KEY']
            self.config = orig_config.copy()

    def test_profile_based_auth(self):
        """Test AWS profile based authentication rather than access keys"""
        try:
            # Save original config to restore later
            orig_config = self.config.copy()

            # Remove access keys from config and add profile name
            del self.config['aws_access_key_id']
            del self.config['aws_secret_access_key']
            self.config['aws_profile'] = 'fake-profile'

            # Create a new S3 client using profile based authentication
            with self.assertRaises(botocore.exceptions.ProfileNotFound):
                s3UploaddClient = S3UploadClient(self.config)
                s3UploaddClient._create_s3_client()

        # Restore the original state to not confuse other tests
        finally:
            self.config = orig_config.copy()

    def test_profile_based_auth_aws_env_var(self):
        """Test AWS profile based authentication using AWS environment variables"""
        try:
            # Save original config to restore later
            orig_config = self.config.copy()

            # Remove access keys from config and add profile name environment variable
            del self.config['aws_access_key_id']
            del self.config['aws_secret_access_key']
            os.environ['AWS_PROFILE'] = 'fake_profile'

            # Create a new S3 client using profile based authentication
            with self.assertRaises(botocore.exceptions.ProfileNotFound):
                s3UploaddClient = S3UploadClient(self.config)
                s3UploaddClient._create_s3_client()

        # Restore the original state to not confuse other tests
        finally:
            del os.environ['AWS_PROFILE']
            self.config = orig_config.copy()

    def test_s3_custom_endpoint_url(self):
        """Test S3 connection with custom region and endpoint URL"""
        try:
            # Save original config to restore later
            orig_config = self.config.copy()

            # Define custom S3 endpoint
            self.config['s3_endpoint_url'] = 'fake-endpoint-url'

            # Botocore should raise ValurError in case of fake S3 endpoint url
            with self.assertRaises(ValueError):
                s3UploaddClient = S3UploadClient(self.config)
                s3UploaddClient._create_s3_client()

        # Restore the original state to not confuse other tests
        finally:
            self.config = orig_config.copy()

    def test_too_many_records_exception(self):
        """Test if query function raise exception if max_records exceeded"""
        snowflake = DbSync(self.config)

        # No max_record limit by default
        sample_rows = snowflake.query("SELECT seq4() FROM TABLE(GENERATOR(ROWCOUNT => 50000))")
        self.assertEqual(len(sample_rows), 50000)

        # Should raise exception when max_records exceeded
        with self.assertRaises(target_snowflake.db_sync.TooManyRecordsException):
            snowflake.query("SELECT seq4() FROM TABLE(GENERATOR(ROWCOUNT => 50000))", max_records=10000)

    def test_loading_tables_with_no_compression(self):
        """Loading multiple tables with compression turned off"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning off client-side encryption and load
        self.config['no_compression'] = True
        self.persist_lines_with_cache(tap_lines)

        self.assert_three_streams_are_into_snowflake()

    def test_quoted_identifiers_ignore_case_session_parameter(self):
        """Test if QUOTED_IDENTIFIERS_IGNORE_CASE session parameter set to FALSE"""
        snowflake = DbSync(self.config)

        # Set QUOTED_IDENTIFIERS_IGNORE_CASE to True on user level
        snowflake.query(f"ALTER USER {self.config['user']} SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE")

        # Quoted column names should be case sensitive even if the
        # QUOTED_IDENTIFIERS_IGNORE_CASE parameter set to TRUE on user or account level
        result = snowflake.query('SELECT 1 AS "Foo", 1 AS "foo", 1 AS "FOO", 1 AS foo, 1 AS FOO')
        self.assertEqual(result, [{
            'Foo': 1,
            'foo': 1,
            'FOO': 1
        }])

        # Reset parameters default
        snowflake.query(f"ALTER USER {self.config['user']} UNSET QUOTED_IDENTIFIERS_IGNORE_CASE")

    def test_query_tagging(self):
        """Loading multiple tables with query tagging"""
        snowflake = DbSync(self.config)
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        current_time = datetime.datetime.now().strftime('%H:%M:%s')

        # Tag queries with dynamic schema and table tokens
        self.config['query_tag'] = f'PPW test tap run at {current_time}. ' \
                                   f'Loading into {{{{database}}}}.{{{{schema}}}}.{{{{table}}}}'
        self.persist_lines_with_cache(tap_lines)

        # Get query tags from QUERY_HISTORY
        result = snowflake.query(f"""SELECT query_tag, count(*) queries
                                 FROM table(information_schema.query_history_by_user('{self.config['user']}'))
                                 WHERE query_tag like '%%PPW test tap run at {current_time}%%'
                                 GROUP BY query_tag
                                 ORDER BY 1""")

        target_db = self.config['dbname']
        target_schema = self.config['default_target_schema']
        self.assertEqual(result, [{
            'QUERY_TAG': f'PPW test tap run at {current_time}. Loading into {target_db}..',
            'QUERIES': 4
        },
            {
                'QUERY_TAG': f'PPW test tap run at {current_time}. Loading into {target_db}.{target_schema}.TEST_TABLE_ONE',
                'QUERIES': 10
            },
            {
                'QUERY_TAG': f'PPW test tap run at {current_time}. Loading into {target_db}.{target_schema}.TEST_TABLE_THREE',
                'QUERIES': 9
            },
            {
                'QUERY_TAG': f'PPW test tap run at {current_time}. Loading into {target_db}.{target_schema}.TEST_TABLE_TWO',
                'QUERIES': 9
            }
        ])

        # Detecting file format type should run only once
        result = snowflake.query(f"""SELECT count(*) show_file_format_queries
                                 FROM table(information_schema.query_history_by_user('{self.config['user']}'))
                                 WHERE query_tag like '%%PPW test tap run at {current_time}%%'
                                   AND query_text like 'SHOW FILE FORMATS%%'""")
        self.assertEqual(result, [{
            'SHOW_FILE_FORMAT_QUERIES': 1
        }
        ])

    def test_table_stage(self):
        """Test if data can be loaded via table stages"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Set s3_bucket and stage to None to use table stages
        self.config['s3_bucket'] = None
        self.config['stage'] = None

        # Table stages should work with CSV files
        self.config['file_format'] = os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT_CSV')
        self.persist_lines_with_cache(tap_lines)

        self.assert_three_streams_are_into_snowflake()

        # Table stages should not work with Parquet files
        self.config['file_format'] = os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT_PARQUET')
        with self.assertRaises(SystemExit):
            self.persist_lines_with_cache(tap_lines)

    def test_custom_role(self):
        """Test if custom role can be used"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Set custom role
        self.config['role'] = 'invalid-not-existing-role'

        # Using not existing or not authorized role should raise snowflake Database exception:
        # 250001 (08001): Role 'INVALID-ROLE' specified in the connect string does not exist or not authorized.
        with self.assertRaises(DatabaseError):
            self.persist_lines_with_cache(tap_lines)

    def test_parsing_date_failure(self):
        """Test if custom role can be used"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-unexpected-types.json')

        with self.assertRaises(target_snowflake.UnexpectedValueTypeException):
            self.persist_lines_with_cache(tap_lines)

    def test_parquet(self):
        """Test if parquet file can be loaded"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Set parquet file format
        self.config['file_format'] = os.environ.get('TARGET_SNOWFLAKE_FILE_FORMAT_PARQUET')
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_snowflake()

    def test_archive_load_files(self):
        """Test if load file is copied to archive folder"""
        self.config['archive_load_files'] = True
        self.config['archive_load_files_s3_prefix'] = 'archive_folder'
        self.config['tap_id'] = 'test_tap_id'
        self.config['client_side_encryption_master_key'] = ''

        s3_bucket = self.config['s3_bucket']

        # Delete any dangling files from archive
        files_in_s3_archive = self.s3_client.list_objects(
            Bucket=s3_bucket, Prefix="archive_folder/test_tap_id/").get('Contents', [])
        for file_in_archive in files_in_s3_archive:
            key = file_in_archive["Key"]
            self.s3_client.delete_object(Bucket=s3_bucket, Key=key)

        tap_lines = test_utils.get_test_tap_lines('messages-simple-table.json')
        self.persist_lines_with_cache(tap_lines)

        # Verify expected file metadata in S3
        files_in_s3_archive = self.s3_client.list_objects(Bucket=s3_bucket, Prefix="archive_folder/test_tap_id/").get(
            'Contents')
        self.assertIsNotNone(files_in_s3_archive)
        self.assertEqual(1, len(files_in_s3_archive))

        archived_file_key = files_in_s3_archive[0]['Key']
        archive_metadata = self.s3_client.head_object(Bucket=s3_bucket, Key=archived_file_key)['Metadata']
        self.assertEqual({
            'tap': 'test_tap_id',
            'schema': 'tap_mysql_test',
            'table': 'test_simple_table',
            'archived-by': 'pipelinewise_target_snowflake',
            'incremental-key': 'id',
            'incremental-key-min': '1',
            'incremental-key-max': '5'
        }, archive_metadata)

        # Verify expected file contents
        tmpfile = tempfile.NamedTemporaryFile()
        with open(tmpfile.name, 'wb') as f:
            self.s3_client.download_fileobj(s3_bucket, archived_file_key, f)

        lines = []
        with gzip.open(tmpfile, "rt") as gzipfile:
            for line in gzipfile.readlines():
                lines.append(line)

        self.assertEqual(''.join(lines), '''1,"xyz1","not-formatted-time-1"
2,"xyz2","not-formatted-time-2"
3,"xyz3","not-formatted-time-3"
4,"xyz4","not-formatted-time-4"
5,"xyz5","not-formatted-time-5"
''')

    def test_stream_with_changing_pks_should_succeed(self):
        """Test if table will have its PKs adjusted according to changes in schema key-properties"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-changing-pk.json')

        self.persist_lines_with_cache(tap_lines)

        table_desc = self.snowflake.query(f'desc table {self.config["default_target_schema"]}.test_simple_table;')
        rows_count = self.snowflake.query(f'select count(1) as _count from'
                                          f' {self.config["default_target_schema"]}.test_simple_table;')

        self.assertEqual(6, rows_count[0]['_COUNT'])

        self.assertEqual(4, len(table_desc))

        self.assertEqual('ID', table_desc[0]['name'])
        self.assertEqual('Y', table_desc[0]['null?'])
        self.assertEqual('Y', table_desc[0]['primary key'])

        self.assertEqual('RESULTS', table_desc[1]['name'])
        self.assertEqual('Y', table_desc[1]['null?'])
        self.assertEqual('N', table_desc[1]['primary key'])

        self.assertEqual('TIME_CREATED', table_desc[2]['name'])
        self.assertEqual('Y', table_desc[2]['null?'])
        self.assertEqual('N', table_desc[2]['primary key'])

        self.assertEqual('NAME', table_desc[3]['name'])
        self.assertEqual('Y', table_desc[3]['null?'])
        self.assertEqual('Y', table_desc[3]['primary key'])

    def test_stream_with_null_values_in_pks_should_fail(self):
        """Test if null values in PK column should abort the process"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-null-pk.json')

        with self.assertRaises(PrimaryKeyNotFoundException):
            self.persist_lines_with_cache(tap_lines)

    def test_stream_with_new_pks_should_succeed(self):
        """Test if table will have new PKs after not having any"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-new-pk.json')

        self.config['primary_key_required'] = False

        self.persist_lines_with_cache(tap_lines)

        table_desc = self.snowflake.query(f'desc table {self.config["default_target_schema"]}.test_simple_table;')
        rows_count = self.snowflake.query(f'select count(1) as _count from'
                                          f' {self.config["default_target_schema"]}.test_simple_table;')

        self.assertEqual(6, rows_count[0]['_COUNT'])

        self.assertEqual(4, len(table_desc))

        self.assertEqual('ID', table_desc[0]['name'])
        self.assertEqual('Y', table_desc[0]['null?'])
        self.assertEqual('Y', table_desc[0]['primary key'])

        self.assertEqual('RESULTS', table_desc[1]['name'])
        self.assertEqual('Y', table_desc[1]['null?'])
        self.assertEqual('N', table_desc[1]['primary key'])

        self.assertEqual('TIME_CREATED', table_desc[2]['name'])
        self.assertEqual('Y', table_desc[2]['null?'])
        self.assertEqual('N', table_desc[2]['primary key'])

        self.assertEqual('NAME', table_desc[3]['name'])
        self.assertEqual('Y', table_desc[3]['null?'])
        self.assertEqual('Y', table_desc[3]['primary key'])

    def test_stream_with_falsy_pks_should_succeed(self):
        """Test if data will be loaded if records have falsy values"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-falsy-pk-values.json')

        self.persist_lines_with_cache(tap_lines)

        rows_count = self.snowflake.query(f'select count(1) as _count from'
                                          f' {self.config["default_target_schema"]}.test_simple_table;')

        self.assertEqual(8, rows_count[0]['_COUNT'])
