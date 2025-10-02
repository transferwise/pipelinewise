import os
import unittest
import json
import singer
import snowflake.connector

import tap_snowflake
import tap_snowflake.sync_strategies.common as common

from singer.schema import Schema

try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils

LOGGER = singer.get_logger('tap_snowflake_tests')

SCHEMA_NAME = test_utils.SCHEMA_NAME

SINGER_MESSAGES = []


def accumulate_singer_messages(message):
    SINGER_MESSAGES.append(message)


singer.write_message = accumulate_singer_messages


class TestTypeMapping(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.config = test_utils.get_db_config()
        cls.snowflake_conn = test_utils.get_test_connection()

        with cls.snowflake_conn.open_connection() as open_conn:
            with open_conn.cursor() as cur:
                cur.execute('''
                CREATE TABLE {}.test_type_mapping (
                c_pk INTEGER PRIMARY KEY,
                c_decimal DECIMAL,
                c_decimal_2 DECIMAL(11, 2),
                c_smallint SMALLINT,
                c_int INT,
                c_bigint BIGINT,
                c_float FLOAT,
                c_double DOUBLE,
                c_date DATE,
                c_datetime DATETIME,
                c_time TIME,
                c_binary BINARY,
                c_varbinary VARBINARY(16)
                )'''.format(SCHEMA_NAME))

                cur.execute('''
                INSERT INTO {}.test_type_mapping
                SELECT 1
                      ,12345
                      ,123456789.12
                      ,123
                      ,12345
                      ,1234567890
                      ,123.123
                      ,123.123
                      ,'2019-08-01'
                      ,'2019-08-01 17:23:59'
                      ,'17:23:59'
                      ,HEX_ENCODE('binary')
                      ,HEX_ENCODE('varbinary')
                '''.format(SCHEMA_NAME))

                cur.execute('''
                CREATE TABLE {}.empty_table_1 (
                c_pk INTEGER PRIMARY KEY,
                c_int INT
                )'''.format(SCHEMA_NAME))

                cur.execute('''
                CREATE TABLE {}.empty_table_2 (
                c_pk INTEGER PRIMARY KEY,
                c_int INT
                )'''.format(SCHEMA_NAME))

                cur.execute('''
                CREATE VIEW {}.empty_view_1 AS
                SELECT c_pk, c_int FROM {}.empty_table_1
                '''.format(SCHEMA_NAME, SCHEMA_NAME))

        # Discover catalog object including only TEST_TYPE_MAPPING table to run detailed tests later
        cls.dt_catalog = test_utils.discover_catalog(
            cls.snowflake_conn,
            {'tables': f'{SCHEMA_NAME}.test_type_mapping'})

        cls.dt_stream = cls.dt_catalog.streams[0]
        cls.dt_schema = cls.dt_catalog.streams[0].schema
        cls.dt_metadata = cls.dt_catalog.streams[0].metadata

    def get_dt_metadata_for_column(self, col_name):
        """Helper function to get metadata entry from catalog with TEST_TYPE_MAPPING table"""
        return next(md for md in self.dt_metadata if md['breadcrumb'] == ('properties', col_name))['metadata']

    def test_discover_catalog_with_multiple_table(self):
        """Validate if discovering catalog with filter_tables option working as expected"""
        # Create config to discover three tables
        catalog = test_utils.discover_catalog(
            self.snowflake_conn,
            {'tables': f'{SCHEMA_NAME}.empty_table_1,{SCHEMA_NAME}.empty_table_2,{SCHEMA_NAME}.test_type_mapping'})

        # Three tables should be discovered
        tap_stream_ids = [s.tap_stream_id for s in catalog.streams]

        expected_tap_stream_ids = [
            f'{self.config["dbname"]}-{SCHEMA_NAME}-EMPTY_TABLE_1',
            f'{self.config["dbname"]}-{SCHEMA_NAME}-EMPTY_TABLE_2',
            f'{self.config["dbname"]}-{SCHEMA_NAME}-TEST_TYPE_MAPPING'
        ]

        self.assertCountEqual(tap_stream_ids, expected_tap_stream_ids)

    def test_discover_catalog_with_single_table(self):
        """Validate if discovering catalog with filter_tables option working as expected"""
        # Create config to discover only one table
        catalog = test_utils.discover_catalog(
            self.snowflake_conn, {'tables': f'{SCHEMA_NAME}.empty_table_2'})

        # Only one table should be discovered
        tap_stream_ids = [s.tap_stream_id for s in catalog.streams]
        expected_tap_stream_ids = [f'{self.config["dbname"]}-{SCHEMA_NAME}-EMPTY_TABLE_2']
        self.assertCountEqual(tap_stream_ids, expected_tap_stream_ids)

    def test_discover_catalog_with_not_existing_table(self):
        """Validate if discovering catalog raises as exception when table not exist"""
        # Create config to discover a not existing table
        with self.assertRaises(snowflake.connector.errors.ProgrammingError):
            test_utils.discover_catalog(
                self.snowflake_conn, {'tables': f'{SCHEMA_NAME}.empty_table_2,{SCHEMA_NAME}.not_existing_table'})

    def test_discover_catalog_with_view(self):
        """Validate if discovering catalog with filter_tables option working as expected"""
        # Create config to discover only one view
        catalog = test_utils.discover_catalog(
            self.snowflake_conn, {'tables': f'{SCHEMA_NAME}.empty_view_1'})

        # Only one view should be discovered
        tap_stream_ids = [s.tap_stream_id for s in catalog.streams]
        expected_tap_stream_ids = [f'{self.config["dbname"]}-{SCHEMA_NAME}-EMPTY_VIEW_1']
        self.assertCountEqual(tap_stream_ids, expected_tap_stream_ids)

    def test_decimal(self):
        self.assertEqual(self.dt_schema.properties['C_DECIMAL'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_dt_metadata_for_column('C_DECIMAL'),
                         {'selected-by-default': True,
                          'sql-datatype': 'number'})

    def test_decimal_with_defined_scale_and_precision(self):
        self.assertEqual(self.dt_schema.properties['C_DECIMAL_2'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_dt_metadata_for_column('C_DECIMAL_2'),
                         {'selected-by-default': True,
                          'sql-datatype': 'number'})

    def test_smallint(self):
        self.assertEqual(self.dt_schema.properties['C_SMALLINT'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_dt_metadata_for_column('C_SMALLINT'),
                         {'selected-by-default': True,
                          'sql-datatype': 'number'})

    def test_int(self):
        self.assertEqual(self.dt_schema.properties['C_INT'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_dt_metadata_for_column('C_INT'),
                         {'selected-by-default': True,
                          'sql-datatype': 'number'})

    def test_bigint(self):
        self.assertEqual(self.dt_schema.properties['C_BIGINT'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_dt_metadata_for_column('C_BIGINT'),
                         {'selected-by-default': True,
                          'sql-datatype': 'number'})

    def test_float(self):
        self.assertEqual(self.dt_schema.properties['C_FLOAT'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_dt_metadata_for_column('C_FLOAT'),
                         {'selected-by-default': True,
                          'sql-datatype': 'float'})

    def test_double(self):
        self.assertEqual(self.dt_schema.properties['C_DOUBLE'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_dt_metadata_for_column('C_DOUBLE'),
                         {'selected-by-default': True,
                          'sql-datatype': 'float'})

    def test_date(self):
        self.assertEqual(self.dt_schema.properties['C_DATE'],
                         Schema(['null', 'string'],
                                format='date-time',
                                inclusion='available'))
        self.assertEqual(self.get_dt_metadata_for_column('C_DATE'),
                         {'selected-by-default': True,
                          'sql-datatype': 'date'})

    def test_time(self):
        self.assertEqual(self.dt_schema.properties['C_TIME'],
                         Schema(['null', 'string'],
                                format='time',
                                inclusion='available'))
        self.assertEqual(self.get_dt_metadata_for_column('C_TIME'),
                         {'selected-by-default': True,
                          'sql-datatype': 'time'})

    def test_binary(self):
        self.assertEqual(self.dt_schema.properties['C_BINARY'],
                         Schema(['null', 'string'],
                                format='binary',
                                inclusion='available'))
        self.assertEqual(self.get_dt_metadata_for_column('C_BINARY'),
                         {'selected-by-default': True,
                          'sql-datatype': 'binary'})

    def test_varbinary(self):
        self.assertEqual(self.dt_schema.properties['C_VARBINARY'],
                         Schema(['null', 'string'],
                                format='binary',
                                inclusion='available'))
        self.assertEqual(self.get_dt_metadata_for_column('C_VARBINARY'),
                         {'selected-by-default': True,
                          'sql-datatype': 'binary'})

    def test_row_to_singer_record(self):
        """Select every supported data type from snowflake,
        generate the singer JSON output message and compare to expected JSON"""
        catalog_entry = self.dt_stream
        columns = list(catalog_entry.schema.properties.keys())
        select_sql = common.generate_select_sql(catalog_entry, columns)

        # Run query to export data
        with self.snowflake_conn.open_connection() as open_conn:
            with open_conn.cursor() as cur:
                cur.execute(select_sql, params={})
                row = cur.fetchone()

                # Convert the exported data to singer JSON
                record_message = common.row_to_singer_record(catalog_entry=catalog_entry,
                                                             version=1,
                                                             row=row,
                                                             columns=columns,
                                                             time_extracted=singer.utils.now())

                # Convert to formatted JSON
                formatted_record = singer.messages.format_message(record_message)

                # Reload the generated JSON to object and assert keys
                self.assertEqual(json.loads(formatted_record)['type'], 'RECORD')
                self.assertEqual(json.loads(formatted_record)['stream'], 'TEST_TYPE_MAPPING')
                self.assertEqual(json.loads(formatted_record)['record'],
                                  {
                                      'C_PK': 1,
                                      'C_DECIMAL': 12345,
                                      'C_DECIMAL_2': 123456789.12,
                                      'C_SMALLINT': 123,
                                      'C_INT': 12345,
                                      'C_BIGINT': 1234567890,
                                      'C_FLOAT': 123.123,
                                      'C_DOUBLE': 123.123,
                                      'C_DATE': '2019-08-01T00:00:00+00:00',
                                      'C_DATETIME': '2019-08-01T17:23:59+00:00',
                                      'C_TIME': '17:23:59',
                                      'C_BINARY': '62696E617279',
                                      'C_VARBINARY': '76617262696E617279'
                                  })


class TestSelectsAppropriateColumns(unittest.TestCase):

    def runTest(self):
        selected_cols = set(['a', 'b', 'd'])
        table_schema = Schema(type='object',
                              properties={
                                  'a': Schema(None, inclusion='available'),
                                  'b': Schema(None, inclusion='unsupported'),
                                  'c': Schema(None, inclusion='automatic')})

        got_cols = tap_snowflake.desired_columns(selected_cols, table_schema)

        self.assertEqual(got_cols,
                         set(['a', 'c']),
                         'Keep automatic as well as selected, available columns.')
