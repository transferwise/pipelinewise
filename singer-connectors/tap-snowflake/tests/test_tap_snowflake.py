import unittest
import singer

import tap_snowflake

from singer.schema import Schema


try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils

LOGGER = singer.get_logger()

SCHEMA_NAME='tap_snowflake_test'

SINGER_MESSAGES = []

def accumulate_singer_messages(message):
    SINGER_MESSAGES.append(message)

singer.write_message = accumulate_singer_messages

class TestTypeMapping(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        snowflake_conn = test_utils.get_test_connection()

        with snowflake_conn.open_connection() as open_conn:
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
                c_time TIME,
                c_binary BINARY,
                c_varbinary VARBINARY(16)
                )'''.format(SCHEMA_NAME))

        catalog = test_utils.discover_catalog(snowflake_conn)
        cls.schema = catalog.streams[0].schema
        cls.metadata = catalog.streams[0].metadata

    def get_metadata_for_column(self, colName):
        return next(md for md in self.metadata if md['breadcrumb'] == ('properties', colName))['metadata']

    def test_decimal(self):
        self.assertEqual(self.schema.properties['C_DECIMAL'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('C_DECIMAL'),
                         {'selected-by-default': True,
                          'sql-datatype': 'number'})

    def test_decimal_with_defined_scale_and_precision(self):
        self.assertEqual(self.schema.properties['C_DECIMAL_2'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('C_DECIMAL_2'),
                         {'selected-by-default': True,
                          'sql-datatype': 'number'})

    def test_smallint(self):
        self.assertEqual(self.schema.properties['C_SMALLINT'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('C_SMALLINT'),
                         {'selected-by-default': True,
                          'sql-datatype': 'number'})

    def test_int(self):
        self.assertEqual(self.schema.properties['C_INT'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('C_INT'),
                         {'selected-by-default': True,
                          'sql-datatype': 'number'})

    def test_bigint(self):
        self.assertEqual(self.schema.properties['C_BIGINT'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('C_BIGINT'),
                         {'selected-by-default': True,
                          'sql-datatype': 'number'})

    def test_float(self):
        self.assertEqual(self.schema.properties['C_FLOAT'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('C_FLOAT'),
                         {'selected-by-default': True,
                          'sql-datatype': 'float'})

    def test_double(self):
        self.assertEqual(self.schema.properties['C_DOUBLE'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('C_DOUBLE'),
                         {'selected-by-default': True,
                          'sql-datatype': 'float'})

    def test_date(self):
        self.assertEqual(self.schema.properties['C_DATE'],
                         Schema(['null', 'string'],
                                format='date-time',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('C_DATE'),
                         {'selected-by-default': True,
                          'sql-datatype': 'date'})

    def test_time(self):
        self.assertEqual(self.schema.properties['C_TIME'],
                         Schema(['null', 'string'],
                                format='date-time',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('C_TIME'),
                         {'selected-by-default': True,
                          'sql-datatype': 'time'})

    def test_binary(self):
        self.assertEqual(self.schema.properties['C_BINARY'],
                         Schema(['null', 'string'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('C_BINARY'),
                         {'selected-by-default': True,
                          'sql-datatype': 'binary'})

    def test_varbinary(self):
        self.assertEqual(self.schema.properties['C_VARBINARY'],
                         Schema(['null', 'string'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('C_VARBINARY'),
                         {'selected-by-default': True,
                          'sql-datatype': 'binary'})


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

