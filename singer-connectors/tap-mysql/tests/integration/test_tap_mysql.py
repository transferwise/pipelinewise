import os
import unittest
from unittest.mock import patch

import pymysql
import singer
import singer.metadata

import tap_mysql
import tap_mysql.discover_utils
from tap_mysql.connection import connect_with_backoff, MySQLConnection, fetch_server_id, MYSQL_ENGINE, MARIADB_ENGINE

try:
    import tests.integration.utils as test_utils
except ImportError:
    import utils as test_utils

import tap_mysql.sync_strategies.binlog as binlog
import tap_mysql.sync_strategies.common as common

from singer.schema import Schema

SINGER_MESSAGES = []


def accumulate_singer_messages(message):
    SINGER_MESSAGES.append(message)


singer.write_message = accumulate_singer_messages


class TestTypeMapping(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None

    @classmethod
    def setUpClass(cls):
        conn = test_utils.get_test_connection()

        with connect_with_backoff(conn) as open_conn:
            with open_conn.cursor() as cur:
                cur.execute('''
                CREATE TABLE test_type_mapping (
                c_pk INTEGER PRIMARY KEY,
                c_decimal DECIMAL,
                c_decimal_2_unsigned DECIMAL(5, 2) UNSIGNED,
                c_decimal_2 DECIMAL(11, 2),
                c_tinyint TINYINT,
                c_tinyint_1 TINYINT(1),
                c_tinyint_1_unsigned TINYINT(1) UNSIGNED,
                c_smallint SMALLINT,
                c_tinytext TINYTEXT,
                c_mediumint MEDIUMINT,
                c_int INT,
                c_bigint BIGINT,
                c_bigint_unsigned BIGINT(20) UNSIGNED,
                c_float FLOAT,
                c_double DOUBLE,
                c_bit BIT(4),
                c_date DATE,
                c_time TIME,
                c_year YEAR,
                c_geometry GEOMETRY,
                c_point POINT,
                c_linestring LINESTRING,
                c_polygon POLYGON,
                c_multipoint MULTIPOINT,
                c_multilinestring MULTILINESTRING,
                c_multipolygon MULTIPOLYGON,
                c_geometrycollection GEOMETRYCOLLECTION,
                c_blob BLOB
                )''')

        catalog = test_utils.discover_catalog(conn, {})
        cls.schema = catalog.streams[0].schema
        cls.metadata = catalog.streams[0].metadata

    def get_metadata_for_column(self, colName):
        return next(md for md in self.metadata if md['breadcrumb'] == ('properties', colName))['metadata']

    def test_decimal(self):
        self.assertEqual(self.schema.properties['c_decimal'],
                         Schema(['null', 'number'],
                                inclusion='available',
                                multipleOf=1))
        self.assertEqual(self.get_metadata_for_column('c_decimal'),
                         {'selected-by-default': True,
                          'sql-datatype': 'decimal(10,0)',
                          'datatype': 'decimal'})

    def test_decimal_unsigned(self):
        self.assertEqual(self.schema.properties['c_decimal_2_unsigned'],
                         Schema(['null', 'number'],
                                inclusion='available',
                                multipleOf=0.01))
        self.assertEqual(self.get_metadata_for_column('c_decimal_2_unsigned'),
                         {'selected-by-default': True,
                          'sql-datatype': 'decimal(5,2) unsigned',
                          'datatype': 'decimal'})

    def test_decimal_with_defined_scale_and_precision(self):
        self.assertEqual(self.schema.properties['c_decimal_2'],
                         Schema(['null', 'number'],
                                inclusion='available',
                                multipleOf=0.01))
        self.assertEqual(self.get_metadata_for_column('c_decimal_2'),
                         {'selected-by-default': True,
                          'sql-datatype': 'decimal(11,2)',
                          'datatype': 'decimal'})

    def test_tinyint(self):
        self.assertEqual(self.schema.properties['c_tinyint'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=-128,
                                maximum=127))
        self.assertEqual(self.get_metadata_for_column('c_tinyint'),
                         {'selected-by-default': True,
                          'sql-datatype': 'tinyint(4)',
                          'datatype': 'tinyint'})

    def test_tinyint_1(self):
        self.assertEqual(self.schema.properties['c_tinyint_1'],
                         Schema(['null', 'boolean'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_tinyint_1'),
                         {'selected-by-default': True,
                          'sql-datatype': 'tinyint(1)',
                          'datatype': 'tinyint'})

    def test_tinyint_1_unsigned(self):
        self.assertEqual(self.schema.properties['c_tinyint_1_unsigned'],
                         Schema(['null', 'boolean'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_tinyint_1_unsigned'),
                         {'selected-by-default': True,
                          'sql-datatype': 'tinyint(1) unsigned',
                          'datatype': 'tinyint'})

    def test_smallint(self):
        self.assertEqual(self.schema.properties['c_smallint'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=-32768,
                                maximum=32767))
        self.assertEqual(self.get_metadata_for_column('c_smallint'),
                         {'selected-by-default': True,
                          'sql-datatype': 'smallint(6)',
                          'datatype': 'smallint'})

    def test_mediumint(self):
        self.assertEqual(self.schema.properties['c_mediumint'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=-8388608,
                                maximum=8388607))
        self.assertEqual(self.get_metadata_for_column('c_mediumint'),
                         {'selected-by-default': True,
                          'sql-datatype': 'mediumint(9)',
                          'datatype': 'mediumint'})

    def test_int(self):
        self.assertEqual(self.schema.properties['c_int'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=-2147483648,
                                maximum=2147483647))
        self.assertEqual(self.get_metadata_for_column('c_int'),
                         {'selected-by-default': True,
                          'sql-datatype': 'int(11)',
                          'datatype': 'int'})

    def test_bigint(self):
        self.assertEqual(self.schema.properties['c_bigint'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=-9223372036854775808,
                                maximum=9223372036854775807))
        self.assertEqual(self.get_metadata_for_column('c_bigint'),
                         {'selected-by-default': True,
                          'sql-datatype': 'bigint(20)',
                          'datatype': 'bigint'})

    def test_bigint_unsigned(self):
        self.assertEqual(self.schema.properties['c_bigint_unsigned'],
                         Schema(['null', 'integer'],
                                inclusion='available',
                                minimum=0,
                                maximum=18446744073709551615))

        self.assertEqual(self.get_metadata_for_column('c_bigint_unsigned'),
                         {'selected-by-default': True,
                          'sql-datatype': 'bigint(20) unsigned',
                          'datatype': 'bigint'})

    def test_float(self):
        self.assertEqual(self.schema.properties['c_float'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_float'),
                         {'selected-by-default': True,
                          'sql-datatype': 'float',
                          'datatype': 'float'})

    def test_double(self):
        self.assertEqual(self.schema.properties['c_double'],
                         Schema(['null', 'number'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_double'),
                         {'selected-by-default': True,
                          'sql-datatype': 'double',
                          'datatype': 'double'})

    def test_bit(self):
        self.assertEqual(self.schema.properties['c_bit'],
                         Schema(['null', 'boolean'],
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_bit'),
                         {'selected-by-default': True,
                          'sql-datatype': 'bit(4)',
                          'datatype': 'bit'})

    def test_date(self):
        self.assertEqual(self.schema.properties['c_date'],
                         Schema(['null', 'string'],
                                format='date-time',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_date'),
                         {'selected-by-default': True,
                          'sql-datatype': 'date',
                          'datatype': 'date'})

    def test_time(self):
        self.assertEqual(self.schema.properties['c_time'],
                         Schema(['null', 'string'],
                                format='time',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_time'),
                         {'selected-by-default': True,
                          'sql-datatype': 'time',
                          'datatype': 'time'})

    def test_year(self):
        self.assertEqual(self.schema.properties['c_year'].inclusion,
                         'unsupported')
        self.assertEqual(self.get_metadata_for_column('c_year'),
                         {'selected-by-default': False,
                          'sql-datatype': 'year(4)',
                          'datatype': 'year'})

    def test_pk(self):
        self.assertEqual(
            self.schema.properties['c_pk'].inclusion,
            'automatic')

    def test_geometry(self):
        self.assertEqual(self.schema.properties['c_geometry'],
                         Schema(['null', 'object'],
                                format='spatial',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_geometry'),
                         {'selected-by-default': True,
                          'sql-datatype': 'geometry',
                          'datatype': 'geometry'})

    def test_point(self):
        self.assertEqual(self.schema.properties['c_point'],
                         Schema(['null', 'object'],
                                format='spatial',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_point'),
                         {'selected-by-default': True,
                          'sql-datatype': 'point',
                          'datatype': 'point'})

    def test_linestring(self):
        self.assertEqual(self.schema.properties['c_linestring'],
                         Schema(['null', 'object'],
                                format='spatial',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_linestring'),
                         {'selected-by-default': True,
                          'sql-datatype': 'linestring',
                          'datatype': 'linestring'})

    def test_polygon(self):
        self.assertEqual(self.schema.properties['c_polygon'],
                         Schema(['null', 'object'],
                                format='spatial',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_polygon'),
                         {'selected-by-default': True,
                          'sql-datatype': 'polygon',
                          'datatype': 'polygon'})

    def test_multipoint(self):
        self.assertEqual(self.schema.properties['c_multipoint'],
                         Schema(['null', 'object'],
                                format='spatial',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_multipoint'),
                         {'selected-by-default': True,
                          'sql-datatype': 'multipoint',
                          'datatype': 'multipoint'})

    def test_multilinestring(self):
        self.assertEqual(self.schema.properties['c_multilinestring'],
                         Schema(['null', 'object'],
                                format='spatial',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_multilinestring'),
                         {'selected-by-default': True,
                          'sql-datatype': 'multilinestring',
                          'datatype': 'multilinestring'})

    def test_multipolygon(self):
        self.assertEqual(self.schema.properties['c_multipolygon'],
                         Schema(['null', 'object'],
                                format='spatial',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_multipolygon'),
                         {'selected-by-default': True,
                          'sql-datatype': 'multipolygon',
                          'datatype': 'multipolygon'})

    def test_geometrycollection(self):
        self.assertEqual(self.schema.properties['c_geometrycollection'],
                         Schema(['null', 'object'],
                                format='spatial',
                                inclusion='available'))
        self.assertEqual(self.get_metadata_for_column('c_geometrycollection'),
                         {'selected-by-default': True,
                          'sql-datatype': 'geometrycollection',
                          'datatype': 'geometrycollection'})


class TestSelectsAppropriateColumns(unittest.TestCase):

    def runTest(self):
        selected_cols = set(['a', 'b', 'd'])
        table_schema = Schema(type='object',
                              properties={
                                  'a': Schema(None, inclusion='available'),
                                  'b': Schema(None, inclusion='unsupported'),
                                  'c': Schema(None, inclusion='automatic')})

        got_cols = tap_mysql.discover_utils.desired_columns(selected_cols, table_schema)

        self.assertEqual(got_cols,
                         {'a', 'c'},
                         'Keep automatic as well as selected, available columns.')


class TestSchemaMessages(unittest.TestCase):

    def runTest(self):
        conn = test_utils.get_test_connection()

        with connect_with_backoff(conn) as open_conn:
            with open_conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE tab (
                      id INTEGER PRIMARY KEY,
                      a INTEGER,
                      b INTEGER)
                ''')

        catalog = test_utils.discover_catalog(conn, {})
        catalog.streams[0].stream = 'tab'
        catalog.streams[0].metadata = [
            {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'tap_mysql_test'}},
            {'breadcrumb': ('properties', 'a'), 'metadata': {'selected': True}}
        ]

        test_utils.set_replication_method_and_key(catalog.streams[0], 'FULL_TABLE', None)

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(conn, {}, catalog, {})

        schema_message = list(filter(lambda m: isinstance(m, singer.SchemaMessage), SINGER_MESSAGES))[0]
        self.assertTrue(isinstance(schema_message, singer.SchemaMessage))
        # tap-mysql selects new fields by default. If a field doesn't appear in the schema, then it should be
        # selected
        expectedKeys = ['id', 'a', 'b']

        self.assertEqual(schema_message.schema['properties'].keys(), set(expectedKeys))


def currently_syncing_seq(messages):
    return ''.join(
        [(m.value.get('currently_syncing', '_') or '_')[-1]
         for m in messages
         if isinstance(m, singer.StateMessage)]
    )


class TestCurrentStream(unittest.TestCase):

    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE a (val int)')
                cursor.execute('CREATE TABLE b (val int)')
                cursor.execute('CREATE TABLE c (val int)')
                cursor.execute('INSERT INTO a (val) VALUES (1)')
                cursor.execute('INSERT INTO b (val) VALUES (1)')
                cursor.execute('INSERT INTO c (val) VALUES (1)')

        self.catalog = test_utils.discover_catalog(self.conn, {})

        for stream in self.catalog.streams:
            stream.key_properties = []

            stream.metadata = [
                {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'tap_mysql_test'}},
                {'breadcrumb': ('properties', 'val'), 'metadata': {'selected': True}}
            ]

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, 'FULL_TABLE', None)

    def test_emit_currently_syncing(self):
        state = {}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        tap_mysql.do_sync(self.conn, {}, self.catalog, state)
        self.assertRegex(currently_syncing_seq(SINGER_MESSAGES), '^a+b+c+_+')

    def test_start_at_currently_syncing(self):
        state = {
            'currently_syncing': 'tap_mysql_test-b',
            'bookmarks': {
                'tap_mysql_test-a': {
                    'version': 123
                },
                'tap_mysql_test-b': {
                    'version': 456
                }
            }
        }

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        self.assertRegex(currently_syncing_seq(SINGER_MESSAGES), '^b+c+a+_+')


def message_types_and_versions(messages):
    message_types = []
    versions = []
    for message in messages:
        t = type(message)
        if t in {singer.RecordMessage, singer.ActivateVersionMessage}:
            message_types.append(t.__name__)
            versions.append(message.version)
    return message_types, versions


class TestStreamVersionFullTable(unittest.TestCase):

    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE full_table (val int)')
                cursor.execute('INSERT INTO full_table (val) VALUES (1)')

        self.catalog = test_utils.discover_catalog(self.conn, {})
        for stream in self.catalog.streams:
            stream.key_properties = []

            stream.metadata = [
                {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'tap_mysql_test'}},
                {'breadcrumb': ('properties', 'val'), 'metadata': {'selected': True}}
            ]

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, 'FULL_TABLE', None)

    def test_with_no_state(self):
        state = {}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        self.assertEqual(['ActivateVersionMessage', 'RecordMessage', 'ActivateVersionMessage'], message_types)
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])

    def test_with_no_initial_full_table_complete_in_state(self):
        common.get_stream_version = lambda a, b: 12345

        state = {
            'bookmarks': {
                'tap_mysql_test-full_table': {
                    'version': None
                }
            }
        }

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        self.assertEqual(['RecordMessage', 'ActivateVersionMessage'], message_types)
        self.assertEqual(versions, [12345, 12345])

        self.assertFalse('version' in state['bookmarks']['tap_mysql_test-full_table'].keys())
        self.assertTrue(state['bookmarks']['tap_mysql_test-full_table']['initial_full_table_complete'])

    def test_with_initial_full_table_complete_in_state(self):
        common.get_stream_version = lambda a, b: 12345

        state = {
            'bookmarks': {
                'tap_mysql_test-full_table': {
                    'initial_full_table_complete': True
                }
            }
        }

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        self.assertEqual(['RecordMessage', 'ActivateVersionMessage'], message_types)
        self.assertEqual(versions, [12345, 12345])

    def test_version_cleared_from_state_after_full_table_success(self):
        common.get_stream_version = lambda a, b: 12345

        state = {
            'bookmarks': {
                'tap_mysql_test-full_table': {
                    'version': 1,
                    'initial_full_table_complete': True
                }
            }
        }

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        self.assertEqual(['RecordMessage', 'ActivateVersionMessage'], message_types)
        self.assertEqual(versions, [12345, 12345])

        self.assertFalse('version' in state['bookmarks']['tap_mysql_test-full_table'].keys())
        self.assertTrue(state['bookmarks']['tap_mysql_test-full_table']['initial_full_table_complete'])


class TestIncrementalReplication(unittest.TestCase):

    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE incremental (val int, updated datetime, ctime time)')
                cursor.execute('INSERT INTO incremental (val, updated, ctime) VALUES (1, \'2017-06-01\', '
                               'current_time())')
                cursor.execute('INSERT INTO incremental (val, updated, ctime) VALUES (2, \'2017-06-20\', '
                               'current_time())')
                cursor.execute('INSERT INTO incremental (val, updated, ctime) VALUES (3, \'2017-09-22\', '
                               'current_time())')
                cursor.execute('CREATE TABLE integer_incremental (val int, updated int)')
                cursor.execute('INSERT INTO integer_incremental (val, updated) VALUES (1, 1)')
                cursor.execute('INSERT INTO integer_incremental (val, updated) VALUES (2, 2)')
                cursor.execute('INSERT INTO integer_incremental (val, updated) VALUES (3, 3)')

        self.catalog = test_utils.discover_catalog(self.conn, {})

        for stream in self.catalog.streams:
            stream.metadata = [
                {'breadcrumb': (),
                 'metadata': {
                     'selected': True,
                     'table-key-properties': [],
                     'database-name': 'tap_mysql_test'
                 }},
                {'breadcrumb': ('properties', 'val'), 'metadata': {'selected': True}}
            ]

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, 'INCREMENTAL', 'updated')

    def test_with_no_state(self):
        state = {}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        self.assertEqual(
            ['ActivateVersionMessage',
             'RecordMessage',
             'RecordMessage',
             'RecordMessage',
             'ActivateVersionMessage',
             'RecordMessage',
             'RecordMessage',
             'RecordMessage'],
            message_types)
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])

    def test_with_state(self):
        state = {
            'bookmarks': {
                'tap_mysql_test-incremental': {
                    'version': 1,
                    'replication_key_value': '2017-06-20',
                    'replication_key': 'updated'
                },
                'tap_mysql_test-integer_incremental': {
                    'version': 1,
                    'replication_key_value': 3,
                    'replication_key': 'updated'
                }
            }
        }

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        self.assertEqual(
            ['ActivateVersionMessage',
             'RecordMessage',
             'RecordMessage',
             'ActivateVersionMessage',
             'RecordMessage'],
            message_types)
        self.assertTrue(isinstance(versions[0], int))
        self.assertEqual(versions[0], versions[1])
        self.assertEqual(versions[1], 1)

    def test_change_replication_key(self):
        state = {
            'bookmarks': {
                'tap_mysql_test-incremental': {
                    'version': 1,
                    'replication_key_value': '2017-06-20',
                    'replication_key': 'updated'
                }
            }
        }

        stream = [x for x in self.catalog.streams if x.stream == 'incremental'][0]

        stream.metadata = [
            {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'tap_mysql_test'}},
            {'breadcrumb': ('properties', 'val'), 'metadata': {'selected': True}},
            {'breadcrumb': ('properties', 'updated'), 'metadata': {'selected': True}}
        ]

        test_utils.set_replication_method_and_key(stream, 'INCREMENTAL', 'val')

        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        self.assertEqual(state['bookmarks']['tap_mysql_test-incremental']['replication_key'], 'val')
        self.assertEqual(state['bookmarks']['tap_mysql_test-incremental']['replication_key_value'], 3)
        self.assertEqual(state['bookmarks']['tap_mysql_test-incremental']['version'], 1)

    def test_version_not_cleared_from_state_after_incremental_success(self):
        state = {
            'bookmarks': {
                'tap_mysql_test-incremental': {
                    'version': 1,
                    'replication_key_value': '2017-06-20',
                    'replication_key': 'updated'
                }
            }
        }

        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        self.assertEqual(state['bookmarks']['tap_mysql_test-incremental']['version'], 1)


class TestBinlogReplication(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.state = {}
        self.conn = test_utils.get_test_connection()

        log_file, log_pos = binlog.fetch_current_log_file_and_pos(self.conn)

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE binlog_1 (id int, updated datetime, '
                               'created_date Date)')
                cursor.execute("""
                    CREATE TABLE binlog_2 (id int, 
                    updated datetime, 
                    is_good bool default False, 
                    ctime time, 
                    cjson json)
                """)
                cursor.execute(
                    'INSERT INTO binlog_1 (id, updated, created_date) VALUES (1, \'2017-06-01\', current_date())')
                cursor.execute(
                    'INSERT INTO binlog_1 (id, updated, created_date) VALUES (2, \'2017-06-20\', current_date())')
                cursor.execute(
                    'INSERT INTO binlog_1 (id, updated, created_date) VALUES (3, \'2017-09-22\', current_date())')
                cursor.execute('INSERT INTO binlog_2 (id, updated, ctime, cjson) VALUES (1, \'2017-10-22\', '
                               'current_time(), \'[{"key1": "A", "key2": ["B", 2], "key3": {}}]\')')
                cursor.execute('INSERT INTO binlog_2 (id, updated, ctime, cjson) VALUES (2, \'2017-11-10\', '
                               'current_time(), \'[{"key1": "A", "key2": ["B", 2], "key3": {}}]\')')
                cursor.execute('INSERT INTO binlog_2 (id, updated, ctime, cjson) VALUES (3, \'2017-12-10\', '
                               'current_time(), \'[{"key1": "A", "key2": ["B", 2], "key3": {}}]\')')
                cursor.execute('UPDATE binlog_1 set updated=\'2018-06-18\' WHERE id = 3')
                cursor.execute('UPDATE binlog_2 set updated=\'2018-06-18\' WHERE id = 2')
                cursor.execute('DELETE FROM binlog_1 WHERE id = 2')
                cursor.execute('DELETE FROM binlog_2 WHERE id = 1')

            open_conn.commit()

        self.catalog = test_utils.discover_catalog(self.conn, {})

        for stream in self.catalog.streams:
            stream.stream = stream.table

            stream.metadata = [
                {'breadcrumb': (),
                 'metadata': {
                     'selected': True,
                     'database-name': 'tap_mysql_test',
                     'table-key-properties': ['id']
                 }},
                {'breadcrumb': ('properties', 'id'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'updated'), 'metadata': {'selected': True}}
            ]

            test_utils.set_replication_method_and_key(stream, 'LOG_BASED', None)

            self.state = singer.write_bookmark(self.state,
                                               stream.tap_stream_id,
                                               'log_file',
                                               log_file)

            self.state = singer.write_bookmark(self.state,
                                               stream.tap_stream_id,
                                               'log_pos',
                                               log_pos)

            self.state = singer.write_bookmark(self.state,
                                               stream.tap_stream_id,
                                               'version',
                                               singer.utils.now())

    def tearDown(self) -> None:
        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

    def test_initial_full_table(self):
        state = {}

        global SINGER_MESSAGES

        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        message_types = [type(m) for m in SINGER_MESSAGES]

        self.assertEqual(message_types,
                         [singer.StateMessage,
                          singer.SchemaMessage,
                          singer.ActivateVersionMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.StateMessage,
                          singer.ActivateVersionMessage,
                          singer.StateMessage,
                          singer.SchemaMessage,
                          singer.ActivateVersionMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.StateMessage,
                          singer.ActivateVersionMessage,
                          singer.StateMessage])

        activate_version_message_1 = list(filter(
            lambda m: isinstance(m, singer.ActivateVersionMessage) and m.stream == 'tap_mysql_test-binlog_1',
            SINGER_MESSAGES))[0]

        activate_version_message_2 = list(filter(
            lambda m: isinstance(m, singer.ActivateVersionMessage) and m.stream == 'tap_mysql_test-binlog_2',
            SINGER_MESSAGES))[0]

        self.assertIsNotNone(singer.get_bookmark(state, 'tap_mysql_test-binlog_1', 'log_file'))
        self.assertIsNotNone(singer.get_bookmark(state, 'tap_mysql_test-binlog_1', 'log_pos'))
        self.assertIsNone(singer.get_bookmark(state, 'tap_mysql_test-binlog_1', 'gtid'))

        self.assertIsNotNone(singer.get_bookmark(state, 'tap_mysql_test-binlog_2', 'log_file'))
        self.assertIsNotNone(singer.get_bookmark(state, 'tap_mysql_test-binlog_2', 'log_pos'))
        self.assertIsNone(singer.get_bookmark(state, 'tap_mysql_test-binlog_2', 'gtid'))

        self.assertEqual(singer.get_bookmark(state, 'tap_mysql_test-binlog_1', 'version'),
                         activate_version_message_1.version)

        self.assertEqual(singer.get_bookmark(state, 'tap_mysql_test-binlog_2', 'version'),
                         activate_version_message_2.version)

    def test_fail_on_view(self):
        for stream in self.catalog.streams:
            md = singer.metadata.to_map(stream.metadata)
            singer.metadata.write(md, (), 'is-view', True)

        state = {}

        expected_exception_message = "Unable to replicate stream(tap_mysql_test-{}) with binlog because it is a view.". \
            format(self.catalog.streams[0].stream)

        with self.assertRaises(Exception) as context:
            tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        self.assertEqual(expected_exception_message, str(context.exception))

    def test_fail_if_log_file_does_not_exist(self):
        log_file = 'chicken'
        stream = self.catalog.streams[0]
        state = {
            'bookmarks': {
                stream.tap_stream_id: {
                    'version': singer.utils.now(),
                    'log_file': log_file,
                    'log_pos': 1
                }
            }
        }

        expected_exception_message = "Unable to replicate binlog stream because the following binary log(s) no " \
                                     "longer exist: {}".format(log_file)

        with self.assertRaises(Exception) as context:
            tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        self.assertEqual(expected_exception_message, str(context.exception))

    def test_binlog_stream(self):
        global SINGER_MESSAGES

        config = test_utils.get_db_config()
        config['server_id'] = "100"

        tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        schema_messages = list(filter(lambda m: isinstance(m, singer.SchemaMessage), SINGER_MESSAGES))

        for schema_msg in schema_messages:
            for prop, val in schema_msg.schema['properties'].items():
                self.assertIn('type', val, f'property "{prop}" has no "type" in stream "{schema_msg.stream}"')

        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        message_types = [type(m) for m in SINGER_MESSAGES]
        self.assertEqual(message_types,
                         [singer.StateMessage,
                          singer.SchemaMessage,
                          singer.SchemaMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.StateMessage])

        self.assertEqual([('tap_mysql_test-binlog_1', 1, '2017-06-01T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_1', 2, '2017-06-20T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_1', 3, '2017-09-22T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_2', 1, '2017-10-22T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_2', 2, '2017-11-10T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_2', 3, '2017-12-10T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_1', 3, '2018-06-18T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_2', 2, '2018-06-18T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_1', 2, '2017-06-20T00:00:00+00:00', True),
                          ('tap_mysql_test-binlog_2', 1, '2017-10-22T00:00:00+00:00', True)],
                         [(m.stream,
                           m.record['id'],
                           m.record['updated'],
                           m.record.get(binlog.SDC_DELETED_AT) is not None)
                          for m in record_messages])

        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_file'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_pos'))

        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_file'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_pos'))

    def test_binlog_stream_with_alteration(self):
        global SINGER_MESSAGES

        config = test_utils.get_db_config()
        config['server_id'] = "100"

        tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('ALTER TABLE binlog_1 add column data blob;')
                cursor.execute('ALTER TABLE binlog_1 add column is_cancelled boolean;')
                cursor.execute(
                    'INSERT INTO binlog_1 (id, updated, is_cancelled, data) VALUES (2, \'2017-06-20\', true, \'blob content\')')
                cursor.execute('INSERT INTO binlog_1 (id, updated, is_cancelled) VALUES (3, \'2017-09-21\', false)')
                cursor.execute('INSERT INTO binlog_2 (id, updated) VALUES (3, \'2017-12-10\')')
                cursor.execute('ALTER TABLE binlog_1 change column updated date_updated datetime;')
                cursor.execute('UPDATE binlog_1 set date_updated=\'2018-06-18\' WHERE id = 3')

            open_conn.commit()

        tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        schema_messages = list(filter(lambda m: isinstance(m, singer.SchemaMessage), SINGER_MESSAGES))

        for schema_msg in schema_messages:
            for prop, val in schema_msg.schema['properties'].items():
                self.assertIn('type', val, f'property "{prop}" has no "type" in stream "{schema_msg.stream}"')

        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        message_types = [type(m) for m in SINGER_MESSAGES]
        self.assertEqual(message_types,
                         [singer.StateMessage,
                          singer.SchemaMessage,
                          singer.SchemaMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.StateMessage,  # end of 1st sync
                          singer.StateMessage,  # start of 2nd sync
                          singer.SchemaMessage,
                          singer.SchemaMessage,
                          singer.SchemaMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.StateMessage,
                          ])

        self.assertEqual([('tap_mysql_test-binlog_1', 1, '2017-06-01T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_1', 2, '2017-06-20T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_1', 3, '2017-09-22T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_2', 1, '2017-10-22T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_2', 2, '2017-11-10T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_2', 3, '2017-12-10T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_1', 3, '2018-06-18T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_2', 2, '2018-06-18T00:00:00+00:00', False),
                          ('tap_mysql_test-binlog_1', 2, '2017-06-20T00:00:00+00:00', True),
                          ('tap_mysql_test-binlog_2', 1, '2017-10-22T00:00:00+00:00', True)],
                         [(m.stream,
                           m.record['id'],
                           m.record['updated'],
                           m.record.get(binlog.SDC_DELETED_AT) is not None)
                          for m in record_messages[:10]])

        self.assertIn('tap_mysql_test-binlog_1', SINGER_MESSAGES[15].stream)
        self.assertNotIn('date_updated', SINGER_MESSAGES[15].schema['properties'])
        self.assertNotIn('is_cancelled', SINGER_MESSAGES[15].schema['properties'])

        self.assertIn('tap_mysql_test-binlog_1', SINGER_MESSAGES[17].stream)
        self.assertIn('date_updated', SINGER_MESSAGES[17].schema['properties'])
        self.assertIn('is_cancelled', SINGER_MESSAGES[17].schema['properties'])

        self.assertIn('tap_mysql_test-binlog_1', SINGER_MESSAGES[18].stream)
        self.assertIn('date_updated', SINGER_MESSAGES[18].record)
        self.assertIn('is_cancelled', SINGER_MESSAGES[18].record)

        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_file'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_pos'))

        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_file'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_pos'))

    def test_binlog_stream_with_gtid(self):
        global SINGER_MESSAGES

        engine = os.getenv('TAP_MYSQL_ENGINE', MYSQL_ENGINE)
        gtid = binlog.fetch_current_gtid_pos(self.conn, os.environ['TAP_MYSQL_ENGINE'])

        config = test_utils.get_db_config()
        config['use_gtid'] = True
        config['engine'] = engine

        self.state = singer.write_bookmark(self.state,
                                           'tap_mysql_test-binlog_1',
                                           'gtid',
                                           gtid)

        self.state = singer.write_bookmark(self.state,
                                           'tap_mysql_test-binlog_2',
                                           'gtid',
                                           gtid)

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('INSERT INTO binlog_1 (id, updated) VALUES (4, \'2022-06-20\')')
                cursor.execute('INSERT INTO binlog_1 (id, updated) VALUES (5, \'2022-09-21\')')
                cursor.execute('INSERT INTO binlog_2 (id, updated) VALUES (4, \'2017-12-10\')')
                cursor.execute('delete from binlog_1 WHERE id = 3')

            open_conn.commit()

        tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        message_types = [type(m) for m in SINGER_MESSAGES]
        self.assertEqual(message_types,
                         [singer.StateMessage,
                          singer.SchemaMessage,
                          singer.SchemaMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.StateMessage,
                          ])

        self.assertEqual([('tap_mysql_test-binlog_1', 4, False),
                          ('tap_mysql_test-binlog_1', 5, False),
                          ('tap_mysql_test-binlog_2', 4, False),
                          ('tap_mysql_test-binlog_1', 3, True)],
                         [(m.stream,
                           m.record['id'],
                           m.record.get(binlog.SDC_DELETED_AT) is not None)
                          for m in record_messages])

        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_file'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_pos'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'gtid'))

        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_file'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_pos'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'gtid'))

    def test_binlog_stream_switching_from_binlog_to_gtid_with_mysql_fails(self):
        global SINGER_MESSAGES

        engine = os.getenv('TAP_MYSQL_ENGINE', MYSQL_ENGINE)

        if engine != MYSQL_ENGINE:
            self.skipTest('This test is only meant for Mysql flavor')

        log_file, log_pos = binlog.fetch_current_log_file_and_pos(self.conn)

        self.state = singer.write_bookmark(self.state,
                                           'tap_mysql_test-binlog_1',
                                           'log_file',
                                           log_file)

        self.state = singer.write_bookmark(self.state,
                                           'tap_mysql_test-binlog_2',
                                           'log_pos',
                                           log_pos)

        config = test_utils.get_db_config()

        config['use_gtid'] = True
        config['engine'] = engine

        with self.assertRaises(Exception) as context:
            tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        self.assertEqual("Couldn't find any gtid in state bookmarks to resume logical replication",
                         str(context.exception))

    def test_binlog_stream_switching_from_binlog_to_gtid_with_mariadb_success(self):
        global SINGER_MESSAGES

        engine = os.getenv('TAP_MYSQL_ENGINE', MYSQL_ENGINE)

        if engine != MARIADB_ENGINE:
            self.skipTest('This test is only meant for Mariadb flavor')

        config = test_utils.get_db_config()

        config['use_gtid'] = True
        config['engine'] = engine

        tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        message_types = [type(m) for m in SINGER_MESSAGES]
        self.assertEqual(message_types,
                         [singer.StateMessage,
                          singer.SchemaMessage,
                          singer.SchemaMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.RecordMessage,
                          singer.StateMessage,
                          ])

        self.assertEqual([
            ('tap_mysql_test-binlog_1', 1, False),
            ('tap_mysql_test-binlog_1', 2, False),
            ('tap_mysql_test-binlog_1', 3, False),
            ('tap_mysql_test-binlog_2', 1, False),
            ('tap_mysql_test-binlog_2', 2, False),
            ('tap_mysql_test-binlog_2', 3, False),
            ('tap_mysql_test-binlog_1', 3, False),
            ('tap_mysql_test-binlog_2', 2, False),
            ('tap_mysql_test-binlog_1', 2, True),
            ('tap_mysql_test-binlog_2', 1, True),
        ],
            [(m.stream,
              m.record['id'],
              m.record.get(binlog.SDC_DELETED_AT) is not None)
             for m in record_messages])

        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_file'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_pos'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'gtid'))

        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_file'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_pos'))
        self.assertIsNotNone(singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'gtid'))


class TestViews(unittest.TestCase):
    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute(
                    '''
                    CREATE TABLE a_table (
                      id int primary key,
                      a int,
                      b int)
                    ''')

                cursor.execute(
                    '''
                    CREATE VIEW a_view AS SELECT id, a FROM a_table
                    ''')

    def test_discovery_sets_is_view(self):
        catalog = test_utils.discover_catalog(self.conn, {})
        is_view = {}

        for stream in catalog.streams:
            md_map = singer.metadata.to_map(stream.metadata)
            is_view[stream.table] = md_map.get((), {}).get('is-view')

        self.assertEqual(
            is_view,
            {'a_table': False,
             'a_view': True})

    def test_do_not_discover_key_properties_for_view(self):
        catalog = test_utils.discover_catalog(self.conn, {})
        primary_keys = {}
        for c in catalog.streams:
            primary_keys[c.table] = singer.metadata.to_map(c.metadata).get((), {}).get('table-key-properties')

        self.assertEqual(
            primary_keys,
            {'a_table': ['id'],
             'a_view': None})


class TestEscaping(unittest.TestCase):

    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE a (`b c` int)')
                cursor.execute('INSERT INTO a (`b c`) VALUES (1)')

        self.catalog = test_utils.discover_catalog(self.conn, {})

        self.catalog.streams[0].stream = 'some_stream_name'

        self.catalog.streams[0].metadata = [
            {'breadcrumb': (),
             'metadata': {
                 'selected': True,
                 'table-key-properties': [],
                 'database-name': 'tap_mysql_test'
             }},
            {'breadcrumb': ('properties', 'b c'), 'metadata': {'selected': True}}
        ]

        test_utils.set_replication_method_and_key(self.catalog.streams[0], 'FULL_TABLE', None)

    def runTest(self):
        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, {})

        record_message = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))[0]

        self.assertTrue(isinstance(record_message, singer.RecordMessage))
        self.assertEqual(record_message.record, {'b c': 1})


class TestJsonTables(unittest.TestCase):

    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE json_table (val json)')
                cursor.execute('INSERT INTO json_table (val) VALUES ( \'{"a": 10, "b": "c"}\')')

        self.catalog = test_utils.discover_catalog(self.conn, {})
        for stream in self.catalog.streams:
            stream.key_properties = []

            stream.metadata = [
                {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'tap_mysql_test'}},
                {'breadcrumb': ('properties', 'val'), 'metadata': {'selected': True}}
            ]

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, 'FULL_TABLE', None)

    def runTest(self):
        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, {})

        record_message = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))[0]
        self.assertTrue(isinstance(record_message, singer.RecordMessage))
        self.assertEqual(record_message.record, {'val': '{"a": 10, "b": "c"}'})


class TestSupportedPK(unittest.TestCase):

    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute(
                    # BINARY is presently supported
                    'CREATE TABLE good_pk_tab (good_pk BINARY(10), age INT, PRIMARY KEY (good_pk))')

                cursor.execute("INSERT INTO good_pk_tab (good_pk, age) VALUES "
                               "(BINARY('a'), 20), "
                               "(BINARY('b'), 30), "
                               "(BINARY('c'), 30), "
                               "(BINARY('d'), 40)")

        self.catalog = test_utils.discover_catalog(self.conn, {})

    def test_primary_key_is_in_metadata(self):
        primary_keys = {}
        for c in self.catalog.streams:
            primary_keys[c.table] = singer.metadata.to_map(c.metadata).get((), {}).get('table-key-properties')

        self.assertEqual(primary_keys, {'good_pk_tab': ['good_pk']})

    def test_sync_messages_are_correct(self):
        self.catalog.streams[0] = test_utils.set_replication_method_and_key(self.catalog.streams[0], 'LOG_BASED', None)
        self.catalog.streams[0] = test_utils.set_selected(self.catalog.streams[0], True)

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        # inital sync
        tap_mysql.do_sync(self.conn, {}, self.catalog, {})

        # get schema message to test that it has all the table's columns
        schema_message = next(filter(lambda m: isinstance(m, singer.SchemaMessage), SINGER_MESSAGES))
        expectedKeys = ['good_pk', 'age']

        self.assertEqual(schema_message.schema['properties'].keys(), set(expectedKeys))

        # get the records, these are generated by Full table replication
        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        self.assertEqual(len(record_messages), 4)
        self.assertListEqual([
            {'age': 20, 'good_pk': '61000000000000000000'},
            {'age': 30, 'good_pk': '62000000000000000000'},
            {'age': 30, 'good_pk': '63000000000000000000'},
            {'age': 40, 'good_pk': '64000000000000000000'},
        ], [rec.record for rec in record_messages])

        # get the last state message to be fed to the next sync
        state_message = list(filter(lambda m: isinstance(m, singer.StateMessage), SINGER_MESSAGES))[-1]

        SINGER_MESSAGES.clear()

        # run some queries
        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute("UPDATE good_pk_tab set age=age+5")
                cursor.execute("INSERT INTO good_pk_tab (good_pk, age) VALUES "
                               "(BINARY('e'), 16), "
                               "(BINARY('f'), 5)")

        # do a sync and give the state so that binlog replication start from the last synced position
        tap_mysql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state_message.value)

        # get the changed/new records
        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        self.assertEqual(len(record_messages), 6)
        self.assertListEqual([
            {'age': 25, 'good_pk': '61000000000000000000'},
            {'age': 35, 'good_pk': '62000000000000000000'},
            {'age': 35, 'good_pk': '63000000000000000000'},
            {'age': 45, 'good_pk': '64000000000000000000'},
            {'age': 16, 'good_pk': '65000000000000000000'},
            {'age': 5, 'good_pk': '66000000000000000000'},
        ], [rec.record for rec in record_messages])

    def tearDown(self) -> None:
        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('DROP TABLE good_pk_tab;')


class MySQLConnectionMock(MySQLConnection):
    """
    Mocked MySQLConnection class
    """

    def __init__(self, config):
        super().__init__(config)

        self.executed_queries = []

    def run_sql(self, sql):
        self.executed_queries.append(sql)


class TestSessionSqls(unittest.TestCase):

    def setUp(self) -> None:
        self.executed_queries = []

    def run_sql_mock(self, connection, sql):
        if sql.startswith('INVALID-SQL'):
            raise pymysql.err.InternalError

        self.executed_queries.append(sql)

    def test_open_connections_with_default_session_sqls(self):
        """Default session parameters should be applied if no custom session SQLs"""
        with patch('tap_mysql.connection.MySQLConnection.connect'):
            with patch('tap_mysql.connection.run_sql') as run_sql_mock:
                run_sql_mock.side_effect = self.run_sql_mock
                conn = MySQLConnectionMock(config=test_utils.get_db_config())
                connect_with_backoff(conn)

        # Test if session variables applied on connection
        self.assertEqual(self.executed_queries, tap_mysql.connection.DEFAULT_SESSION_SQLS)

    def test_open_connections_with_session_sqls(self):
        """Custom session parameters should be applied if defined"""
        session_sqls = [
            'SET SESSION max_statement_time=0',
            'SET SESSION wait_timeout=28800'
        ]

        with patch('tap_mysql.connection.MySQLConnection.connect'):
            with patch('tap_mysql.connection.run_sql') as run_sql_mock:
                run_sql_mock.side_effect = self.run_sql_mock
                conn = MySQLConnectionMock(config={**test_utils.get_db_config(),
                                                   **{'session_sqls': session_sqls}})
                connect_with_backoff(conn)

        # Test if session variables applied on connection
        self.assertEqual(self.executed_queries, session_sqls)

    def test_open_connections_with_invalid_session_sqls(self):
        """Invalid SQLs in session_sqls should be ignored"""
        session_sqls = [
            'SET SESSION max_statement_time=0',
            'INVALID-SQL-SHOULD-BE-SILENTLY-IGNORED',
            'SET SESSION wait_timeout=28800'
        ]

        with patch('tap_mysql.connection.MySQLConnection.connect'):
            with patch('tap_mysql.connection.run_sql') as run_sql_mock:
                run_sql_mock.side_effect = self.run_sql_mock
                conn = MySQLConnectionMock(config={**test_utils.get_db_config(),
                                                   **{'session_sqls': session_sqls}})
                connect_with_backoff(conn)

        # Test if session variables applied on connection
        self.assertEqual(self.executed_queries, ['SET SESSION max_statement_time=0',
                                                 'SET SESSION wait_timeout=28800'])


class TestBitBooleanMapping(unittest.TestCase):

    def setUp(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute("CREATE TABLE bit_booleans_table(`id` int, `c_bit` BIT(4))")
                cursor.execute("INSERT INTO bit_booleans_table(`id`,`c_bit`) VALUES "
                               "(1, b'0000'),"
                               "(2, NULL),"
                               "(3, b'0010')")

        self.catalog = test_utils.discover_catalog(self.conn, {})

    def test_sync_messages_are_correct(self):
        self.catalog.streams[0] = test_utils.set_replication_method_and_key(self.catalog.streams[0], 'FULL_TABLE', None)
        self.catalog.streams[0] = test_utils.set_selected(self.catalog.streams[0], True)

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        tap_mysql.do_sync(self.conn, {}, self.catalog, {})

        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        self.assertEqual(len(record_messages), 3)
        self.assertListEqual([
            {'id': 1, 'c_bit': False},
            {'id': 2, 'c_bit': None},
            {'id': 3, 'c_bit': True},
        ], [rec.record for rec in record_messages])

    def tearDown(self) -> None:
        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('DROP TABLE bit_booleans_table;')
