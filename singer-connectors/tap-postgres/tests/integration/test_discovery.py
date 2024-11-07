import contextlib
import io
import unittest
import psycopg2
import tap_postgres

from psycopg2.extensions import quote_ident
from singer import metadata

from tap_postgres.discovery_utils import BASE_RECURSIVE_SCHEMAS
from ..utils import get_test_connection, ensure_test_table, get_test_connection_config

import tap_postgres.db as post_db


class TestStringTableWithPK(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": "id", "type": "integer", "primary_key": True, "serial": True},
                                  {"name": '"character-varying_name"', "type": "character varying"},
                                  {"name": '"varchar-name"', "type": "varchar(28)"},
                                  {"name": 'char_name', "type": "char(10)"},
                                  {"name": '"text-name"', "type": "text"}],
                      "name": TestStringTableWithPK.table_name}
        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == "public-CHICKEN TIMES"]

        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]
        self.assertEqual(TestStringTableWithPK.table_name, stream_dict.get('table_name'))
        self.assertEqual(TestStringTableWithPK.table_name, stream_dict.get('stream'))

        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                         {(): {'table-key-properties': ['id'], 'database-name': 'postgres',
                               'schema-name': 'public', 'is-view': False, 'row-count': 0},
                          ('properties', 'character-varying_name'): {'inclusion': 'available',
                                                                     'sql-datatype': 'character varying',
                                                                     'selected-by-default': True},
                          ('properties', 'id'): {'inclusion': 'automatic', 'sql-datatype': 'integer',
                                                 'selected-by-default': True},
                          ('properties', 'varchar-name'): {'inclusion': 'available',
                                                           'sql-datatype': 'character varying',
                                                           'selected-by-default': True},
                          ('properties', 'text-name'): {'inclusion': 'available', 'sql-datatype': 'text',
                                                        'selected-by-default': True},
                          ('properties', 'char_name'): {'selected-by-default': True, 'inclusion': 'available',
                                                        'sql-datatype': 'character'}})

        self.assertEqual({'properties': {'id': {'type': ['integer'],
                                                'maximum': 2147483647,
                                                'minimum': -2147483648},
                                         'character-varying_name': {'type': ['null', 'string']},
                                         'varchar-name': {'type': ['null', 'string'], 'maxLength': 28},
                                         'char_name': {'type': ['null', 'string'], 'maxLength': 10},
                                         'text-name': {'type': ['null', 'string']}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS}, stream_dict.get('schema'))


class TestIntegerTable(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": "id", "type": "integer", "serial": True},
                                  {"name": 'size integer', "type": "integer", "quoted": True},
                                  {"name": 'size smallint', "type": "smallint", "quoted": True},
                                  {"name": 'size bigint', "type": "bigint", "quoted": True}],
                      "name": TestIntegerTable.table_name}
        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-CHICKEN TIMES']

        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]

        self.assertEqual(TestStringTableWithPK.table_name, stream_dict.get('table_name'))
        self.assertEqual(TestStringTableWithPK.table_name, stream_dict.get('stream'))

        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                         {(): {'table-key-properties': [], 'database-name': 'postgres', 'schema-name': 'public',
                               'is-view': False, 'row-count': 0},
                          ('properties', 'id'): {'inclusion': 'available', 'sql-datatype': 'integer',
                                                 'selected-by-default': True},
                          ('properties', 'size integer'): {'inclusion': 'available', 'sql-datatype': 'integer',
                                                           'selected-by-default': True},
                          ('properties', 'size smallint'): {'inclusion': 'available', 'sql-datatype': 'smallint',
                                                            'selected-by-default': True},
                          ('properties', 'size bigint'): {'inclusion': 'available', 'sql-datatype': 'bigint',
                                                          'selected-by-default': True}})

        self.assertEqual({'definitions': BASE_RECURSIVE_SCHEMAS,
                          'type': 'object',
                          'properties': {
                              'id': {'type': ['null', 'integer'], 'minimum': -2147483648, 'maximum': 2147483647},
                              'size smallint': {'type': ['null', 'integer'], 'minimum': -32768, 'maximum': 32767},
                              'size integer': {'type': ['null', 'integer'], 'minimum': -2147483648,
                                               'maximum': 2147483647},
                              'size bigint': {'type': ['null', 'integer'], 'minimum': -9223372036854775808,
                                              'maximum': 9223372036854775807}}},
                         stream_dict.get('schema'))


class TestDecimalPK(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": 'our_decimal', "type": "numeric", "primary_key": True},
                                  {"name": 'our_decimal_10_2', "type": "decimal(10,2)"},
                                  {"name": 'our_decimal_38_4', "type": "decimal(38,4)"}],
                      "name": TestDecimalPK.table_name}
        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-CHICKEN TIMES']
        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]

        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                         {(): {'table-key-properties': ['our_decimal'], 'database-name': 'postgres',
                               'schema-name': 'public', 'is-view': False, 'row-count': 0},
                          ('properties', 'our_decimal'): {'inclusion': 'automatic', 'sql-datatype': 'numeric',
                                                          'selected-by-default': True},
                          ('properties', 'our_decimal_38_4'): {'inclusion': 'available', 'sql-datatype': 'numeric',
                                                               'selected-by-default': True},
                          ('properties', 'our_decimal_10_2'): {'inclusion': 'available', 'sql-datatype': 'numeric',
                                                               'selected-by-default': True}})

        self.assertEqual({'properties': {'our_decimal': {'exclusiveMaximum': True,
                                                         'exclusiveMinimum': True,
                                                         'multipleOf': 10 ** (0 - post_db.MAX_SCALE),
                                                         'maximum': 10 ** (post_db.MAX_PRECISION - post_db.MAX_SCALE),
                                                         'minimum': -10 ** (post_db.MAX_PRECISION - post_db.MAX_SCALE),
                                                         'type': ['number']},
                                         'our_decimal_10_2': {'exclusiveMaximum': True,
                                                              'exclusiveMinimum': True,
                                                              'maximum': 100000000,
                                                              'minimum': -100000000,
                                                              'multipleOf': 0.01,
                                                              'type': ['null', 'number']},
                                         'our_decimal_38_4': {'exclusiveMaximum': True,
                                                              'exclusiveMinimum': True,
                                                              'maximum': 10000000000000000000000000000000000,
                                                              'minimum': -10000000000000000000000000000000000,
                                                              'multipleOf': 0.0001,
                                                              'type': ['null', 'number']}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream_dict.get('schema'))


class TestDatesTablePK(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": 'our_date', "type": "DATE", "primary_key": True},
                                  {"name": 'our_ts', "type": "TIMESTAMP"},
                                  {"name": 'our_ts_tz', "type": "TIMESTAMP WITH TIME ZONE"},
                                  {"name": 'our_time', "type": "TIME"},
                                  {"name": 'our_time_tz', "type": "TIME WITH TIME ZONE"}],
                      "name": TestDatesTablePK.table_name}
        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-CHICKEN TIMES']
        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]

        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                         {(): {'table-key-properties': ['our_date'], 'database-name': 'postgres',
                               'schema-name': 'public', 'is-view': False, 'row-count': 0},
                          ('properties', 'our_date'): {'inclusion': 'automatic', 'sql-datatype': 'date',
                                                       'selected-by-default': True},
                          ('properties', 'our_ts'): {'inclusion': 'available',
                                                     'sql-datatype': 'timestamp without time zone',
                                                     'selected-by-default': True},
                          ('properties', 'our_ts_tz'): {'inclusion': 'available',
                                                        'sql-datatype': 'timestamp with time zone',
                                                        'selected-by-default': True},
                          ('properties', 'our_time'): {'inclusion': 'available',
                                                       'sql-datatype': 'time without time zone',
                                                       'selected-by-default': True},
                          ('properties', 'our_time_tz'): {'inclusion': 'available',
                                                          'sql-datatype': 'time with time zone',
                                                          'selected-by-default': True}})

        self.assertEqual({'properties': {'our_date': {'type': ['string'], 'format': 'date-time'},
                                         'our_ts': {'type': ['null', 'string'], 'format': 'date-time'},
                                         'our_ts_tz': {'type': ['null', 'string'], 'format': 'date-time'},
                                         'our_time': {'format': 'time', 'type': ['null', 'string']},
                                         'our_time_tz': {'format': 'time', 'type': ['null', 'string']}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream_dict.get('schema'))


class TestFloatTablePK(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": 'our_float', "type": "float", "primary_key": True},
                                  {"name": 'our_real', "type": "real"},
                                  {"name": 'our_double', "type": "double precision"}],
                      "name": TestFloatTablePK.table_name}
        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-CHICKEN TIMES']
        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]

        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])
        self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                         {(): {'table-key-properties': ['our_float'], 'database-name': 'postgres',
                               'schema-name': 'public', 'is-view': False, 'row-count': 0},
                          ('properties', 'our_float'): {'inclusion': 'automatic', 'sql-datatype': 'double precision',
                                                        'selected-by-default': True},
                          ('properties', 'our_real'): {'inclusion': 'available', 'sql-datatype': 'real',
                                                       'selected-by-default': True},
                          ('properties', 'our_double'): {'inclusion': 'available', 'sql-datatype': 'double precision',
                                                         'selected-by-default': True}})

        self.assertEqual({'properties': {'our_float': {'type': ['number']},
                                         'our_real': {'type': ['null', 'number']},
                                         'our_double': {'type': ['null', 'number']}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream_dict.get('schema'))


class TestBoolsAndBits(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": 'our_bool', "type": "boolean"},
                                  {"name": 'our_bit', "type": "bit"}],
                      "name": TestBoolsAndBits.table_name}
        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-CHICKEN TIMES']
        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]

        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])
        self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                         {(): {'table-key-properties': [], 'database-name': 'postgres', 'schema-name': 'public',
                               'is-view': False, 'row-count': 0},
                          ('properties', 'our_bool'): {'inclusion': 'available', 'sql-datatype': 'boolean',
                                                       'selected-by-default': True},
                          ('properties', 'our_bit'): {'inclusion': 'available', 'sql-datatype': 'bit',
                                                      'selected-by-default': True}})

        self.assertEqual({'properties': {'our_bool': {'type': ['null', 'boolean']},
                                         'our_bit': {'type': ['null', 'boolean']}},
                          'definitions': BASE_RECURSIVE_SCHEMAS,
                          'type': 'object'},
                         stream_dict.get('schema'))


class TestJsonTables(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": 'our_secrets', "type": "json"},
                                  {"name": 'our_secrets_b', "type": "jsonb"}],
                      "name": TestJsonTables.table_name}
        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-CHICKEN TIMES']
        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]

        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                         {(): {'table-key-properties': [], 'database-name': 'postgres', 'schema-name': 'public',
                               'is-view': False, 'row-count': 0},
                          ('properties', 'our_secrets'): {'inclusion': 'available', 'sql-datatype': 'json',
                                                          'selected-by-default': True},
                          ('properties', 'our_secrets_b'): {'inclusion': 'available', 'sql-datatype': 'jsonb',
                                                            'selected-by-default': True}})

        self.assertEqual({'properties': {'our_secrets': {'type': ['null', 'object', 'array']},
                                         'our_secrets_b': {'type': ['null', 'object', 'array']}},
                          'definitions': BASE_RECURSIVE_SCHEMAS,
                          'type': 'object'},
                         stream_dict.get('schema'))


class TestUUIDTables(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": 'our_pk', "type": "uuid", "primary_key": True},
                                  {"name": 'our_uuid', "type": "uuid"}],
                      "name": TestUUIDTables.table_name}
        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == "public-CHICKEN TIMES"]
        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]

        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                         {(): {'table-key-properties': ['our_pk'], 'database-name': 'postgres', 'schema-name': 'public',
                               'is-view': False, 'row-count': 0},
                          ('properties', 'our_pk'): {'inclusion': 'automatic', 'sql-datatype': 'uuid',
                                                     'selected-by-default': True},
                          ('properties', 'our_uuid'): {'inclusion': 'available', 'sql-datatype': 'uuid',
                                                       'selected-by-default': True}})

        self.assertEqual({'properties': {'our_uuid': {'type': ['null', 'string']},
                                         'our_pk': {'type': ['string']}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream_dict.get('schema'))


class TestHStoreTable(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": 'our_pk', "type": "hstore", "primary_key": True},
                                  {"name": 'our_hstore', "type": "hstore"}],
                      "name": TestHStoreTable.table_name}
        with get_test_connection(superuser=True) as conn:
            cur = conn.cursor()
            cur.execute(""" SELECT installed_version FROM pg_available_extensions WHERE name = 'hstore' """)
            if cur.fetchone()[0] is None:
                cur.execute(""" CREATE EXTENSION hstore; """)

        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-CHICKEN TIMES']
        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]
        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        with get_test_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """INSERT INTO "CHICKEN TIMES" (our_pk, our_hstore) VALUES ('size=>"small",name=>"betty"', 'size=>"big",name=>"fred"')""")
                cur.execute("""SELECT * FROM  "CHICKEN TIMES" """)

                self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                                 {(): {'table-key-properties': ['our_pk'], 'database-name': 'postgres',
                                       'schema-name': 'public', 'is-view': False, 'row-count': 0},
                                  ('properties', 'our_pk'): {'inclusion': 'automatic', 'sql-datatype': 'hstore',
                                                             'selected-by-default': True},
                                  ('properties', 'our_hstore'): {'inclusion': 'available', 'sql-datatype': 'hstore',
                                                                 'selected-by-default': True}})

                self.assertEqual({'properties': {'our_hstore': {'type': ['null', 'object'], 'properties': {}},
                                                 'our_pk': {'type': ['object'], 'properties': {}}},
                                  'type': 'object',
                                  'definitions': BASE_RECURSIVE_SCHEMAS},
                                 stream_dict.get('schema'))

    def test_escaping_values(self):
        key = 'nickname'
        value = "Dave's Courtyard"
        elem = '"{}"=>"{}"'.format(key, value)

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                query = tap_postgres.sync_strategies.logical_replication.create_hstore_elem_query(elem)
                self.assertEqual(query.as_string(cur), "SELECT hstore_to_array('\"nickname\"=>\"Dave''s Courtyard\"')")


class TestEnumTable(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": 'our_mood_enum_pk', "type": "mood_enum", "primary_key": True},
                                  {"name": 'our_mood_enum', "type": "mood_enum"}],
                      "name": TestHStoreTable.table_name}
        with get_test_connection() as conn:
            cur = conn.cursor()
            cur.execute("""     DROP TYPE IF EXISTS mood_enum CASCADE """)
            cur.execute("""     CREATE TYPE mood_enum AS ENUM ('sad', 'ok', 'happy'); """)

        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-CHICKEN TIMES']
        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]
        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        with get_test_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""INSERT INTO "CHICKEN TIMES" (our_mood_enum_pk, our_mood_enum) VALUES ('sad', 'happy')""")
                cur.execute("""SELECT * FROM  "CHICKEN TIMES" """)

                self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                                 {(): {'table-key-properties': ['our_mood_enum_pk'], 'database-name': 'postgres',
                                       'schema-name': 'public', 'is-view': False, 'row-count': 0},
                                  ('properties', 'our_mood_enum_pk'): {'inclusion': 'automatic',
                                                                       'sql-datatype': 'mood_enum',
                                                                       'selected-by-default': True},
                                  ('properties', 'our_mood_enum'): {'inclusion': 'available',
                                                                    'sql-datatype': 'mood_enum',
                                                                    'selected-by-default': True}})

                self.assertEqual({'properties': {'our_mood_enum': {'type': ['null', 'string']},
                                                 'our_mood_enum_pk': {'type': ['string']}},
                                  'type': 'object',
                                  'definitions': BASE_RECURSIVE_SCHEMAS},
                                 stream_dict.get('schema'))


class TestMoney(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": 'our_money_pk', "type": "money", "primary_key": True},
                                  {"name": 'our_money', "type": "money"}],
                      "name": TestHStoreTable.table_name}
        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-CHICKEN TIMES']
        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]
        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        with get_test_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""INSERT INTO "CHICKEN TIMES" (our_money_pk, our_money) VALUES ('$1.24', '$777.63')""")
                cur.execute("""SELECT * FROM  "CHICKEN TIMES" """)

                self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                                 {(): {'table-key-properties': ['our_money_pk'], 'database-name': 'postgres',
                                       'schema-name': 'public', 'is-view': False, 'row-count': 0},
                                  ('properties', 'our_money_pk'): {'inclusion': 'automatic', 'sql-datatype': 'money',
                                                                   'selected-by-default': True},
                                  ('properties', 'our_money'): {'inclusion': 'available', 'sql-datatype': 'money',
                                                                'selected-by-default': True}})

                self.assertEqual({'properties': {'our_money': {'type': ['null', 'string']},
                                                 'our_money_pk': {'type': ['string']}},
                                  'type': 'object',
                                  'definitions': BASE_RECURSIVE_SCHEMAS},
                                 stream_dict.get('schema'))


class TestArraysTable(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
        table_spec = {"columns": [{"name": 'our_int_array_pk', "type": "integer[]", "primary_key": True},
                                  {"name": 'our_string_array', "type": "varchar[]"}],
                      "name": TestHStoreTable.table_name}
        ensure_test_table(table_spec)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-CHICKEN TIMES']
        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]
        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        with get_test_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """INSERT INTO "CHICKEN TIMES" (our_int_array_pk, our_string_array) VALUES ('{{1,2,3},{4,5,6}}', '{{"a","b","c"}}' )""")
                cur.execute("""SELECT * FROM  "CHICKEN TIMES" """)

                self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                                 {(): {'table-key-properties': ['our_int_array_pk'], 'database-name': 'postgres',
                                       'schema-name': 'public', 'is-view': False, 'row-count': 0},
                                  ('properties', 'our_int_array_pk'): {'inclusion': 'automatic',
                                                                       'sql-datatype': 'integer[]',
                                                                       'selected-by-default': True},
                                  ('properties', 'our_string_array'): {'inclusion': 'available',
                                                                       'sql-datatype': 'character varying[]',
                                                                       'selected-by-default': True}})

                self.assertEqual({'properties': {'our_int_array_pk': {'type': ['null', 'array'], 'items': {
                    '$ref': '#/definitions/sdc_recursive_integer_array'}},
                                                 'our_string_array': {'type': ['null', 'array'], 'items': {
                                                     '$ref': '#/definitions/sdc_recursive_string_array'}}},
                                  'type': 'object',
                                  'definitions': BASE_RECURSIVE_SCHEMAS},
                                 stream_dict.get('schema'))


class TestArraysLikeTable(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'
    like_table_name = 'LIKE CHICKEN TIMES'

    def setUp(self):
        with get_test_connection('postgres') as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute('DROP MATERIALIZED VIEW IF EXISTS "LIKE CHICKEN TIMES"')
        table_spec = {"columns": [{"name": 'our_int_array_pk', "type": "integer[]", "primary_key": True},
                                  {"name": 'our_text_array', "type": "text[]"}],
                      "name": TestArraysLikeTable.table_name}
        ensure_test_table(table_spec)

        with get_test_connection('postgres') as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                create_sql = "CREATE MATERIALIZED VIEW {} AS SELECT * FROM {}\n".format(
                    quote_ident(TestArraysLikeTable.like_table_name, cur),
                    quote_ident(TestArraysLikeTable.table_name, cur))

                cur.execute(create_sql)

    def test_catalog(self):
        conn_config = get_test_connection_config()

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-LIKE CHICKEN TIMES']
        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]
        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        with get_test_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                                 {(): {'table-key-properties': [], 'database-name': 'postgres', 'schema-name': 'public',
                                       'is-view': True, 'row-count': 0},
                                  ('properties', 'our_int_array_pk'): {'inclusion': 'available',
                                                                       'sql-datatype': 'integer[]',
                                                                       'selected-by-default': True},
                                  ('properties', 'our_text_array'): {'inclusion': 'available', 'sql-datatype': 'text[]',
                                                                     'selected-by-default': True}})
                self.assertEqual({'properties': {'our_int_array_pk': {'type': ['null', 'array'], 'items': {
                    '$ref': '#/definitions/sdc_recursive_integer_array'}},
                                                 'our_text_array': {'type': ['null', 'array'], 'items': {
                                                     '$ref': '#/definitions/sdc_recursive_string_array'}}},
                                  'type': 'object',
                                  'definitions': BASE_RECURSIVE_SCHEMAS},
                                 stream_dict.get('schema'))


class TestColumnGrants(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'
    user = 'tmp_user_for_grant_tests'
    password = 'password'

    def setUp(self):
        table_spec = {"columns": [{"name": "id", "type": "integer", "serial": True},
                                  {"name": 'size integer', "type": "integer", "quoted": True},
                                  {"name": 'size smallint', "type": "smallint", "quoted": True},
                                  {"name": 'size bigint', "type": "bigint", "quoted": True}],
                      "name": TestColumnGrants.table_name}
        ensure_test_table(table_spec)

        with get_test_connection(superuser=True) as conn:
            cur = conn.cursor()

            sql = """ DROP USER IF EXISTS {} """.format(self.user, self.password)
            cur.execute(sql)

            sql = """ CREATE USER {} WITH PASSWORD '{}' """.format(self.user, self.password)
            cur.execute(sql)

            sql = """ GRANT SELECT ("id") ON "{}" TO {}""".format(TestColumnGrants.table_name, self.user)
            cur.execute(sql)

    def test_catalog(self):
        conn_config = get_test_connection_config()
        conn_config['user'] = self.user
        conn_config['password'] = self.password

        my_stdout = io.StringIO()
        with contextlib.redirect_stdout(my_stdout):
            streams = tap_postgres.do_discovery(conn_config)

        chicken_streams = [s for s in streams if s['tap_stream_id'] == 'public-CHICKEN TIMES']

        self.assertEqual(len(chicken_streams), 1)
        stream_dict = chicken_streams[0]

        self.assertEqual(TestStringTableWithPK.table_name, stream_dict.get('table_name'))
        self.assertEqual(TestStringTableWithPK.table_name, stream_dict.get('stream'))

        stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

        self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                         {(): {'table-key-properties': [],
                               'database-name': 'postgres',
                               'schema-name': 'public',
                               'is-view': False,
                               'row-count': 0},
                          ('properties', 'id'): {'inclusion': 'available',
                                                 'selected-by-default': True,
                                                 'sql-datatype': 'integer'}})

        self.assertEqual({'definitions': BASE_RECURSIVE_SCHEMAS,
                          'type': 'object',
                          'properties': {'id': {'type': ['null', 'integer'],
                                                'minimum': -2147483648,
                                                'maximum': 2147483647}}},
                         stream_dict.get('schema'))
