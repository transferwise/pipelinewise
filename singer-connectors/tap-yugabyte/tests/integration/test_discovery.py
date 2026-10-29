import unittest
from unittest.mock import patch

import psycopg2.extras
from psycopg2.extensions import quote_ident
from singer import metadata

import tap_yugabyte
import tap_yugabyte.db as yb_db
from tap_yugabyte.discovery_utils import BASE_RECURSIVE_SCHEMAS

from tests.utils import ensure_test_table, get_test_connection, get_test_connection_config

TABLE_NAME = 'CHICKEN TIMES'


def discover_stream(conn_config, tap_stream_id=f'public-{TABLE_NAME}'):
    """Run discovery and return the only stream with the given id, metadata sorted by breadcrumb."""
    with patch('tap_yugabyte.dump_catalog'):
        streams = tap_yugabyte.do_discovery(conn_config)

    matching = [s for s in streams if s['tap_stream_id'] == tap_stream_id]
    assert len(matching) == 1, f'expected one {tap_stream_id} stream, got {len(matching)}'
    stream = matching[0]
    stream['metadata'].sort(key=lambda md: md['breadcrumb'])
    return stream


def table_metadata(conn_config, key_properties, is_view=False):
    return {'table-key-properties': key_properties, 'database-name': conn_config['dbname'],
            'schema-name': 'public', 'is-view': is_view, 'row-count': 0}


def column_metadata(sql_datatype, inclusion='available'):
    return {'inclusion': inclusion, 'sql-datatype': sql_datatype, 'selected-by-default': True}


class TestStringTableWithPK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        ensure_test_table({'columns': [{'name': 'id', 'type': 'integer', 'primary_key': True, 'serial': True},
                                       {'name': '"character-varying_name"', 'type': 'character varying'},
                                       {'name': '"varchar-name"', 'type': 'varchar(28)'},
                                       {'name': 'char_name', 'type': 'char(10)'},
                                       {'name': '"text-name"', 'type': 'text'}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        self.assertEqual(TABLE_NAME, stream['table_name'])
        self.assertEqual(TABLE_NAME, stream['stream'])
        self.assertEqual({(): table_metadata(conn_config, ['id']),
                          ('properties', 'id'): column_metadata('integer', 'automatic'),
                          ('properties', 'character-varying_name'): column_metadata('character varying'),
                          ('properties', 'varchar-name'): column_metadata('character varying'),
                          ('properties', 'char_name'): column_metadata('character'),
                          ('properties', 'text-name'): column_metadata('text')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'id': {'type': ['integer'], 'maximum': 2147483647, 'minimum': -2147483648},
                                         'character-varying_name': {'type': ['null', 'string']},
                                         'varchar-name': {'type': ['null', 'string'], 'maxLength': 28},
                                         'char_name': {'type': ['null', 'string'], 'maxLength': 10},
                                         'text-name': {'type': ['null', 'string']}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream['schema'])


class TestIntegerTable(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        ensure_test_table({'columns': [{'name': 'id', 'type': 'integer', 'serial': True},
                                       {'name': 'size integer', 'type': 'integer', 'quoted': True},
                                       {'name': 'size smallint', 'type': 'smallint', 'quoted': True},
                                       {'name': 'size bigint', 'type': 'bigint', 'quoted': True}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        self.assertEqual(TABLE_NAME, stream['table_name'])
        self.assertEqual(TABLE_NAME, stream['stream'])
        self.assertEqual({(): table_metadata(conn_config, []),
                          ('properties', 'id'): column_metadata('integer'),
                          ('properties', 'size integer'): column_metadata('integer'),
                          ('properties', 'size smallint'): column_metadata('smallint'),
                          ('properties', 'size bigint'): column_metadata('bigint')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'definitions': BASE_RECURSIVE_SCHEMAS,
                          'type': 'object',
                          'properties': {
                              'id': {'type': ['null', 'integer'], 'minimum': -2147483648, 'maximum': 2147483647},
                              'size smallint': {'type': ['null', 'integer'], 'minimum': -32768, 'maximum': 32767},
                              'size integer': {'type': ['null', 'integer'], 'minimum': -2147483648,
                                               'maximum': 2147483647},
                              'size bigint': {'type': ['null', 'integer'], 'minimum': -9223372036854775808,
                                              'maximum': 9223372036854775807}}},
                         stream['schema'])


class TestDecimalPK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        ensure_test_table({'columns': [{'name': 'our_decimal', 'type': 'numeric', 'primary_key': True},
                                       {'name': 'our_decimal_10_2', 'type': 'decimal(10,2)'},
                                       {'name': 'our_decimal_38_4', 'type': 'decimal(38,4)'}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        self.assertEqual({(): table_metadata(conn_config, ['our_decimal']),
                          ('properties', 'our_decimal'): column_metadata('numeric', 'automatic'),
                          ('properties', 'our_decimal_38_4'): column_metadata('numeric'),
                          ('properties', 'our_decimal_10_2'): column_metadata('numeric')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'our_decimal': {'exclusiveMaximum': True,
                                                         'exclusiveMinimum': True,
                                                         'multipleOf': 10 ** (0 - yb_db.MAX_SCALE),
                                                         'maximum': 10 ** (yb_db.MAX_PRECISION - yb_db.MAX_SCALE),
                                                         'minimum': -10 ** (yb_db.MAX_PRECISION - yb_db.MAX_SCALE),
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
                         stream['schema'])


class TestDatesTablePK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        ensure_test_table({'columns': [{'name': 'our_date', 'type': 'DATE', 'primary_key': True},
                                       {'name': 'our_ts', 'type': 'TIMESTAMP'},
                                       {'name': 'our_ts_tz', 'type': 'TIMESTAMP WITH TIME ZONE'},
                                       {'name': 'our_time', 'type': 'TIME'},
                                       {'name': 'our_time_tz', 'type': 'TIME WITH TIME ZONE'}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        self.assertEqual({(): table_metadata(conn_config, ['our_date']),
                          ('properties', 'our_date'): column_metadata('date', 'automatic'),
                          ('properties', 'our_ts'): column_metadata('timestamp without time zone'),
                          ('properties', 'our_ts_tz'): column_metadata('timestamp with time zone'),
                          ('properties', 'our_time'): column_metadata('time without time zone'),
                          ('properties', 'our_time_tz'): column_metadata('time with time zone')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'our_date': {'type': ['string'], 'format': 'date-time'},
                                         'our_ts': {'type': ['null', 'string'], 'format': 'date-time'},
                                         'our_ts_tz': {'type': ['null', 'string'], 'format': 'date-time'},
                                         'our_time': {'format': 'time', 'type': ['null', 'string']},
                                         'our_time_tz': {'format': 'time', 'type': ['null', 'string']}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream['schema'])


class TestFloatTablePK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        ensure_test_table({'columns': [{'name': 'our_float', 'type': 'float', 'primary_key': True},
                                       {'name': 'our_real', 'type': 'real'},
                                       {'name': 'our_double', 'type': 'double precision'}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        self.assertEqual({(): table_metadata(conn_config, ['our_float']),
                          ('properties', 'our_float'): column_metadata('double precision', 'automatic'),
                          ('properties', 'our_real'): column_metadata('real'),
                          ('properties', 'our_double'): column_metadata('double precision')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'our_float': {'type': ['number']},
                                         'our_real': {'type': ['null', 'number']},
                                         'our_double': {'type': ['null', 'number']}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream['schema'])


class TestBoolsAndBits(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        ensure_test_table({'columns': [{'name': 'our_bool', 'type': 'boolean'},
                                       {'name': 'our_bit', 'type': 'bit'}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        self.assertEqual({(): table_metadata(conn_config, []),
                          ('properties', 'our_bool'): column_metadata('boolean'),
                          ('properties', 'our_bit'): column_metadata('bit')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'our_bool': {'type': ['null', 'boolean']},
                                         'our_bit': {'type': ['null', 'boolean']}},
                          'definitions': BASE_RECURSIVE_SCHEMAS,
                          'type': 'object'},
                         stream['schema'])


class TestJsonTables(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        ensure_test_table({'columns': [{'name': 'our_secrets', 'type': 'json'},
                                       {'name': 'our_secrets_b', 'type': 'jsonb'}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        self.assertEqual({(): table_metadata(conn_config, []),
                          ('properties', 'our_secrets'): column_metadata('json'),
                          ('properties', 'our_secrets_b'): column_metadata('jsonb')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'our_secrets': {'type': ['null', 'object', 'array']},
                                         'our_secrets_b': {'type': ['null', 'object', 'array']}},
                          'definitions': BASE_RECURSIVE_SCHEMAS,
                          'type': 'object'},
                         stream['schema'])


class TestUUIDTables(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        ensure_test_table({'columns': [{'name': 'our_pk', 'type': 'uuid', 'primary_key': True},
                                       {'name': 'our_uuid', 'type': 'uuid'}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        self.assertEqual({(): table_metadata(conn_config, ['our_pk']),
                          ('properties', 'our_pk'): column_metadata('uuid', 'automatic'),
                          ('properties', 'our_uuid'): column_metadata('uuid')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'our_uuid': {'type': ['null', 'string']},
                                         'our_pk': {'type': ['string']}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream['schema'])


class TestHStoreTable(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('CREATE EXTENSION IF NOT EXISTS hstore')

        # YSQL rejects hstore primary keys, so the non-null hstore schema has no table to come from.
        ensure_test_table({'columns': [{'name': 'id', 'type': 'integer', 'primary_key': True},
                                       {'name': 'our_hstore', 'type': 'hstore'}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        with get_test_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""INSERT INTO "CHICKEN TIMES" (id, our_hstore) VALUES (1, 'size=>"big",name=>"fred"')""")
                cur.execute('SELECT * FROM "CHICKEN TIMES"')

        self.assertEqual({(): table_metadata(conn_config, ['id']),
                          ('properties', 'id'): column_metadata('integer', 'automatic'),
                          ('properties', 'our_hstore'): column_metadata('hstore')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'id': {'type': ['integer'], 'maximum': 2147483647, 'minimum': -2147483648},
                                         'our_hstore': {'type': ['null', 'object'], 'properties': {}}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream['schema'])

    def test_escaping_values(self):
        elem = '"nickname"=>"Dave\'s Courtyard"'

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                query = tap_yugabyte.sync_strategies.logical_replication.create_hstore_elem_query(elem)
                self.assertEqual("SELECT hstore_to_array('\"nickname\"=>\"Dave''s Courtyard\"')", query.as_string(cur))


class TestEnumTable(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DROP TYPE IF EXISTS mood_enum CASCADE')
                cur.execute("CREATE TYPE mood_enum AS ENUM ('sad', 'ok', 'happy')")

        ensure_test_table({'columns': [{'name': 'our_mood_enum_pk', 'type': 'mood_enum', 'primary_key': True},
                                       {'name': 'our_mood_enum', 'type': 'mood_enum'}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        with get_test_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""INSERT INTO "CHICKEN TIMES" (our_mood_enum_pk, our_mood_enum) VALUES ('sad', 'happy')""")
                cur.execute('SELECT * FROM "CHICKEN TIMES"')

        self.assertEqual({(): table_metadata(conn_config, ['our_mood_enum_pk']),
                          ('properties', 'our_mood_enum_pk'): column_metadata('mood_enum', 'automatic'),
                          ('properties', 'our_mood_enum'): column_metadata('mood_enum')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'our_mood_enum': {'type': ['null', 'string']},
                                         'our_mood_enum_pk': {'type': ['string']}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream['schema'])


class TestMoney(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        ensure_test_table({'columns': [{'name': 'our_money_pk', 'type': 'money', 'primary_key': True},
                                       {'name': 'our_money', 'type': 'money'}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        with get_test_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""INSERT INTO "CHICKEN TIMES" (our_money_pk, our_money) VALUES ('$1.24', '$777.63')""")
                cur.execute('SELECT * FROM "CHICKEN TIMES"')

        self.assertEqual({(): table_metadata(conn_config, ['our_money_pk']),
                          ('properties', 'our_money_pk'): column_metadata('money', 'automatic'),
                          ('properties', 'our_money'): column_metadata('money')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'our_money': {'type': ['null', 'string']},
                                         'our_money_pk': {'type': ['string']}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream['schema'])


class TestArraysTable(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        # YSQL rejects array primary keys; the array schema is nullable either way.
        ensure_test_table({'columns': [{'name': 'id', 'type': 'integer', 'primary_key': True},
                                       {'name': 'our_int_array', 'type': 'integer[]'},
                                       {'name': 'our_string_array', 'type': 'varchar[]'}],
                           'name': TABLE_NAME})

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config)

        with get_test_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""INSERT INTO "CHICKEN TIMES" (id, our_int_array, our_string_array)
                               VALUES (1, '{{1,2,3},{4,5,6}}', '{{"a","b","c"}}')""")
                cur.execute('SELECT * FROM "CHICKEN TIMES"')

        self.assertEqual({(): table_metadata(conn_config, ['id']),
                          ('properties', 'id'): column_metadata('integer', 'automatic'),
                          ('properties', 'our_int_array'): column_metadata('integer[]'),
                          ('properties', 'our_string_array'): column_metadata('character varying[]')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'id': {'type': ['integer'], 'maximum': 2147483647, 'minimum': -2147483648},
                                         'our_int_array': {'type': ['null', 'array'], 'items': {
                                             '$ref': '#/definitions/sdc_recursive_integer_array'}},
                                         'our_string_array': {'type': ['null', 'array'], 'items': {
                                             '$ref': '#/definitions/sdc_recursive_string_array'}}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream['schema'])


class TestArraysLikeTable(unittest.TestCase):
    maxDiff = None
    like_table_name = 'LIKE CHICKEN TIMES'

    def setUp(self):
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DROP MATERIALIZED VIEW IF EXISTS {}'.format(quote_ident(self.like_table_name, cur)))

        # YSQL rejects array primary keys, so the source table is keyed on a separate id column.
        ensure_test_table({'columns': [{'name': 'id', 'type': 'integer', 'primary_key': True},
                                       {'name': 'our_int_array', 'type': 'integer[]'},
                                       {'name': 'our_text_array', 'type': 'text[]'}],
                           'name': TABLE_NAME})

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('CREATE MATERIALIZED VIEW {} AS SELECT our_int_array, our_text_array FROM {}'.format(
                    quote_ident(self.like_table_name, cur), quote_ident(TABLE_NAME, cur)))
                cur.execute('ANALYZE {}'.format(quote_ident(self.like_table_name, cur)))

    def tearDown(self):
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DROP MATERIALIZED VIEW IF EXISTS {}'.format(quote_ident(self.like_table_name, cur)))

    def test_catalog(self):
        conn_config = get_test_connection_config()
        stream = discover_stream(conn_config, f'public-{self.like_table_name}')

        self.assertEqual({(): table_metadata(conn_config, [], is_view=True),
                          ('properties', 'our_int_array'): column_metadata('integer[]'),
                          ('properties', 'our_text_array'): column_metadata('text[]')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'properties': {'our_int_array': {'type': ['null', 'array'], 'items': {
                                             '$ref': '#/definitions/sdc_recursive_integer_array'}},
                                         'our_text_array': {'type': ['null', 'array'], 'items': {
                                             '$ref': '#/definitions/sdc_recursive_string_array'}}},
                          'type': 'object',
                          'definitions': BASE_RECURSIVE_SCHEMAS},
                         stream['schema'])


class TestColumnGrants(unittest.TestCase):
    maxDiff = None
    user = 'tmp_user_for_grant_tests'
    password = 'password'

    def setUp(self):
        ensure_test_table({'columns': [{'name': 'id', 'type': 'integer', 'serial': True},
                                       {'name': 'size integer', 'type': 'integer', 'quoted': True},
                                       {'name': 'size smallint', 'type': 'smallint', 'quoted': True},
                                       {'name': 'size bigint', 'type': 'bigint', 'quoted': True}],
                           'name': TABLE_NAME})

        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DROP USER IF EXISTS {}'.format(self.user))
                cur.execute("CREATE USER {} WITH PASSWORD '{}'".format(self.user, self.password))
                cur.execute('GRANT SELECT ("id") ON {} TO {}'.format(quote_ident(TABLE_NAME, cur), self.user))

    def tearDown(self):
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DROP OWNED BY {}'.format(self.user))
                cur.execute('DROP USER IF EXISTS {}'.format(self.user))

    def test_catalog(self):
        conn_config = get_test_connection_config()
        conn_config['user'] = self.user
        conn_config['password'] = self.password
        stream = discover_stream(conn_config)

        self.assertEqual(TABLE_NAME, stream['table_name'])
        self.assertEqual(TABLE_NAME, stream['stream'])
        self.assertEqual({(): table_metadata(conn_config, []),
                          ('properties', 'id'): column_metadata('integer')},
                         metadata.to_map(stream['metadata']))
        self.assertEqual({'definitions': BASE_RECURSIVE_SCHEMAS,
                          'type': 'object',
                          'properties': {'id': {'type': ['null', 'integer'],
                                                'minimum': -2147483648,
                                                'maximum': 2147483647}}},
                         stream['schema'])
