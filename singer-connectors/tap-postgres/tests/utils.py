from singer import get_logger, metadata
from nose.tools import nottest
import psycopg2
import singer
import os
import decimal
import math
import datetime
import pdb
from psycopg2.extensions import quote_ident

LOGGER = get_logger()

def get_test_connection_config(target_db='postgres'):
    missing_envs = [x for x in [os.getenv('TAP_POSTGRES_HOST'),
                                os.getenv('TAP_POSTGRES_USER'),
                                os.getenv('TAP_POSTGRES_PASSWORD'),
                                os.getenv('TAP_POSTGRES_PORT')] if x == None]
    if len(missing_envs) != 0:
        #pylint: disable=line-too-long
        raise Exception("set TAP_POSTGRES_HOST, TAP_POSTGRES_USER, TAP_POSTGRES_PASSWORD, TAP_POSTGRES_PORT")

    conn_config = {}
    conn_config['host'] = os.environ.get('TAP_POSTGRES_HOST')
    conn_config['user'] = os.environ.get('TAP_POSTGRES_USER')
    conn_config['password'] = os.environ.get('TAP_POSTGRES_PASSWORD')
    conn_config['port'] = os.environ.get('TAP_POSTGRES_PORT')
    conn_config['dbname'] = target_db
    return conn_config

def get_test_connection(target_db='postgres'):
    conn_config = get_test_connection_config(target_db)
    conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(conn_config['host'],
                                                                                   conn_config['dbname'],
                                                                                   conn_config['user'],
                                                                                   conn_config['password'],
                                                                                   conn_config['port'])
    LOGGER.info("connecting to {}".format(conn_config['host']))

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True

    return conn

def build_col_sql(col, cur):
    if col.get('quoted'):
        col_sql = "{} {}".format(quote_ident(col['name'], cur), col['type'])
    else:
        col_sql = "{} {}".format(col['name'], col['type'])

    return col_sql

def build_table(table, cur):
    create_sql = "CREATE TABLE {}\n".format(quote_ident(table['name'], cur))
    col_sql = map(lambda c: build_col_sql(c, cur), table['columns'])
    pks = [c['name'] for c in table['columns'] if c.get('primary_key')]
    if len(pks) != 0:
        pk_sql = ",\n CONSTRAINT {}  PRIMARY KEY({})".format(quote_ident(table['name'] + "_pk", cur), " ,".join(pks))
    else:
       pk_sql = ""

    sql = "{} ( {} {})".format(create_sql, ",\n".join(col_sql), pk_sql)

    return sql

@nottest
def ensure_test_table(table_spec, target_db='postgres'):
    with get_test_connection(target_db) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            sql = """SELECT *
                       FROM information_schema.tables
                      WHERE table_schema = 'public'
                        AND table_name = %s"""

            cur.execute(sql,
                        [table_spec['name']])
            old_table = cur.fetchall()

            if len(old_table) != 0:
                cur.execute('DROP TABLE {} cascade'.format(quote_ident(table_spec['name'], cur)))

            sql = build_table(table_spec, cur)
            LOGGER.info("create table sql: %s", sql)
            cur.execute(sql)

def unselect_column(our_stream, col):
    md = metadata.to_map(our_stream['metadata'])
    md.get(('properties', col))['selected'] = False
    our_stream['metadata'] = metadata.to_list(md)
    return our_stream

def set_replication_method_for_stream(stream, method):
    new_md = metadata.to_map(stream['metadata'])
    old_md = new_md.get(())
    old_md.update({'replication-method': method})

    stream['metadata'] = metadata.to_list(new_md)
    return stream

def select_all_of_stream(stream):
    new_md = metadata.to_map(stream['metadata'])

    old_md = new_md.get(())
    old_md.update({'selected': True})
    for col_name, col_schema in stream['schema']['properties'].items():
        #explicitly select column if it is not automatic
        if new_md.get(('properties', col_name)).get('inclusion') != 'automatic' and new_md.get(('properties', col_name)).get('inclusion') != 'unsupported':
            old_md = new_md.get(('properties', col_name))
            old_md.update({'selected' : True})

    stream['metadata'] = metadata.to_list(new_md)
    return stream


def crud_up_value(value):
    if isinstance(value, str):
        return value
    elif isinstance(value, int):
        return str(value)
    elif isinstance(value, float):
        if (value == float('+inf')):
            return "'+Inf'"
        elif (value == float('-inf')):
            return "'-Inf'"
        elif (math.isnan(value)):
            return "'NaN'"
        else:
            return "{:f}".format(value)
    elif isinstance(value, decimal.Decimal):
        return "{:f}".format(value)
    elif value is None:
        return 'NULL'
    elif isinstance(value, datetime.datetime) and value.tzinfo is None:
        return "TIMESTAMP '{}'".format(str(value))
    elif isinstance(value, datetime.datetime):
        return "TIMESTAMP '{}'".format(str(value))
    elif isinstance(value, datetime.date):
        return "Date  '{}'".format(str(value))
    else:
        raise Exception("crud_up_value does not yet support {}".format(value.__class__))

def insert_record(cursor, table_name, data):
    our_keys = list(data.keys())
    our_keys.sort()
    our_values = list(map( lambda k: data.get(k), our_keys))


    columns_sql = ", \n".join(map(lambda k: quote_ident(k, cursor), our_keys))
    value_sql = ",".join(["%s" for i in range(len(our_keys))])

    insert_sql = """ INSERT INTO {}
                            ( {} )
                     VALUES ( {} )""".format(quote_ident(table_name, cursor), columns_sql, value_sql)
    LOGGER.info("INSERT: {}".format(insert_sql))
    cursor.execute(insert_sql, list(map(crud_up_value, our_values)))


def verify_crud_messages(that, caught_messages, pks):

    that.assertEqual(14, len(caught_messages))
    that.assertTrue(isinstance(caught_messages[0], singer.SchemaMessage))
    that.assertTrue(isinstance(caught_messages[1], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[2], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[3], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[4], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[5], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[6], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[7], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[8], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[9], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[10], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[11], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[12], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[13], singer.StateMessage))

    #schema includes scn && _sdc_deleted_at because we selected logminer as our replication method
    that.assertEqual({"type" : ['integer']}, caught_messages[0].schema.get('properties').get('scn') )
    that.assertEqual({"type" : ['null', 'string'], "format" : "date-time"}, caught_messages[0].schema.get('properties').get('_sdc_deleted_at') )

    that.assertEqual(pks, caught_messages[0].key_properties)

    #verify first STATE message
    bookmarks_1 = caught_messages[2].value.get('bookmarks')['ROOT-CHICKEN']
    that.assertIsNotNone(bookmarks_1)
    bookmarks_1_scn = bookmarks_1.get('scn')
    bookmarks_1_version = bookmarks_1.get('version')
    that.assertIsNotNone(bookmarks_1_scn)
    that.assertIsNotNone(bookmarks_1_version)

    #verify STATE message after UPDATE
    bookmarks_2 = caught_messages[6].value.get('bookmarks')['ROOT-CHICKEN']
    that.assertIsNotNone(bookmarks_2)
    bookmarks_2_scn = bookmarks_2.get('scn')
    bookmarks_2_version = bookmarks_2.get('version')
    that.assertIsNotNone(bookmarks_2_scn)
    that.assertIsNotNone(bookmarks_2_version)
    that.assertGreater(bookmarks_2_scn, bookmarks_1_scn)
    that.assertEqual(bookmarks_2_version, bookmarks_1_version)
