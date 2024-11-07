import psycopg2
import singer
import os
import decimal
import math
import datetime

from psycopg2.extensions import quote_ident
from singer import get_logger, metadata

LOGGER = get_logger()


class MockedConnect:
    class cursor:
        return_value = 1234
        counter_limit = 3
        fetchone_return_value = [5]

        def __init__(self, *args, **kwargs):
            self.counter = 0

        def __enter__(self):
            return self

        def __exit__(self, *args, **kwargs):
            pass

        def __iter__(self):
            return self

        def __next__(self):
            self.counter += 1
            if self.counter < self.counter_limit:
                return [self.return_value]
            raise StopIteration

        def fetchone(self):
            return self.fetchone_return_value

        def execute(self, *args, **kwargs):
            pass

    def __enter__(self):
        pass

    def __exit__(self, *args, **kwargs):
        pass

    def __init__(self, *args, **kwargs):
        pass


def get_test_connection_config(target_db='postgres', use_secondary=False):
    try:
        conn_config = {'host': os.environ['TAP_POSTGRES_HOST'],
                       'user': os.environ['TAP_POSTGRES_USER'],
                       'password': os.environ['TAP_POSTGRES_PASSWORD'],
                       'postgres_password': os.environ['TAP_POSTGRES_PG_PASSWORD'],
                       'port': os.environ['TAP_POSTGRES_PORT'],
                       'dbname': target_db,
                       'use_secondary': use_secondary,
                       'tap_id': 'tap_test',
                       'max_run_seconds': 43200,
                       'break_at_end_lsn': True,
                       'logical_poll_total_seconds': 2
                       }
    except KeyError as exc:
        raise Exception(
            "set TAP_POSTGRES_HOST, TAP_POSTGRES_USER, TAP_POSTGRES_PASSWORD, TAP_POSTGRES_PORT"
        ) from exc

    if use_secondary:
        try:
            conn_config.update({
                'secondary_host': os.environ['TAP_POSTGRES_SECONDARY_HOST'],
                'secondary_port': os.environ['TAP_POSTGRES_SECONDARY_PORT'],
            })
        except KeyError as exc:
            raise Exception(
                "set TAP_POSTGRES_SECONDARY_HOST, TAP_POSTGRES_SECONDARY_PORT"
            ) from exc

    return conn_config


def get_test_connection(target_db='postgres', superuser=False):
    conn_config = get_test_connection_config(target_db)

    user, password = ('postgres', conn_config['postgres_password']) if superuser \
        else (conn_config['user'], conn_config['password'])

    conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(conn_config['host'],
                                                                                   conn_config['dbname'],
                                                                                   user,
                                                                                   password,
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

def ensure_test_table(table_spec, target_db='postgres'):
    with get_test_connection(target_db) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute('DROP TABLE if exists {} cascade'.format(quote_ident(table_spec['name'], cur)))

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
    old_md.update({'replication-method': method, 'selected': True})

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


def create_replication_slot(target_db='postgres', tap_id='tap_test'):

    sql = f"select pg_create_logical_replication_slot('pipelinewise_{target_db}_{tap_id}', 'wal2json');"

    with get_test_connection(target_db) as conn:
        with conn.cursor() as cur:
            LOGGER.info("Creating replication slot: %s", sql)
            cur.execute(sql)


def drop_replication_slot(target_db='postgres', tap_id='tap_test'):

    sql = f"SELECT pg_drop_replication_slot('pipelinewise_{target_db}_{tap_id}');"

    with get_test_connection(target_db) as conn:
        with conn.cursor() as cur:
            LOGGER.info("Dropping replication slot: %s", sql)
            cur.execute(sql)

def drop_table(table_name, target_db='postgres'):
    with get_test_connection(target_db) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute('DROP TABLE IF EXISTS {} cascade'.format(quote_ident(table_name, cur)))
