import os

import psycopg2
import psycopg2.extras

from psycopg2.extensions import quote_ident
from singer import get_logger

LOGGER = get_logger()


def get_test_connection_config():
    """Build a tap connection config from the TAP_YUGABYTE_* environment variables."""
    try:
        return {
            'host': os.environ['TAP_YUGABYTE_HOST'],
            'port': os.environ['TAP_YUGABYTE_PORT'],
            'user': os.environ['TAP_YUGABYTE_USER'],
            'password': os.environ['TAP_YUGABYTE_PASSWORD'],
            'dbname': os.environ['TAP_YUGABYTE_DB'],
            'tap_id': 'tap_test',
        }
    except KeyError as exc:
        raise Exception(
            'set TAP_YUGABYTE_HOST, TAP_YUGABYTE_PORT, TAP_YUGABYTE_USER, '
            'TAP_YUGABYTE_PASSWORD, TAP_YUGABYTE_DB'
        ) from exc


def get_test_connection():
    """Open an autocommit connection to the test database."""
    conn_config = get_test_connection_config()

    conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(
        conn_config['host'],
        conn_config['dbname'],
        conn_config['user'],
        conn_config['password'],
        conn_config['port'])

    LOGGER.info('connecting to %s', conn_config['host'])

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True

    return conn


def build_col_sql(col, cur):
    if col.get('quoted'):
        return '{} {}'.format(quote_ident(col['name'], cur), col['type'])

    return '{} {}'.format(col['name'], col['type'])


def build_table(table, cur):
    create_sql = 'CREATE TABLE {}\n'.format(quote_ident(table['name'], cur))
    col_sql = map(lambda c: build_col_sql(c, cur), table['columns'])
    pks = [c['name'] for c in table['columns'] if c.get('primary_key')]
    if len(pks) != 0:
        pk_sql = ',\n CONSTRAINT {}  PRIMARY KEY({})'.format(quote_ident(table['name'] + '_pk', cur), ' ,'.join(pks))
    else:
        pk_sql = ''

    return '{} ( {} {})'.format(create_sql, ',\n'.join(col_sql), pk_sql)


def ensure_test_table(table_spec):
    """Recreate the table described by table_spec, dropping any previous version."""
    with get_test_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute('DROP TABLE if exists {} cascade'.format(quote_ident(table_spec['name'], cur)))

            sql = build_table(table_spec, cur)
            LOGGER.info('create table sql: %s', sql)
            cur.execute(sql)
            cur.execute('ANALYZE {}'.format(quote_ident(table_spec['name'], cur)))


def drop_table(table_name):
    with get_test_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute('DROP TABLE IF EXISTS {} cascade'.format(quote_ident(table_name, cur)))
