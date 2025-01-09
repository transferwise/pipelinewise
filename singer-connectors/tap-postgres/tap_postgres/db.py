import copy
import datetime
import json
import decimal
import math
import pytz
import psycopg2
import psycopg2.extras
import singer

from typing import List
from dateutil.parser import parse

LOGGER = singer.get_logger('tap_postgres')

CURSOR_ITER_SIZE = 20000


# pylint: disable=invalid-name,missing-function-docstring
def calculate_destination_stream_name(stream, md_map):
    return f"{md_map.get((), {}).get('schema-name')}-{stream['stream']}"


# from the postgres docs:
# Quoted identifiers can contain any character, except the character with code zero.
# (To include a double #quote, write two double quotes.)
def canonicalize_identifier(identifier):
    return identifier.replace('"', '""')


def fully_qualified_column_name(schema, table, column):
    return f'"{canonicalize_identifier(schema)}"."{canonicalize_identifier(table)}"."{canonicalize_identifier(column)}"'


def fully_qualified_table_name(schema, table):
    return f'"{canonicalize_identifier(schema)}"."{canonicalize_identifier(table)}"'


def open_connection(conn_config, logical_replication=False, prioritize_primary=False):
    cfg = {
        'application_name': 'pipelinewise',
        'host': conn_config['host'],
        'dbname': conn_config['dbname'],
        'user': conn_config['user'],
        'password': conn_config['password'],
        'port': conn_config['port'],
        'connect_timeout': 30
    }

    if conn_config['use_secondary'] and not prioritize_primary and not logical_replication:
        # Try to use replica but fallback to primary if keys are missing. This is the same behavior as
        # https://github.com/transferwise/pipelinewise/blob/master/pipelinewise/fastsync/commons/tap_postgres.py#L129
        cfg.update({
            'host': conn_config.get("secondary_host", conn_config['host']),
            'port': conn_config.get("secondary_port", conn_config['port']),
        })

    if conn_config.get('sslmode'):
        cfg['sslmode'] = conn_config['sslmode']

    if logical_replication:
        cfg['connection_factory'] = psycopg2.extras.LogicalReplicationConnection

    conn = psycopg2.connect(**cfg)

    return conn

def prepare_columns_for_select_sql(c, md_map):
    column_name = f' "{canonicalize_identifier(c)}" '

    if ('properties', c) in md_map:
        sql_datatype = md_map[('properties', c)]['sql-datatype']
        if sql_datatype.startswith('timestamp') and not sql_datatype.endswith('[]'):
            return f'CASE ' \
                   f'WHEN {column_name} < \'0001-01-01 00:00:00.000\' ' \
                   f'OR {column_name} > \'9999-12-31 23:59:59.999\' THEN \'9999-12-31 23:59:59.999\' ' \
                   f'ELSE {column_name} ' \
                   f'END AS {column_name}'
    return column_name

def prepare_columns_sql(c):
    column_name = f""" "{canonicalize_identifier(c)}" """
    return column_name


def filter_dbs_sql_clause(sql, filter_dbs):
    in_clause = " AND datname in (" + ",".join([f"'{b.strip(' ')}'" for b in filter_dbs.split(',')]) + ")"
    return sql + in_clause


def filter_schemas_sql_clause(sql, filer_schemas):
    in_clause = " AND n.nspname in (" + ",".join([f"'{b.strip(' ')}'" for b in filer_schemas.split(',')]) + ")"
    return sql + in_clause


# pylint: disable=too-many-branches,too-many-nested-blocks,too-many-statements
def selected_value_to_singer_value_impl(elem, sql_datatype):
    sql_datatype = sql_datatype.replace('[]', '')
    if elem is None:
        cleaned_elem = elem
    elif sql_datatype == 'money':
        cleaned_elem = elem
    elif sql_datatype in ['json', 'jsonb']:
        cleaned_elem = json.loads(elem)
    elif sql_datatype == 'time with time zone':
        # time with time zone values will be converted to UTC and time zone dropped
        # Replace hour=24 with hour=0
        elem = str(elem)
        if elem.startswith('24'):
            elem = elem.replace('24', '00', 1)
        # convert to UTC
        elem = datetime.datetime.strptime(elem, '%H:%M:%S%z')
        if elem.utcoffset() != datetime.timedelta(seconds=0):
            LOGGER.warning('time with time zone values are converted to UTC')
        elem = elem.astimezone(pytz.utc)
        # drop time zone
        elem = str(elem.strftime('%H:%M:%S'))
        cleaned_elem = parse(elem).isoformat().split('T')[1]
    elif sql_datatype == 'time without time zone':
        # Replace hour=24 with hour=0
        elem = str(elem)
        if elem.startswith('24'):
            elem = elem.replace('24', '00', 1)
        cleaned_elem = parse(elem).isoformat().split('T')[1]
    elif isinstance(elem, datetime.datetime):
        if sql_datatype == 'timestamp with time zone':
            cleaned_elem = elem.isoformat()
        else:  # timestamp WITH OUT time zone
            cleaned_elem = elem.isoformat() + '+00:00'
    elif isinstance(elem, datetime.date):
        cleaned_elem = elem.isoformat() + 'T00:00:00+00:00'
    elif sql_datatype == 'bit':
        cleaned_elem = elem == '1'
    elif sql_datatype == 'boolean':
        cleaned_elem = elem
    elif isinstance(elem, int):
        cleaned_elem = elem
    elif isinstance(elem, datetime.time):
        cleaned_elem = str(elem)
    elif isinstance(elem, str):
        cleaned_elem = elem
    elif isinstance(elem, decimal.Decimal):
        # NB> We cast NaN's to NULL as wal2json does not support them and now we are at least consistent(ly wrong)
        if elem.is_nan():
            cleaned_elem = None
        else:
            cleaned_elem = elem
    elif isinstance(elem, float):
        # NB> We cast NaN's, +Inf, -Inf to NULL as wal2json does not support them and
        # now we are at least consistent(ly wrong)
        if math.isnan(elem):
            cleaned_elem = None
        elif math.isinf(elem):
            cleaned_elem = None
        else:
            cleaned_elem = elem
    elif isinstance(elem, dict):
        if sql_datatype == 'hstore':
            cleaned_elem = elem
        else:
            raise Exception(f"do not know how to marshall a dict if its not an hstore or json: {sql_datatype}")
    else:
        raise Exception(
            f"do not know how to marshall value of class( {elem.__class__} ) and sql_datatype ( {sql_datatype} )")

    return cleaned_elem


def selected_array_to_singer_value(elem, sql_datatype):
    if isinstance(elem, list):
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype), elem))

    return selected_value_to_singer_value_impl(elem, sql_datatype)


def selected_value_to_singer_value(elem, sql_datatype):
    # are we dealing with an array?
    if sql_datatype.find('[]') > 0:
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype), (elem or [])))

    return selected_value_to_singer_value_impl(elem, sql_datatype)


# pylint: disable=too-many-arguments
def selected_row_to_singer_message(stream, row, version, columns, time_extracted, md_map):
    row_to_persist = ()
    for idx, elem in enumerate(row):
        sql_datatype = md_map.get(('properties', columns[idx]))['sql-datatype']
        cleaned_elem = selected_value_to_singer_value(elem, sql_datatype)
        row_to_persist += (cleaned_elem,)

    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=calculate_destination_stream_name(stream, md_map),
        record=rec,
        version=version,
        time_extracted=time_extracted)


def hstore_available(conn_info):
    with open_connection(conn_info) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
            cur.execute(""" SELECT installed_version FROM pg_available_extensions WHERE name = 'hstore' """)
            res = cur.fetchone()
            if res and res[0]:
                return True
            return False


def compute_tap_stream_id(schema_name, table_name):
    return schema_name + '-' + table_name


# NB> numeric/decimal columns in postgres without a specified scale && precision
# default to 'up to 131072 digits before the decimal point; up to 16383
# digits after the decimal point'. For practical reasons, we are capping this at 74/38
#  https://www.postgresql.org/docs/10/static/datatype-numeric.html#DATATYPE-NUMERIC-TABLE
MAX_SCALE = 38
MAX_PRECISION = 100


def numeric_precision(c):
    if c.numeric_precision is None:
        return MAX_PRECISION

    if c.numeric_precision > MAX_PRECISION:
        LOGGER.warning('capping decimal precision to 100.  THIS MAY CAUSE TRUNCATION')
        return MAX_PRECISION

    return c.numeric_precision


def numeric_scale(c):
    if c.numeric_scale is None:
        return MAX_SCALE
    if c.numeric_scale > MAX_SCALE:
        LOGGER.warning('capping decimal scale to 38.  THIS MAY CAUSE TRUNCATION')
        return MAX_SCALE

    return c.numeric_scale


def numeric_multiple_of(scale):
    return 10 ** (0 - scale)


def numeric_max(precision, scale):
    return 10 ** (precision - scale)


def numeric_min(precision, scale):
    return -10 ** (precision - scale)


def filter_tables_sql_clause(sql, tables: List[str]):
    in_clause = " AND pg_class.relname in (" + ",".join([f"'{b.strip(' ')}'" for b in tables]) + ")"
    return sql + in_clause

def get_database_name(connection):
    cur = connection.cursor()
    rows = cur.execute("SELECT name FROM v$database").fetchall()
    return rows[0][0]

def attempt_connection_to_db(conn_config, dbname):
    nascent_config = copy.deepcopy(conn_config)
    nascent_config['dbname'] = dbname
    LOGGER.info('(%s) Testing connectivity...', dbname)
    try:
        conn = open_connection(nascent_config)
        LOGGER.info('(%s) connectivity verified', dbname)
        conn.close()
        return True
    except Exception as err:
        LOGGER.warning('Unable to connect to %s. This maybe harmless if you '
                       'have not desire to replicate from this database: "%s"', dbname, err)
        return False
