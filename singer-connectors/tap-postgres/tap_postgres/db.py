import datetime
import decimal
import math
import psycopg2
import psycopg2.extras
import singer
LOGGER = singer.get_logger()

cursor_iter_size = 20000
include_schemas_in_destination_stream_name = False


def calculate_destination_stream_name(stream, md_map):
    if include_schemas_in_destination_stream_name:
        return "{}-{}".format(md_map.get((), {}).get('schema-name'), stream['stream'])

    return stream['stream']

#from the postgres docs:
#Quoted identifiers can contain any character, except the character with code zero. (To include a double #quote, write two double quotes.)
def canonicalize_identifier(identifier):
    return identifier.replace('"', '""')

def fully_qualified_column_name(schema, table, column):
    return '"{}"."{}"."{}"'.format(canonicalize_identifier(schema), canonicalize_identifier(table), canonicalize_identifier(column))

def fully_qualified_table_name(schema, table):
    return '"{}"."{}"'.format(canonicalize_identifier(schema), canonicalize_identifier(table))

def open_connection(conn_config, logical_replication=False):
    if logical_replication:
        conn = psycopg2.connect(host=conn_config['host'], dbname=conn_config['dbname'], user=conn_config['user'], password=conn_config['password'], port=conn_config['port'],
                                connection_factory=psycopg2.extras.LogicalReplicationConnection, connect_timeout=30)
    else:
        conn = psycopg2.connect(host=conn_config['host'], dbname=conn_config['dbname'], user=conn_config['user'], password=conn_config['password'], port=conn_config['port'], connect_timeout=30)

    return conn

def prepare_columns_sql(c):
    column_name = """ "{}" """.format(canonicalize_identifier(c))
    return column_name

def filter_dbs_sql_clause(sql, filter_dbs):
    in_clause = " AND datname in (" + ",".join(["'{}'".format(b.strip(' ')) for b in filter_dbs.split(',')]) + ")"
    return sql + in_clause

def filter_schemas_sql_clause(sql, filer_schemas):
    in_clause = " AND n.nspname in (" + ",".join(["'{}'".format(b.strip(' ')) for b in filer_schemas.split(',')]) + ")"
    return sql + in_clause

#pylint: disable=too-many-branches,too-many-nested-blocks
def selected_value_to_singer_value_impl(elem, sql_datatype):
    sql_datatype = sql_datatype.replace('[]', '')
    if elem is None:
        cleaned_elem = elem
    elif sql_datatype == 'money':
        cleaned_elem = elem
    elif isinstance(elem, datetime.datetime):
        if sql_datatype == 'timestamp with time zone':
            cleaned_elem = elem.isoformat()
        else: #timestamp WITH OUT time zone
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
        #NB> We cast NaN's to NULL as wal2json does not support them and now we are at least consistent(ly wrong)
        if elem.is_nan():
            cleaned_elem = None
        else:
            cleaned_elem = elem
    elif isinstance(elem, float):
        #NB> We cast NaN's, +Inf, -Inf to NULL as wal2json does not support them and now we are at least consistent(ly wrong)
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
            raise Exception("do not know how to marshall a dict if its not an hstore or json: {}".format(sql_datatype))
    else:
        raise Exception("do not know how to marshall value of class( {} ) and sql_datatype ( {} )".format(elem.__class__, sql_datatype))

    return cleaned_elem

def selected_array_to_singer_value(elem, sql_datatype):
    if isinstance(elem, list):
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype), elem))

    return selected_value_to_singer_value_impl(elem, sql_datatype)

def selected_value_to_singer_value(elem, sql_datatype):
    #are we dealing with an array?
    if sql_datatype.find('[]') > 0:
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype), (elem or [])))

    return selected_value_to_singer_value_impl(elem, sql_datatype)

#pylint: disable=too-many-arguments
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


def compute_tap_stream_id(database_name, schema_name, table_name):
    return database_name + '-' + schema_name + '-' + table_name


#NB> numeric/decimal columns in postgres without a specified scale && precision
#default to 'up to 131072 digits before the decimal point; up to 16383
#digits after the decimal point'. For practical reasons, we are capping this at 74/38
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
