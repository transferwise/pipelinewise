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

LOGGER = singer.get_logger('tap_yugabyte')

CURSOR_ITER_SIZE = 20000


def open_connection(conn_config, logical_replication=False):
    cfg = {
        'application_name': 'pipelinewise',
        'host': conn_config['host'],
        'dbname': conn_config['dbname'],
        'user': conn_config['user'],
        'password': conn_config['password'],
        'port': conn_config['port'],
        'connect_timeout': 30
    }

    if conn_config.get('sslmode'):
        cfg['sslmode'] = conn_config['sslmode']

    if logical_replication:
        cfg['connection_factory'] = psycopg2.extras.LogicalReplicationConnection

    conn = psycopg2.connect(**cfg)

    return conn

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


def compute_tap_stream_id(schema_name, table_name):
    return schema_name + '-' + table_name
