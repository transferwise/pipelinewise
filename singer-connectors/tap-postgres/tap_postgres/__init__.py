#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name,too-many-return-statements,too-many-branches,len-as-condition,too-many-statements,broad-except,unnecessary-lambda

import datetime
import pdb
import json
import os
import sys
import time
import collections
import itertools
from itertools import dropwhile
import copy
import ssl
import psycopg2
import psycopg2.extras
import singer
import singer.schema
from singer import utils, metadata, get_bookmark
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

import tap_postgres.sync_strategies.logical_replication as logical_replication
import tap_postgres.sync_strategies.full_table as full_table
import tap_postgres.sync_strategies.incremental as incremental
import tap_postgres.db as post_db
import tap_postgres.sync_strategies.common as sync_common
LOGGER = singer.get_logger()

#LogMiner do not support LONG, LONG RAW, CLOB, BLOB, NCLOB, ADT, or COLLECTION datatypes.
Column = collections.namedtuple('Column', [
    "column_name",
    "is_primary_key",
    "sql_data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "is_array",
    "is_enum"

])


REQUIRED_CONFIG_KEYS = [
    'dbname',
    'host',
    'port',
    'user',
    'password'
]


INTEGER_TYPES = {'integer', 'smallint', 'bigint'}
FLOAT_TYPES = {'real', 'double precision'}
JSON_TYPES = {'json', 'jsonb'}


def nullable_column(col_type, pk):
    if pk:
        return  [col_type]
    return ['null', col_type]

def schema_for_column_datatype(c):
    schema = {}
    #remove any array notation from type information as we use a separate field for that
    data_type = c.sql_data_type.lower().replace('[]', '')

    if data_type in INTEGER_TYPES:
        schema['type'] = nullable_column('integer', c.is_primary_key)
        schema['minimum'] = -1 * (2**(c.numeric_precision - 1))
        schema['maximum'] = 2**(c.numeric_precision - 1) - 1
        return schema

    if data_type == 'money':
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema
    if c.is_enum:
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    if data_type == 'bit' and c.character_maximum_length == 1:
        schema['type'] = nullable_column('boolean', c.is_primary_key)
        return schema

    if data_type == 'boolean':
        schema['type'] = nullable_column('boolean', c.is_primary_key)
        return schema

    if data_type == 'uuid':
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    if data_type == 'hstore':
        schema['type'] = nullable_column('object', c.is_primary_key)
        schema['properties'] = {}
        return schema

    if data_type == 'citext':
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    if data_type in JSON_TYPES:
        schema['type'] = nullable_column('object', c.is_primary_key)
        return schema

    if data_type == 'numeric':
        schema['type'] = nullable_column('number', c.is_primary_key)
        scale = post_db.numeric_scale(c)
        precision = post_db.numeric_precision(c)

        schema['exclusiveMaximum'] = True
        schema['maximum'] = post_db.numeric_max(precision, scale)
        schema['multipleOf'] = post_db.numeric_multiple_of(scale)
        schema['exclusiveMinimum'] = True
        schema['minimum'] = post_db.numeric_min(precision, scale)
        return schema

    if data_type in {'time without time zone', 'time with time zone'}:
        #times are treated as ordinary strings as they can not possible match RFC3339
        schema['type'] = nullable_column('string', c.is_primary_key)
        schema['format'] = 'time'
        return schema

    if data_type in ('date', 'timestamp without time zone', 'timestamp with time zone'):
        schema['type'] = nullable_column('string', c.is_primary_key)

        schema['format'] = 'date-time'
        return schema

    if data_type in FLOAT_TYPES:
        schema['type'] = nullable_column('number', c.is_primary_key)
        return schema

    if data_type == 'text':
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    if data_type == 'character varying':
        schema['type'] = nullable_column('string', c.is_primary_key)
        if c.character_maximum_length:
            schema['maxLength'] = c.character_maximum_length

        return schema

    if data_type == 'character':
        schema['type'] = nullable_column('string', c.is_primary_key)
        if c.character_maximum_length:
            schema['maxLength'] = c.character_maximum_length
        return schema

    if data_type in {'cidr', 'inet', 'macaddr'}:
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    return schema

def schema_name_for_numeric_array(precision, scale):
    schema_name = 'sdc_recursive_decimal_{}_{}_array'.format(precision, scale)
    return schema_name

def schema_for_column(c):
    #NB> from the post postgres docs: The current implementation does not enforce the declared number of dimensions either.
    #these means we can say nothing about an array column. its items may be more arrays or primitive types like integers
    #and this can vary on a row by row basis

    column_schema = {'type':["null", "array"]}
    if not c.is_array:
        return schema_for_column_datatype(c)

    if c.sql_data_type == 'integer[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_integer_array'}
    elif c.sql_data_type == 'bigint[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_integer_array'}
    elif c.sql_data_type == 'bit[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_boolean_array'}
    elif c.sql_data_type == 'boolean[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_boolean_array'}
    elif c.sql_data_type == 'character varying[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif c.sql_data_type == 'cidr[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif c.sql_data_type == 'citext[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif c.sql_data_type == 'date[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_timestamp_array'}
    elif c.sql_data_type == 'numeric[]':
        scale = post_db.numeric_scale(c)
        precision = post_db.numeric_precision(c)
        schema_name = schema_name_for_numeric_array(precision, scale)
        column_schema['items'] = {'$ref': '#/definitions/{}'.format(schema_name)}
    elif c.sql_data_type == 'double precision[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_number_array'}
    elif c.sql_data_type == 'hstore[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_object_array'}
    elif c.sql_data_type == 'inet[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif c.sql_data_type == 'json[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_object_array'}
    elif c.sql_data_type == 'jsonb[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_object_array'}
    elif c.sql_data_type == 'mac[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif c.sql_data_type == 'money[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif c.sql_data_type == 'real[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_number_array'}
    elif c.sql_data_type == 'smallint[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_integer_array'}
    elif c.sql_data_type == 'text[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif c.sql_data_type == 'timestamp without time zone[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_timestamp_array'}
    elif c.sql_data_type == 'timestamp with time zone[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_timestamp_array'}
    elif c.sql_data_type == 'time[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    elif c.sql_data_type == 'uuid[]':
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    else:
        #custom datatypes like enums
        column_schema['items'] = {'$ref': '#/definitions/sdc_recursive_string_array'}
    return column_schema



#typlen  -1  == variable length arrays
#typelem != 0 points to subtypes. 23 in the case of arrays
# so integer arrays are typlen = -1, typelem = 23 because integer types are oid 23

#this seems to identify all arrays:
#select typname from pg_attribute  as pga join pg_type as pgt on pgt.oid = pga.atttypid  where typlen = -1 and typelem != 0 and pga.attndims > 0;
def produce_table_info(conn, filter_schemas=None):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
        cur.itersize = post_db.cursor_iter_size
        table_info = {}
          # SELECT CASE WHEN $2.typtype = 'd' THEN $2.typbasetype ELSE $1.atttypid END
        sql = """
SELECT
  pg_class.reltuples::BIGINT                            AS approximate_row_count,
  (pg_class.relkind = 'v' or pg_class.relkind = 'm')    AS is_view,
  n.nspname                                             AS schema_name,
  pg_class.relname                                      AS table_name,
  attname                                               AS column_name,
  i.indisprimary                                        AS primary_key,
  format_type(a.atttypid, NULL::integer)                AS data_type,
  information_schema._pg_char_max_length(CASE WHEN COALESCE(subpgt.typtype, pgt.typtype) = 'd'
                                              THEN COALESCE(subpgt.typbasetype, pgt.typbasetype) ELSE COALESCE(subpgt.oid, pgt.oid)
                                          END,
                                          information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number AS character_maximum_length,
  information_schema._pg_numeric_precision(CASE WHEN COALESCE(subpgt.typtype, pgt.typtype) = 'd'
                                                THEN COALESCE(subpgt.typbasetype, pgt.typbasetype) ELSE COALESCE(subpgt.oid, pgt.oid)
                                            END,
                                           information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number AS numeric_precision,
  information_schema._pg_numeric_scale(CASE WHEN COALESCE(subpgt.typtype, pgt.typtype) = 'd'
                                                THEN COALESCE(subpgt.typbasetype, pgt.typbasetype) ELSE COALESCE(subpgt.oid, pgt.oid)
                                        END,
                                       information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number AS numeric_scale,
  pgt.typcategory                       = 'A' AS is_array,
  COALESCE(subpgt.typtype, pgt.typtype) = 'e' AS is_enum
FROM pg_attribute a
LEFT JOIN pg_type AS pgt ON a.atttypid = pgt.oid
JOIN pg_class
  ON pg_class.oid = a.attrelid
JOIN pg_catalog.pg_namespace n
  ON n.oid = pg_class.relnamespace
LEFT OUTER JOIN pg_index as i
  ON a.attrelid = i.indrelid
 AND a.attnum = ANY(i.indkey)
 AND i.indisprimary = true
LEFT OUTER JOIN pg_type AS subpgt
  ON pgt.typelem = subpgt.oid
 AND pgt.typelem != 0
WHERE attnum > 0
AND NOT a.attisdropped
AND pg_class.relkind IN ('r', 'v', 'm')
AND n.nspname NOT in ('pg_toast', 'pg_catalog', 'information_schema')
AND has_table_privilege(pg_class.oid, 'SELECT') = true """

        if filter_schemas:
            sql = post_db.filter_schemas_sql_clause(sql, filter_schemas)

        cur.execute(sql)

        for row in cur.fetchall():
            row_count, is_view, schema_name, table_name, *col_info = row

            if table_info.get(schema_name) is None:
                table_info[schema_name] = {}

            if table_info[schema_name].get(table_name) is None:
                table_info[schema_name][table_name] = {'is_view': is_view, 'row_count' : row_count, 'columns' : {}}

            col_name = col_info[0]

            table_info[schema_name][table_name]['columns'][col_name] = Column(*col_info)

        return table_info

def get_database_name(connection):
    cur = connection.cursor()

    rows = cur.execute("SELECT name FROM v$database").fetchall()
    return rows[0][0]

def write_sql_data_type_md(mdata, col_info):
    c_name = col_info.column_name
    if col_info.sql_data_type == 'bit' and col_info.character_maximum_length > 1:
        mdata = metadata.write(mdata, ('properties', c_name), 'sql-datatype', "bit({})".format(col_info.character_maximum_length))
    else:
        mdata = metadata.write(mdata, ('properties', c_name), 'sql-datatype', col_info.sql_data_type)

    return mdata


BASE_RECURSIVE_SCHEMAS = {'sdc_recursive_integer_array'   : {'type' : ['null', 'integer', 'array'], 'items' : {'$ref': '#/definitions/sdc_recursive_integer_array'}},
                          'sdc_recursive_number_array'    : {'type' : ['null', 'number', 'array'], 'items'  : {'$ref': '#/definitions/sdc_recursive_number_array'}},
                          'sdc_recursive_string_array'    : {'type' : ['null', 'string', 'array'], 'items'  : {'$ref': '#/definitions/sdc_recursive_string_array'}},
                          'sdc_recursive_boolean_array'   : {'type' : ['null', 'boolean', 'array'], 'items' : {'$ref': '#/definitions/sdc_recursive_boolean_array'}},
                          'sdc_recursive_timestamp_array' : {'type' : ['null', 'string', 'array'], 'format' : 'date-time', 'items' : {'$ref': '#/definitions/sdc_recursive_timestamp_array'}},
                          'sdc_recursive_object_array'    : {'type' : ['null', 'object', 'array'], 'items' : {'$ref': '#/definitions/sdc_recursive_object_array'}}}

def include_array_schemas(columns, schema):
    schema['definitions'] = copy.deepcopy(BASE_RECURSIVE_SCHEMAS)


    decimal_array_columns = [key for key, value in columns.items() if value.sql_data_type == 'numeric[]']
    for c in decimal_array_columns:
        scale = post_db.numeric_scale(columns[c])
        precision = post_db.numeric_precision(columns[c])
        schema_name = schema_name_for_numeric_array(precision, scale)
        schema['definitions'][schema_name] = {'type' : ['null', 'number', 'array'],
                                              'multipleOf': post_db.numeric_multiple_of(scale),
                                              'exclusiveMaximum' : True,
                                              'maximum' : post_db.numeric_max(precision, scale),
                                              'exclusiveMinimum': True,
                                              'minimum' : post_db.numeric_min(precision, scale),
                                              'items' : {'$ref': '#/definitions/{}'.format(schema_name)}}

    return schema

def discover_columns(connection, table_info):
    entries = []
    for schema_name in table_info.keys():
        for table_name in table_info[schema_name].keys():

            mdata = {}
            columns = table_info[schema_name][table_name]['columns']
            table_pks = [col_name for col_name, col_info in columns.items() if col_info.is_primary_key]
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(" SELECT current_database()")
                database_name = cur.fetchone()[0]

            metadata.write(mdata, (), 'table-key-properties', table_pks)
            metadata.write(mdata, (), 'schema-name', schema_name)
            metadata.write(mdata, (), 'database-name', database_name)
            metadata.write(mdata, (), 'row-count', table_info[schema_name][table_name]['row_count'])
            metadata.write(mdata, (), 'is-view', table_info[schema_name][table_name].get('is_view'))

            column_schemas = {col_name : schema_for_column(col_info) for col_name, col_info in columns.items()}

            schema = {'type' : 'object',
                      'properties': column_schemas,
                      'definitions' : {}}

            schema = include_array_schemas(columns, schema)

            for c_name in column_schemas.keys():
                mdata = write_sql_data_type_md(mdata, columns[c_name])

                if column_schemas[c_name].get('type') is None:
                    mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'unsupported')
                    mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', False)
                elif table_info[schema_name][table_name]['columns'][c_name].is_primary_key:
                    mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'automatic')
                    mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', True)
                else:
                    mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'available')
                    mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', True)

            entry = {'table_name'    : table_name,
                     'stream'        : table_name,
                     'metadata'      : metadata.to_list(mdata),
                     'tap_stream_id' : post_db.compute_tap_stream_id(database_name, schema_name, table_name),
                     'schema'        : schema}

            entries.append(entry)

    return entries

def discover_db(connection, filter_schemas=None):
    table_info = produce_table_info(connection, filter_schemas)
    db_streams = discover_columns(connection, table_info)
    return db_streams

def attempt_connection_to_db(conn_config, dbname):
    nascent_config = copy.deepcopy(conn_config)
    nascent_config['dbname'] = dbname
    LOGGER.info('(%s) Testing connectivity...', dbname)
    try:
        conn = post_db.open_connection(nascent_config)
        LOGGER.info('(%s) connectivity verified', dbname)
        conn.close()
        return True
    except Exception as err:
        LOGGER.warning('Unable to connect to %s. This maybe harmless if you have not desire to replicate from this database: "%s"', dbname, err)
        return False

def dump_catalog(all_streams):
    json.dump({'streams' : all_streams}, sys.stdout, indent=2)

def do_discovery(conn_config):
    all_streams = []

    with post_db.open_connection(conn_config) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
            cur.itersize = post_db.cursor_iter_size
            sql = """SELECT datname
            FROM pg_database
            WHERE datistemplate = false
              AND datname != 'rdsadmin'"""

            if conn_config.get('filter_dbs'):
                sql = post_db.filter_dbs_sql_clause(sql, conn_config['filter_dbs'])

            LOGGER.info("Running DB discovery: %s with itersize %s", sql, cur.itersize)
            cur.execute(sql)
            found_dbs = (row[0] for row in cur.fetchall())

    filter_dbs = filter(lambda dbname: attempt_connection_to_db(conn_config, dbname), found_dbs)

    for db_row in filter_dbs:
        dbname = db_row
        LOGGER.info("Discovering db %s", dbname)
        conn_config['dbname'] = dbname
        with post_db.open_connection(conn_config) as conn:
            db_streams = discover_db(conn, conn_config.get('filter_schemas'))
            all_streams = all_streams + db_streams


    if len(all_streams) == 0:
        raise RuntimeError('0 tables were discovered across the entire cluster')

    dump_catalog(all_streams)
    return all_streams

def is_selected_via_metadata(stream):
    table_md = metadata.to_map(stream['metadata']).get((), {})
    return table_md.get('selected')

def do_sync_full_table(conn_config, stream, state, desired_columns, md_map):
    LOGGER.info("Stream %s is using full_table replication", stream['tap_stream_id'])
    sync_common.send_schema_message(stream, [])
    if md_map.get((), {}).get('is-view'):
        state = full_table.sync_view(conn_config, stream, state, desired_columns, md_map)
    else:
        state = full_table.sync_table(conn_config, stream, state, desired_columns, md_map)
    return state

#Possible state keys: replication_key, replication_key_value, version
def do_sync_incremental(conn_config, stream, state, desired_columns, md_map):
    replication_key = md_map.get((), {}).get('replication-key')
    LOGGER.info("Stream %s is using incremental replication with replication key %s", stream['tap_stream_id'], replication_key)

    stream_state = state.get('bookmarks', {}).get(stream['tap_stream_id'])
    illegal_bk_keys = set(stream_state.keys()).difference(set(['replication_key', 'replication_key_value', 'version', 'last_replication_method']))
    if len(illegal_bk_keys) != 0:
        raise Exception("invalid keys found in state: {}".format(illegal_bk_keys))

    state = singer.write_bookmark(state, stream['tap_stream_id'], 'replication_key', replication_key)

    sync_common.send_schema_message(stream, [replication_key])
    state = incremental.sync_table(conn_config, stream, state, desired_columns, md_map)

    return state

def clear_state_on_replication_change(state, tap_stream_id, replication_key, replication_method):
    #user changed replication, nuke state
    last_replication_method = singer.get_bookmark(state, tap_stream_id, 'last_replication_method')
    if last_replication_method is not None and (replication_method != last_replication_method):
        state = singer.reset_stream(state, tap_stream_id)

    #key changed
    if replication_method == 'INCREMENTAL':
        if replication_key != singer.get_bookmark(state, tap_stream_id, 'replication_key'):
            state = singer.reset_stream(state, tap_stream_id)

    state = singer.write_bookmark(state, tap_stream_id, 'last_replication_method', replication_method)
    return state

def sync_method_for_streams(streams, state, default_replication_method):
    lookup = {}
    traditional_steams = []
    logical_streams = []

    for stream in streams:
        stream_metadata = metadata.to_map(stream['metadata'])
        replication_method = stream_metadata.get((), {}).get('replication-method', default_replication_method)
        replication_key = stream_metadata.get((), {}).get('replication-key')

        state = clear_state_on_replication_change(state, stream['tap_stream_id'], replication_key, replication_method)

        if replication_method not in set(['LOG_BASED', 'FULL_TABLE', 'INCREMENTAL']):
            raise Exception("Unrecognized replication_method {}".format(replication_method))

        md_map = metadata.to_map(stream['metadata'])
        desired_columns = [c for c in stream['schema']['properties'].keys() if sync_common.should_sync_column(md_map, c)]
        desired_columns.sort()

        if len(desired_columns) == 0:
            LOGGER.warning('There are no columns selected for stream %s, skipping it', stream['tap_stream_id'])
            continue

        if replication_method == 'LOG_BASED' and stream_metadata.get((), {}).get('is-view'):
            raise Exception('Logical Replication is NOT supported for views. Please change the replication method for {}'.format(stream['tap_stream_id']))

        if replication_method == 'FULL_TABLE':
            lookup[stream['tap_stream_id']] = 'full'
            traditional_steams.append(stream)
        elif replication_method == 'INCREMENTAL':
            lookup[stream['tap_stream_id']] = 'incremental'
            traditional_steams.append(stream)

        elif get_bookmark(state, stream['tap_stream_id'], 'xmin') and get_bookmark(state, stream['tap_stream_id'], 'lsn'):
            #finishing previously interrupted full-table (first stage of logical replication)
            lookup[stream['tap_stream_id']] = 'logical_initial_interrupted'
            traditional_steams.append(stream)

        #inconsistent state
        elif get_bookmark(state, stream['tap_stream_id'], 'xmin') and not get_bookmark(state, stream['tap_stream_id'], 'lsn'):
            raise Exception("Xmin found(%s) in state implying full-table replication but no lsn is present")

        elif not get_bookmark(state, stream['tap_stream_id'], 'xmin') and not get_bookmark(state, stream['tap_stream_id'], 'lsn'):
            #initial full-table phase of logical replication
            lookup[stream['tap_stream_id']] = 'logical_initial'
            traditional_steams.append(stream)

        else: #no xmin but we have an lsn
            #initial stage of logical replication(full-table) has been completed. moving onto pure logical replication
            lookup[stream['tap_stream_id']] = 'pure_logical'
            logical_streams.append(stream)

    return lookup, traditional_steams, logical_streams

def sync_traditional_stream(conn_config, stream, state, sync_method, end_lsn):
    LOGGER.info("Beginning sync of stream(%s) with sync method(%s)", stream['tap_stream_id'], sync_method)
    md_map = metadata.to_map(stream['metadata'])
    conn_config['dbname'] = md_map.get(()).get('database-name')
    desired_columns = [c for c in stream['schema']['properties'].keys() if sync_common.should_sync_column(md_map, c)]
    desired_columns.sort()

    if len(desired_columns) == 0:
        LOGGER.warning('There are no columns selected for stream %s, skipping it', stream['tap_stream_id'])
        return state

    register_type_adapters(conn_config)

    if sync_method == 'full':
        state = singer.set_currently_syncing(state, stream['tap_stream_id'])
        state = do_sync_full_table(conn_config, stream, state, desired_columns, md_map)
    elif sync_method == 'incremental':
        state = singer.set_currently_syncing(state, stream['tap_stream_id'])
        state = do_sync_incremental(conn_config, stream, state, desired_columns, md_map)
    elif sync_method == 'logical_initial':
        state = singer.set_currently_syncing(state, stream['tap_stream_id'])
        LOGGER.info("Performing initial full table sync")
        state = singer.write_bookmark(state, stream['tap_stream_id'], 'lsn', end_lsn)

        sync_common.send_schema_message(stream, [])
        state = full_table.sync_table(conn_config, stream, state, desired_columns, md_map)
        state = singer.write_bookmark(state, stream['tap_stream_id'], 'xmin', None)
    elif sync_method == 'logical_initial_interrupted':
        state = singer.set_currently_syncing(state, stream['tap_stream_id'])
        LOGGER.info("Initial stage of full table sync was interrupted. resuming...")
        sync_common.send_schema_message(stream, [])
        state = full_table.sync_table(conn_config, stream, state, desired_columns, md_map)
    else:
        raise Exception("unknown sync method {} for stream {}".format(sync_method, stream['tap_stream_id']))

    state = singer.set_currently_syncing(state, None)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state

def sync_logical_streams(conn_config, logical_streams, state, end_lsn):
    if logical_streams:
        LOGGER.info("Pure Logical Replication upto lsn %s for (%s)", end_lsn, list(map(lambda s: s['tap_stream_id'], logical_streams)))
        logical_streams = list(map(lambda s: logical_replication.add_automatic_properties(s, conn_config), logical_streams))

        state = logical_replication.sync_tables(conn_config, logical_streams, state, end_lsn)

    return state


def register_type_adapters(conn_config):
    with post_db.open_connection(conn_config) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            #citext[]
            cur.execute("SELECT typarray FROM pg_type where typname = 'citext'")
            citext_array_oid = cur.fetchone()
            if citext_array_oid:
                psycopg2.extensions.register_type(
                    psycopg2.extensions.new_array_type(
                        (citext_array_oid[0],), 'CITEXT[]', psycopg2.STRING))

            #bit[]
            cur.execute("SELECT typarray FROM pg_type where typname = 'bit'")
            bit_array_oid = cur.fetchone()[0]
            psycopg2.extensions.register_type(
                psycopg2.extensions.new_array_type(
                    (bit_array_oid,), 'BIT[]', psycopg2.STRING))


            #UUID[]
            cur.execute("SELECT typarray FROM pg_type where typname = 'uuid'")
            uuid_array_oid = cur.fetchone()[0]
            psycopg2.extensions.register_type(
                psycopg2.extensions.new_array_type(
                    (uuid_array_oid,), 'UUID[]', psycopg2.STRING))

            #money[]
            cur.execute("SELECT typarray FROM pg_type where typname = 'money'")
            money_array_oid = cur.fetchone()[0]
            psycopg2.extensions.register_type(
                psycopg2.extensions.new_array_type(
                    (money_array_oid,), 'MONEY[]', psycopg2.STRING))

            #json and jsonb
            psycopg2.extras.register_default_json(loads=lambda x: str(x))
            psycopg2.extras.register_default_jsonb(loads=lambda x: str(x))

            #enum[]'s
            cur.execute("SELECT distinct(t.typarray) FROM pg_type t JOIN pg_enum e ON t.oid = e.enumtypid")
            for oid in cur.fetchall():
                enum_oid = oid[0]
                psycopg2.extensions.register_type(
                    psycopg2.extensions.new_array_type(
                        (enum_oid,), 'ENUM_{}[]'.format(enum_oid), psycopg2.STRING))


def any_logical_streams(streams, default_replication_method):
    for stream in streams:
        stream_metadata = metadata.to_map(stream['metadata'])
        replication_method = stream_metadata.get((), {}).get('replication-method', default_replication_method)
        if replication_method == 'LOG_BASED':
            return True

    return False

def do_sync(conn_config, catalog, default_replication_method, state):
    currently_syncing = singer.get_currently_syncing(state)
    streams = list(filter(is_selected_via_metadata, catalog['streams']))
    streams.sort(key=lambda s: s['tap_stream_id'])
    LOGGER.info("Selected streams: %s ", list(map(lambda s: s['tap_stream_id'], streams)))
    if any_logical_streams(streams, default_replication_method):
        # Use of logical replication requires fetching an lsn
        end_lsn = logical_replication.fetch_current_lsn(conn_config)
        LOGGER.info("end_lsn = %s ", end_lsn)
    else:
        end_lsn = None

    sync_method_lookup, traditional_streams, logical_streams = sync_method_for_streams(streams, state, default_replication_method)

    if currently_syncing:
        LOGGER.info("found currently_syncing: %s", currently_syncing)
        currently_syncing_stream = list(filter(lambda s: s['tap_stream_id'] == currently_syncing, traditional_streams))
        if currently_syncing_stream is None:
            LOGGER.warning("unable to locate currently_syncing(%s) amongst selected traditional streams(%s). will ignore", currently_syncing, list(map(lambda s: s['tap_stream_id'], traditional_streams)))
        other_streams = list(filter(lambda s: s['tap_stream_id'] != currently_syncing, traditional_streams))
        traditional_streams = currently_syncing_stream + other_streams
    else:
        LOGGER.info("No streams marked as currently_syncing in state file")

    for stream in traditional_streams:
        state = sync_traditional_stream(conn_config, stream, state, sync_method_lookup[stream['tap_stream_id']], end_lsn)

    logical_streams.sort(key=lambda s: metadata.to_map(s['metadata']).get(()).get('database-name'))
    for dbname, streams in itertools.groupby(logical_streams, lambda s: metadata.to_map(s['metadata']).get(()).get('database-name')):
        conn_config['dbname'] = dbname
        state = sync_logical_streams(conn_config, list(streams), state, end_lsn)
    return state

def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    conn_config = {'host'     : args.config['host'],
                   'user'     : args.config['user'],
                   'password' : args.config['password'],
                   'port'     : args.config['port'],
                   'dbname'   : args.config['dbname'],
                   'filter_dbs' : args.config.get('filter_dbs'),
                   'filter_schemas' : args.config.get('filter_schemas'),
                   'debug_lsn' : args.config.get('debug_lsn') == 'true',
                   'logical_poll_total_seconds': float(args.config.get('logical_poll_total_seconds', 0))
                   }

    if args.config.get('ssl') == 'true':
        conn_config['sslmode'] = 'require'

    post_db.cursor_iter_size = int(args.config.get('itersize', '20000'))

    post_db.include_schemas_in_destination_stream_name = args.config.get('include_schemas_in_destination_stream_name', True)

    if args.discover:
        do_discovery(conn_config)
    elif args.properties or args.catalog:
        state = args.state
        do_sync(conn_config, args.catalog.to_dict() if args.catalog else args.properties, args.config.get('default_replication_method'), state)
    else:
        LOGGER.info("No properties were selected")

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
