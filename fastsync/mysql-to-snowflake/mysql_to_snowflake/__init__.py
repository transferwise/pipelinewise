#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,too-many-branches,invalid-name,duplicate-code,too-many-statements

import datetime
import collections
import itertools
from itertools import dropwhile
import copy
import time
import os

import pendulum

import pymysql

import singer
import singer.metrics as metrics
import singer.schema

from singer import bookmarks
from singer import metadata
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

import mysql_to_snowflake.utils

import mysql_to_snowflake.mysql.sync_strategies.binlog as binlog
import mysql_to_snowflake.mysql.sync_strategies.common as common
import mysql_to_snowflake.mysql.sync_strategies.fast_full_table as fast_full_table
import mysql_to_snowflake.mysql.sync_strategies.full_table as full_table
import mysql_to_snowflake.mysql.sync_strategies.incremental as incremental
from mysql_to_snowflake.snowflake.db_sync import DbSync

from mysql_to_snowflake.mysql.connection import connect_with_backoff, MySQLConnection


Column = collections.namedtuple('Column', [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "column_type",
    "column_key"])

REQUIRED_CONFIG_KEYS = {
    'mysql': [
        'host',
        'port',
        'user',
        'password'
    ],
    'snowflake': [
        'account',
        'dbname',
        'warehouse',
        'user',
        'password',
        's3_bucket',
        'aws_access_key_id',
        'aws_secret_access_key'
    ]
}

LOGGER = singer.get_logger()

pymysql.converters.conversions[pendulum.Pendulum] = pymysql.converters.escape_datetime


STRING_TYPES = set([
    'char',
    'enum',
    'longtext',
    'mediumtext',
    'text',
    'varchar'
])

BYTES_FOR_INTEGER_TYPE = {
    'tinyint': 1,
    'smallint': 2,
    'mediumint': 3,
    'int': 4,
    'bigint': 8
}

FLOAT_TYPES = set(['float', 'double'])

DATETIME_TYPES = set(['datetime', 'timestamp', 'date', 'time'])


def schema_for_column(c):
    '''Returns the Schema object for the given Column.'''
    data_type = c.data_type.lower()
    column_type = c.column_type.lower()

    inclusion = 'available'
    # We want to automatically include all primary key columns
    if c.column_key.lower() == 'pri':
        inclusion = 'automatic'

    result = Schema(inclusion=inclusion)

    if data_type == 'bit' or column_type.startswith('tinyint(1)'):
        result.type = ['null', 'boolean']

    elif data_type in BYTES_FOR_INTEGER_TYPE:
        result.type = ['null', 'integer']
        bits = BYTES_FOR_INTEGER_TYPE[data_type] * 8
        if 'unsigned' in c.column_type:
            result.minimum = 0
            result.maximum = 2 ** bits - 1
        else:
            result.minimum = 0 - 2 ** (bits - 1)
            result.maximum = 2 ** (bits - 1) - 1

    elif data_type in FLOAT_TYPES:
        result.type = ['null', 'number']

    elif data_type == 'decimal':
        result.type = ['null', 'number']
        result.multipleOf = 10 ** (0 - c.numeric_scale)
        return result

    elif data_type in STRING_TYPES:
        result.type = ['null', 'string']
        result.maxLength = c.character_maximum_length

    elif data_type in DATETIME_TYPES:
        result.type = ['null', 'string']
        result.format = 'date-time'

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        description='Unsupported column type {}'.format(column_type))
    return result


def create_column_metadata(cols):
    mdata = {}
    mdata = metadata.write(mdata, (), 'selected-by-default', False)
    for c in cols:
        schema = schema_for_column(c)
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'selected-by-default',
                               schema.inclusion != 'unsupported')
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'sql-datatype',
                               c.column_type.lower())

    return metadata.to_list(mdata)


def discover_catalog(mysql_conn, config):
    '''Returns a Catalog describing the structure of the database.'''


    filter_dbs_config = config.get('filter_dbs')


    if filter_dbs_config:
        filter_dbs_clause = ",".join(["'{}'".format(db)
                                         for db in filter_dbs_config.split(",")])

        table_schema_clause = "WHERE table_schema IN ({})".format(filter_dbs_clause)
    else:
        table_schema_clause = """
        WHERE table_schema NOT IN (
        'information_schema',
        'performance_schema',
        'mysql'
        )"""

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("""
            SELECT table_schema,
                   table_name,
                   table_type,
                   table_rows
                FROM information_schema.tables
                {}
            """.format(table_schema_clause))

            table_info = {}

            for (db, table, table_type, rows) in cur.fetchall():
                if db not in table_info:
                    table_info[db] = {}

                table_info[db][table] = {
                    'row_count': rows,
                    'is_view': table_type == 'VIEW'
                }

            cur.execute("""
                SELECT table_schema,
                       table_name,
                       column_name,
                       data_type,
                       character_maximum_length,
                       numeric_precision,
                       numeric_scale,
                       column_type,
                       column_key
                    FROM information_schema.columns
                    {}
                    ORDER BY table_schema, table_name
            """.format(table_schema_clause))

            columns = []
            rec = cur.fetchone()
            while rec is not None:
                columns.append(Column(*rec))
                rec = cur.fetchone()

            entries = []
            for (k, cols) in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
                cols = list(cols)
                (table_schema, table_name) = k
                schema = Schema(type='object',
                                properties={c.column_name: schema_for_column(c) for c in cols})
                md = create_column_metadata(cols)
                md_map = metadata.to_map(md)

                md_map = metadata.write(md_map,
                                        (),
                                        'database-name',
                                        table_schema)

                is_view = table_info[table_schema][table_name]['is_view']

                if table_schema in table_info and table_name in table_info[table_schema]:
                    row_count = table_info[table_schema][table_name].get('row_count')

                    if row_count is not None:
                        md_map = metadata.write(md_map,
                                                (),
                                                'row-count',
                                                row_count)

                    md_map = metadata.write(md_map,
                                            (),
                                            'is-view',
                                            is_view)

                column_is_key_prop = lambda c, s: (
                    c.column_key == 'PRI' and
                    s.properties[c.column_name].inclusion != 'unsupported'
                )

                key_properties = [c.column_name for c in cols if column_is_key_prop(c, schema)]

                if not is_view:
                    md_map = metadata.write(md_map,
                                            (),
                                            'table-key-properties',
                                            key_properties)

                entry = CatalogEntry(
                    table=table_name,
                    stream=table_name,
                    metadata=metadata.to_list(md_map),
                    tap_stream_id=common.generate_tap_stream_id(table_schema, table_name),
                    schema=schema)

                entries.append(entry)

    return Catalog(entries)


def do_discover(mysql_conn, config):
    discover_catalog(mysql_conn, config).dump()


# TODO: Maybe put in a singer-db-utils library.
def desired_columns(selected, table_schema):

    '''Return the set of column names we need to include in the SELECT.

    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    '''
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in table_schema.properties.items():
        all_columns.add(column)
        inclusion = column_schema.inclusion
        if inclusion == 'automatic':
            automatic.add(column)
        elif inclusion == 'available':
            available.add(column)
        elif inclusion == 'unsupported':
            unsupported.add(column)
        else:
            raise Exception('Unknown inclusion ' + inclusion)

    selected_but_unsupported = selected.intersection(unsupported)
    if selected_but_unsupported:
        LOGGER.warning(
            'Columns %s were selected but are not supported. Skipping them.',
            selected_but_unsupported)

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning(
            'Columns %s were selected but do not exist.',
            selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning(
            'Columns %s are primary keys but were not selected. Adding them.',
            not_selected_but_automatic)

    return selected.intersection(available).union(automatic)


def log_engine(mysql_conn, catalog_entry):
    is_view = common.get_is_view(catalog_entry)
    database_name = common.get_database_name(catalog_entry)

    if is_view:
        LOGGER.info("Beginning sync for view %s.%s", database_name, catalog_entry.table)
    else:
        with connect_with_backoff(mysql_conn) as open_conn:
            with open_conn.cursor() as cur:
                cur.execute("""
                    SELECT engine
                      FROM information_schema.tables
                     WHERE table_schema = %s
                       AND table_name   = %s
                """, (database_name, catalog_entry.table))

                row = cur.fetchone()

                if row:
                    LOGGER.info("Beginning sync for %s table %s.%s",
                                row[0],
                                database_name,
                                catalog_entry.table)


def is_valid_currently_syncing_stream(selected_stream, state):
    stream_metadata = metadata.to_map(selected_stream.metadata)
    replication_method = stream_metadata.get((), {}).get('replication-method')

    if replication_method != 'LOG_BASED':
        return True

    if replication_method == 'LOG_BASED' and binlog_stream_requires_historical(selected_stream, state):
        return True

    return False

def binlog_stream_requires_historical(catalog_entry, state):
    log_file = singer.get_bookmark(state,
                                   catalog_entry.tap_stream_id,
                                   'log_file')

    log_pos = singer.get_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'log_pos')

    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values')

    last_pk_fetched = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'last_pk_fetched')

    if (log_file and log_pos) and (not max_pk_values and not last_pk_fetched):
        return False

    return True


def resolve_catalog(discovered_catalog, streams_to_sync):
    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams_to_sync:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        discovered_table = discovered_catalog.get_stream(catalog_entry.tap_stream_id)
        database_name = common.get_database_name(catalog_entry)

        if not discovered_table:
            LOGGER.warning('Database %s table %s was selected but does not exist',
                           database_name, catalog_entry.table)
            continue

        selected = {k for k, v in catalog_entry.schema.properties.items()
                    if common.property_is_selected(catalog_entry, k) or k == replication_key}

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema)

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            metadata=catalog_entry.metadata,
            stream=catalog_entry.stream,
            table=catalog_entry.table,
            schema=Schema(
                type='object',
                properties={col: discovered_table.schema.properties[col]
                            for col in columns}
            )
        ))

    return result


def get_streams(mysql_conn, catalog, config):
    '''Returns the Catalog of data we're going to sync for all SELECT-based
    streams (i.e. INCREMENTAL, FULL_TABLE, and LOG_BASED)

    Using the Catalog provided from the input file, this function will return a
    Catalog representing exactly which tables and columns that will be emitted
    by SELECT-based syncs. This is achieved by comparing the input Catalog to a
    freshly discovered Catalog to determine the resulting Catalog.

    The resulting Catalog will include the following any streams marked as
    "selected" that currently exist in the database. Columns marked as "selected"
    and those labled "automatic" (e.g. primary keys and replication keys) will be
    included.
    '''
    discovered = discover_catalog(mysql_conn, config)

    # Filter catalog to include only selected streams
    selected_streams = list(filter(lambda s: common.stream_is_selected(s), catalog.streams))

    return resolve_catalog(discovered, selected_streams)


def write_schema_message(catalog_entry, bookmark_properties=[]):
    key_properties = common.get_key_properties(catalog_entry)

    singer.write_message(singer.SchemaMessage(
        stream=catalog_entry.stream,
        schema=catalog_entry.schema.to_dict(),
        key_properties=key_properties,
        bookmark_properties=bookmark_properties
    ))


def do_sync_fastsync(mysql_conn, catalog_entry, state, columns, stream_version, args):
    fast_full_table.table_to_csv(mysql_conn, catalog_entry, state, columns, stream_version, args)

def do_sync_incremental(mysql_conn, catalog_entry, state, columns, args):
    LOGGER.info("Stream %s is using incremental replication", catalog_entry.stream)

    md_map = metadata.to_map(catalog_entry.metadata)
    replication_key = md_map.get((), {}).get('replication-key')

    if not replication_key:
        raise Exception("Cannot use INCREMENTAL replication for table ({}) without a replication key.".format(catalog_entry.stream))

    write_schema_message(catalog_entry=catalog_entry,
                         bookmark_properties=[replication_key])

    incremental.sync_table(mysql_conn, catalog_entry, state, columns)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync_historical_binlog(mysql_conn, catalog_entry, state, columns, args):
    binlog.verify_binlog_config(mysql_conn)

    is_view = common.get_is_view(catalog_entry)
    key_properties = common.get_key_properties(catalog_entry)

    if is_view:
        raise Exception("Unable to replicate stream({}) with binlog because it is a view.".format(catalog_entry.stream))

    log_file = singer.get_bookmark(state,
                                   catalog_entry.tap_stream_id,
                                   'log_file')

    log_pos = singer.get_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'log_pos')

    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values')

    last_pk_fetched = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'last_pk_fetched')

    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

    if log_file and log_pos and max_pk_values:
        LOGGER.info("Resuming initial full table sync for LOG_BASED stream %s", catalog_entry.tap_stream_id)
        do_sync_fastsync(mysql_conn, catalog_entry, state, columns, stream_version, args)

    else:
        LOGGER.info("Performing initial full table sync for LOG_BASED stream %s", catalog_entry.tap_stream_id)

        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'initial_binlog_complete',
                                      False)

        current_log_file, current_log_pos = binlog.fetch_current_log_file_and_pos(mysql_conn)
        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'version',
                                      stream_version)

        if full_table.pks_are_auto_incrementing(mysql_conn, catalog_entry):
            # We must save log_file and log_pos across FULL_TABLE syncs when using
            # an incrementing PK
            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_file',
                                          current_log_file)

            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_pos',
                                          current_log_pos)

            do_sync_fastsync(mysql_conn, catalog_entry, state, columns, stream_version, args)

        else:
            do_sync_fastsync(mysql_conn, catalog_entry, state, columns, stream_version, args)
            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_file',
                                          current_log_file)

            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_pos',
                                          current_log_pos)


def do_sync_full_table(mysql_conn, catalog_entry, state, columns, args):
    LOGGER.info("Stream %s is using full table replication", catalog_entry.stream)
    key_properties = common.get_key_properties(catalog_entry)

    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

    do_sync_fastsync(mysql_conn, catalog_entry, state, columns, stream_version, args)

    # Prefer initial_full_table_complete going forward
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'version')

    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'initial_full_table_complete',
                                  True)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def snowflake_load_csv(args, catalog_entry, columns):
    schema_message = singer.SchemaMessage(
        stream=catalog_entry.stream,
        schema=catalog_entry.schema.to_dict(),
        key_properties=common.get_key_properties(catalog_entry),
        bookmark_properties=[]
    ).asdict()

    snowflake = DbSync(args.snowflake_config, schema_message, columns)
    snowflake_conn = snowflake.open_connection()

    database_name = common.get_database_name(catalog_entry)
    table_name = catalog_entry.table
    ddl = snowflake.create_table_query(False)
    snowflake.query(ddl)

    database_name = common.get_database_name(catalog_entry)
    table_name = catalog_entry.table
    snowflake.load_csv("pipelinewise_{}_{}.csv.gz".format(database_name, table_name), 'lot of')

def sync_streams(mysql_conn, args, catalog, state):
    for catalog_entry in catalog.streams:
        # Skip table if restricted by limit_tables
        if args.limit_tables and catalog_entry.table not in args.limit_tables:
            continue

        columns = list(catalog_entry.schema.properties.keys())

        if not columns:
            LOGGER.warning('There are no columns selected for stream %s, skipping it.', catalog_entry.stream)
            continue

        state = singer.set_currently_syncing(state, catalog_entry.tap_stream_id)

        # Emit a state message to indicate that we've started this stream
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        md_map = metadata.to_map(catalog_entry.metadata)

        replication_method = md_map.get((), {}).get('replication-method')

        database_name = common.get_database_name(catalog_entry)

        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = database_name
            timer.tags['table'] = catalog_entry.table

            log_engine(mysql_conn, catalog_entry)

            if replication_method == 'INCREMENTAL':
                do_sync_incremental(mysql_conn, catalog_entry, state, columns, args)
            elif replication_method == 'LOG_BASED':
                do_sync_historical_binlog(mysql_conn, catalog_entry, state, columns, args)
            elif replication_method == 'FULL_TABLE':
                do_sync_full_table(mysql_conn, catalog_entry, state, columns, args)
            else:
                raise Exception("only INCREMENTAL, LOG_BASED, and FULL TABLE replication methods are supported {}".format(replication_method))

        # CSV Export finished, load into snowflake
        snowflake_load_csv(args, catalog_entry, columns)

    state = singer.set_currently_syncing(state, None)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync(args, catalog, state):
    mysql_conn = MySQLConnection(args.mysql_config)
    log_server_params(mysql_conn)

    catalog = get_streams(mysql_conn, catalog, args.mysql_config)
    sync_streams(mysql_conn, args, catalog, state)

def log_server_params(mysql_conn):
    with connect_with_backoff(mysql_conn) as open_conn:
        try:
            with open_conn.cursor() as cur:
                cur.execute('''
                SELECT VERSION() as version,
                       @@session.wait_timeout as wait_timeout,
                       @@session.innodb_lock_wait_timeout as innodb_lock_wait_timeout,
                       @@session.max_allowed_packet as max_allowed_packet,
                       @@session.interactive_timeout as interactive_timeout''')
                row = cur.fetchone()
                LOGGER.info('Server Parameters: ' +
                            'version: %s, ' +
                            'wait_timeout: %s, ' +
                            'innodb_lock_wait_timeout: %s, ' +
                            'max_allowed_packet: %s, ' +
                            'interactive_timeout: %s',
                            *row)
            with open_conn.cursor() as cur:
                cur.execute('''
                show session status where Variable_name IN ('Ssl_version', 'Ssl_cipher')''')
                rows = cur.fetchall()
                mapped_row = {k:v for (k,v) in [(r[0], r[1]) for r in rows]}
                LOGGER.info('Server SSL Parameters (blank means SSL is not active): ' +
                            '[ssl_version: %s], ' +
                            '[ssl_cipher: %s]',
                            mapped_row['Ssl_version'],
                            mapped_row['Ssl_cipher'])

        except pymysql.err.InternalError as e:
            LOGGER.warning("Encountered error checking server params. Error: (%s) %s", *e.args)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)


    if args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = args.state or {}
        do_sync(args, catalog, state)
    else:
        LOGGER.info("No properties were selected")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc