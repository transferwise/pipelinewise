# pylint: disable=missing-docstring,too-many-locals
import copy
import pymysql
import singer

from typing import Dict
from singer import metadata, get_logger
from singer import metrics
from singer.catalog import Catalog

from tap_mysql.connection import connect_with_backoff, MySQLConnection, fetch_server_id, MYSQL_ENGINE
from tap_mysql.discover_utils import discover_catalog, resolve_catalog
from tap_mysql.stream_utils import write_schema_message
from tap_mysql.sync_strategies import binlog
from tap_mysql.sync_strategies import common
from tap_mysql.sync_strategies import full_table
from tap_mysql.sync_strategies import incremental

LOGGER = get_logger('tap_mysql')

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password'
]


def do_discover(mysql_conn, config):
    discover_catalog(mysql_conn, config.get('filter_dbs')).dump()


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

    gtid = singer.get_bookmark(state,
                               catalog_entry.tap_stream_id,
                               'gtid')

    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values')

    last_pk_fetched = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'last_pk_fetched')

    if ((log_file and log_pos) or gtid) and (not max_pk_values and not last_pk_fetched):
        return False

    return True


def get_non_binlog_streams(mysql_conn, catalog, config, state):
    """
    Returns the Catalog of data we're going to sync for all SELECT-based
    streams (i.e. INCREMENTAL, FULL_TABLE, and LOG_BASED that require a historical
    sync). LOG_BASED streams that require a historical sync are inferred from lack
    of any state.

    Using the Catalog provided from the input file, this function will return a
    Catalog representing exactly which tables and columns that will be emitted
    by SELECT-based syncs. This is achieved by comparing the input Catalog to a
    freshly discovered Catalog to determine the resulting Catalog.

    The resulting Catalog will include the following any streams marked as
    "selected" that currently exist in the database. Columns marked as "selected"
    and those labeled "automatic" (e.g. primary keys and replication keys) will be
    included. Streams will be prioritized in the following order:
      1. currently_syncing if it is SELECT-based
      2. any streams that do not have state
      3. any streams that do not have a replication method of LOG_BASED

    """
    discovered = discover_catalog(mysql_conn, config.get('filter_dbs'))

    # Filter catalog to include only selected streams
    selected_streams = list(filter(common.stream_is_selected, catalog.streams))
    streams_with_state = []
    streams_without_state = []

    for stream in selected_streams:
        stream_metadata = metadata.to_map(stream.metadata)
        replication_method = stream_metadata.get((), {}).get('replication-method')
        stream_state = state.get('bookmarks', {}).get(stream.tap_stream_id)

        if not stream_state:
            if replication_method == 'LOG_BASED':
                LOGGER.info("LOG_BASED stream %s requires full historical sync", stream.tap_stream_id)

            streams_without_state.append(stream)
        elif stream_state and replication_method == 'LOG_BASED' and binlog_stream_requires_historical(stream, state):
            is_view = common.get_is_view(stream)

            if is_view:
                raise Exception(
                    f"Unable to replicate stream({stream.stream}) with binlog because it is a view.")

            LOGGER.info("LOG_BASED stream %s will resume its historical sync", stream.tap_stream_id)

            streams_with_state.append(stream)
        elif stream_state and replication_method != 'LOG_BASED':
            streams_with_state.append(stream)

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)

    # prioritize streams that have not been processed
    ordered_streams = streams_without_state + streams_with_state

    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s.tap_stream_id == currently_syncing and is_valid_currently_syncing_stream(s, state),
            streams_with_state))

        non_currently_syncing_streams = list(filter(lambda s: s.tap_stream_id != currently_syncing, ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        # prioritize streams that have not been processed
        streams_to_sync = ordered_streams

    return resolve_catalog(discovered, streams_to_sync)


def get_binlog_streams(mysql_conn, catalog, config, state):
    discovered = discover_catalog(mysql_conn, config.get('filter_dbs'))

    selected_streams = list(filter(common.stream_is_selected, catalog.streams))
    binlog_streams = []

    for stream in selected_streams:
        stream_metadata = metadata.to_map(stream.metadata)
        replication_method = stream_metadata.get((), {}).get('replication-method')

        if replication_method == 'LOG_BASED' and not binlog_stream_requires_historical(stream, state):
            binlog_streams.append(stream)

    return resolve_catalog(discovered, binlog_streams)


def do_sync_incremental(mysql_conn, catalog_entry, state, columns):
    LOGGER.info("Stream %s is using incremental replication", catalog_entry.stream)

    md_map = metadata.to_map(catalog_entry.metadata)
    replication_key = md_map.get((), {}).get('replication-key')

    if not replication_key:
        raise Exception(
            f"Cannot use INCREMENTAL replication for table ({catalog_entry.stream}) without a replication key.")

    write_schema_message(catalog_entry=catalog_entry,
                         bookmark_properties=[replication_key])

    incremental.sync_table(mysql_conn, catalog_entry, state, columns)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


# pylint: disable=too-many-arguments
def do_sync_historical_binlog(mysql_conn, catalog_entry, state, columns, use_gtid: bool, engine: str):
    binlog.verify_binlog_config(mysql_conn)

    if use_gtid and engine == MYSQL_ENGINE:
        binlog.verify_gtid_config(mysql_conn)

    is_view = common.get_is_view(catalog_entry)

    if is_view:
        raise Exception(f"Unable to replicate stream({catalog_entry.stream}) with binlog because it is a view.")

    log_file = singer.get_bookmark(state,
                                   catalog_entry.tap_stream_id,
                                   'log_file')

    log_pos = singer.get_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'log_pos')

    gtid = None
    if use_gtid:
        gtid = singer.get_bookmark(state,
                                   catalog_entry.tap_stream_id,
                                   'gtid')

    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values')

    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

    if max_pk_values and ((use_gtid and gtid) or (log_file and log_pos)):
        LOGGER.info("Resuming initial full table sync for LOG_BASED stream %s", catalog_entry.tap_stream_id)
        full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)
    else:
        LOGGER.info("Performing initial full table sync for LOG_BASED stream %s", catalog_entry.tap_stream_id)

        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'initial_binlog_complete',
                                      False)

        current_log_file, current_log_pos = binlog.fetch_current_log_file_and_pos(mysql_conn)

        current_gtid = None
        if use_gtid:
            current_gtid = binlog.fetch_current_gtid_pos(mysql_conn, engine)

        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'version',
                                      stream_version)

        if full_table.pks_are_auto_incrementing(mysql_conn, catalog_entry):
            # We must save log_file, log_pos, gtid across FULL_TABLE syncs when using
            # an incrementing PK
            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_file',
                                          current_log_file)

            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_pos',
                                          current_log_pos)

            if current_gtid:
                state = singer.write_bookmark(state,
                                              catalog_entry.tap_stream_id,
                                              'gtid',
                                              current_gtid)

            full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)

        else:
            full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)
            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_file',
                                          current_log_file)

            state = singer.write_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'log_pos',
                                          current_log_pos)

            if current_gtid:
                state = singer.write_bookmark(state,
                                              catalog_entry.tap_stream_id,
                                              'gtid',
                                              current_gtid)


def do_sync_full_table(mysql_conn, catalog_entry, state, columns):
    LOGGER.info("Stream %s is using full table replication", catalog_entry.stream)

    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

    full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)

    # Prefer initial_full_table_complete going forward
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'version')

    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'initial_full_table_complete',
                                  True)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def sync_non_binlog_streams(mysql_conn, non_binlog_catalog, state, use_gtid, engine):
    for catalog_entry in non_binlog_catalog.streams:
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
                do_sync_incremental(mysql_conn, catalog_entry, state, columns)
            elif replication_method == 'LOG_BASED':
                do_sync_historical_binlog(mysql_conn, catalog_entry, state, columns, use_gtid, engine)
            elif replication_method == 'FULL_TABLE':
                do_sync_full_table(mysql_conn, catalog_entry, state, columns)
            else:
                raise Exception("only INCREMENTAL, LOG_BASED, and FULL TABLE replication methods are supported")

    state = singer.set_currently_syncing(state, None)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def sync_binlog_streams(mysql_conn, binlog_catalog, config, state):

    if binlog_catalog.streams:
        for stream in binlog_catalog.streams:
            write_schema_message(stream)

        with metrics.job_timer('sync_binlog'):
            binlog_streams_map = binlog.generate_streams_map(binlog_catalog.streams)
            binlog.sync_binlog_stream(mysql_conn, config, binlog_streams_map, state)


def do_sync(mysql_conn, config, catalog, state):

    config['use_gtid'] = config.get('use_gtid', False)
    config['engine'] = config.get('engine', MYSQL_ENGINE).lower()

    non_binlog_catalog = get_non_binlog_streams(mysql_conn, catalog, config, state)
    binlog_catalog = get_binlog_streams(mysql_conn, catalog, config, state)

    sync_non_binlog_streams(mysql_conn,
                            non_binlog_catalog,
                            state,
                            config['use_gtid'],
                            config['engine']
                            )
    sync_binlog_streams(mysql_conn, binlog_catalog, config, state)


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
                mapped_row = {r[0]: r[1] for r in rows}
                LOGGER.info(
                    'Server SSL Parameters(blank means SSL is not active): [ssl_version: %s], [ssl_cipher: %s]',
                    mapped_row['Ssl_version'], mapped_row['Ssl_cipher'])

        except pymysql.err.InternalError as exc:
            LOGGER.warning("Encountered error checking server params. Error: (%s) %s", *exc.args)


def main_impl():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    mysql_conn = MySQLConnection(args.config)
    log_server_params(mysql_conn)

    if args.discover:
        do_discover(mysql_conn, args.config)
    elif args.catalog:
        state = args.state or {}
        do_sync(mysql_conn, args.config, args.catalog, state)
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = args.state or {}
        do_sync(mysql_conn, args.config, catalog, state)
    else:
        raise ValueError("Hmm I don't know what to do! Neither discovery nor sync mode was selected.")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
