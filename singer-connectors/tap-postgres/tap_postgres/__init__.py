import argparse
import itertools
import copy
import psycopg2
import psycopg2.extras
import psycopg2.extensions
import singer
import singer.schema

from singer import utils, metadata, get_bookmark
from singer.catalog import Catalog

import tap_postgres.db as post_db
import tap_postgres.sync_strategies.common as sync_common

from tap_postgres.sync_strategies import logical_replication
from tap_postgres.sync_strategies import full_table
from tap_postgres.sync_strategies import incremental
from tap_postgres.discovery_utils import discover_db
from tap_postgres.stream_utils import (
    dump_catalog, clear_state_on_replication_change,
    is_selected_via_metadata, refresh_streams_schema, any_logical_streams)

LOGGER = singer.get_logger('tap_postgres')

REQUIRED_CONFIG_KEYS = [
    'dbname',
    'host',
    'port',
    'user',
    'password'
]


def do_discovery(conn_config):
    """
    Run discovery mode to find all potential streams in the db cluster
    Args:
        conn_config: DB connection config

    Returns: list of discovered streams
    """
    with post_db.open_connection(conn_config) as conn:
        LOGGER.info("Discovering db %s", conn_config['dbname'])
        streams = discover_db(conn, conn_config.get('filter_schemas'))

    if len(streams) == 0:
        raise RuntimeError('0 tables were discovered across the entire cluster')

    dump_catalog(streams)
    return streams


def do_sync_full_table(conn_config, stream, state, desired_columns, md_map):
    """
    Runs full table sync
    """
    LOGGER.info("Stream %s is using full_table replication", stream['tap_stream_id'])
    sync_common.send_schema_message(stream, [])
    if md_map.get((), {}).get('is-view'):
        state = full_table.sync_view(conn_config, stream, state, desired_columns, md_map)
    else:
        state = full_table.sync_table(conn_config, stream, state, desired_columns, md_map)
    return state


# Possible state keys: replication_key, replication_key_value, version
def do_sync_incremental(conn_config, stream, state, desired_columns, md_map):
    """
    Runs Incremental sync
    """
    replication_key = md_map.get((), {}).get('replication-key')
    LOGGER.info("Stream %s is using incremental replication with replication key %s",
                stream['tap_stream_id'],
                replication_key)

    stream_state = state.get('bookmarks', {}).get(stream['tap_stream_id'])
    illegal_bk_keys = set(stream_state.keys()).difference(
        {'replication_key', 'replication_key_value', 'version', 'last_replication_method'})
    if len(illegal_bk_keys) != 0:
        raise Exception(f"invalid keys found in state: {illegal_bk_keys}")

    state = singer.write_bookmark(state, stream['tap_stream_id'], 'replication_key', replication_key)

    sync_common.send_schema_message(stream, [replication_key])
    state = incremental.sync_table(conn_config, stream, state, desired_columns, md_map)

    return state


def sync_method_for_streams(streams, state, default_replication_method):
    """
	Determines the replication method of each stream
	"""
    lookup = {}
    traditional_steams = []
    logical_streams = []

    for stream in streams:
        stream_metadata = metadata.to_map(stream['metadata'])
        replication_method = stream_metadata.get((), {}).get('replication-method', default_replication_method)
        replication_key = stream_metadata.get((), {}).get('replication-key')

        state = clear_state_on_replication_change(state, stream['tap_stream_id'], replication_key, replication_method)

        if replication_method not in {'LOG_BASED', 'FULL_TABLE', 'INCREMENTAL'}:
            raise Exception(f"Unrecognized replication_method {replication_method}")

        md_map = metadata.to_map(stream['metadata'])
        desired_columns = [c for c in stream['schema']['properties'].keys() if
                           sync_common.should_sync_column(md_map, c)]
        desired_columns.sort()

        if len(desired_columns) == 0:
            LOGGER.warning('There are no columns selected for stream %s, skipping it', stream['tap_stream_id'])
            continue

        if replication_method == 'LOG_BASED' and stream_metadata.get((), {}).get('is-view'):
            raise Exception(f'Logical Replication is NOT supported for views. ' \
                            f'Please change the replication method for {stream["tap_stream_id"]}')

        if replication_method == 'FULL_TABLE':
            lookup[stream['tap_stream_id']] = 'full'
            traditional_steams.append(stream)
        elif replication_method == 'INCREMENTAL':
            lookup[stream['tap_stream_id']] = 'incremental'
            traditional_steams.append(stream)

        elif get_bookmark(state, stream['tap_stream_id'], 'xmin') and \
                get_bookmark(state, stream['tap_stream_id'], 'lsn'):
            # finishing previously interrupted full-table (first stage of logical replication)
            lookup[stream['tap_stream_id']] = 'logical_initial_interrupted'
            traditional_steams.append(stream)

        # inconsistent state
        elif get_bookmark(state, stream['tap_stream_id'], 'xmin') and \
                not get_bookmark(state, stream['tap_stream_id'], 'lsn'):
            raise Exception("Xmin found(%s) in state implying full-table replication but no lsn is present")

        elif not get_bookmark(state, stream['tap_stream_id'], 'xmin') and \
                not get_bookmark(state, stream['tap_stream_id'], 'lsn'):
            # initial full-table phase of logical replication
            lookup[stream['tap_stream_id']] = 'logical_initial'
            traditional_steams.append(stream)

        else:  # no xmin but we have an lsn
            # initial stage of logical replication(full-table) has been completed. moving onto pure logical replication
            lookup[stream['tap_stream_id']] = 'pure_logical'
            logical_streams.append(stream)

    return lookup, traditional_steams, logical_streams


def sync_traditional_stream(conn_config, stream, state, sync_method, end_lsn):
    """
    Sync INCREMENTAL and FULL_TABLE streams
    """
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
        raise Exception(f"unknown sync method {sync_method} for stream {stream['tap_stream_id']}")

    state = singer.set_currently_syncing(state, None)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state


def sync_logical_streams(conn_config, logical_streams, state, end_lsn, state_file):
    """
    Sync streams that use LOG_BASED method
    """
    if logical_streams:
        LOGGER.info("Pure Logical Replication upto lsn %s for (%s)", end_lsn,
                    [s['tap_stream_id'] for s in logical_streams])

        logical_streams = [logical_replication.add_automatic_properties(
            s, conn_config.get('debug_lsn', False)) for s in logical_streams]

        # Remove LOG_BASED stream bookmarks from state if it has been de-selected
        # This is to avoid sending very old starting and flushing positions to source
        selected_streams = set()
        for stream in logical_streams:
            selected_streams.add(stream['tap_stream_id'])

        new_state = dict(currently_syncing=state['currently_syncing'], bookmarks={})

        for stream, bookmark in state['bookmarks'].items():
            if bookmark == {} or bookmark['last_replication_method'] != 'LOG_BASED' or stream in selected_streams:
                new_state['bookmarks'][stream] = bookmark
        state = new_state

        state = logical_replication.sync_tables(conn_config, logical_streams, state, end_lsn, state_file)

    return state


def register_type_adapters(conn_config):
    """
    //todo doc needed
    """
    with post_db.open_connection(conn_config) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # citext[]
            cur.execute("SELECT typarray FROM pg_type where typname = 'citext'")
            citext_array_oid = cur.fetchone()
            if citext_array_oid:
                psycopg2.extensions.register_type(
                    psycopg2.extensions.new_array_type(
                        (citext_array_oid[0],), 'CITEXT[]', psycopg2.STRING))

            # bit[]
            cur.execute("SELECT typarray FROM pg_type where typname = 'bit'")
            bit_array_oid = cur.fetchone()[0]
            psycopg2.extensions.register_type(
                psycopg2.extensions.new_array_type(
                    (bit_array_oid,), 'BIT[]', psycopg2.STRING))

            # UUID[]
            cur.execute("SELECT typarray FROM pg_type where typname = 'uuid'")
            uuid_array_oid = cur.fetchone()[0]
            psycopg2.extensions.register_type(
                psycopg2.extensions.new_array_type(
                    (uuid_array_oid,), 'UUID[]', psycopg2.STRING))

            # money[]
            cur.execute("SELECT typarray FROM pg_type where typname = 'money'")
            money_array_oid = cur.fetchone()[0]
            psycopg2.extensions.register_type(
                psycopg2.extensions.new_array_type(
                    (money_array_oid,), 'MONEY[]', psycopg2.STRING))

            # json and jsonb
            # pylint: disable=unnecessary-lambda
            psycopg2.extras.register_default_json(loads=lambda x: str(x))
            psycopg2.extras.register_default_jsonb(loads=lambda x: str(x))

            # enum[]'s
            cur.execute("SELECT distinct(t.typarray) FROM pg_type t JOIN pg_enum e ON t.oid = e.enumtypid")
            for oid in cur.fetchall():
                enum_oid = oid[0]
                psycopg2.extensions.register_type(
                    psycopg2.extensions.new_array_type(
                        (enum_oid,), f'ENUM_{enum_oid}[]', psycopg2.STRING))


def do_sync(conn_config, catalog, default_replication_method, state, state_file=None):
    """
    Orchestrates sync of all streams
    """
    currently_syncing = singer.get_currently_syncing(state)
    streams = list(filter(is_selected_via_metadata, catalog['streams']))
    streams.sort(key=lambda s: s['tap_stream_id'])
    LOGGER.info("Selected streams: %s ", [s['tap_stream_id'] for s in streams])
    if any_logical_streams(streams, default_replication_method):
        # Use of logical replication requires fetching an lsn
        end_lsn = logical_replication.fetch_current_lsn(conn_config)
        LOGGER.debug("end_lsn = %s ", end_lsn)
    else:
        end_lsn = None

    refresh_streams_schema(conn_config, streams)

    sync_method_lookup, traditional_streams, logical_streams = \
        sync_method_for_streams(streams, state, default_replication_method)

    if currently_syncing:
        LOGGER.debug("Found currently_syncing: %s", currently_syncing)

        currently_syncing_stream = list(filter(lambda s: s['tap_stream_id'] == currently_syncing, traditional_streams))

        if not currently_syncing_stream:
            LOGGER.warning("unable to locate currently_syncing(%s) amongst selected traditional streams(%s). "
                           "Will ignore",
                           currently_syncing,
                           {s['tap_stream_id'] for s in traditional_streams})

        other_streams = list(filter(lambda s: s['tap_stream_id'] != currently_syncing, traditional_streams))
        traditional_streams = currently_syncing_stream + other_streams
    else:
        LOGGER.info("No streams marked as currently_syncing in state file")

    for stream in traditional_streams:
        state = sync_traditional_stream(conn_config,
                                        stream,
                                        state,
                                        sync_method_lookup[stream['tap_stream_id']],
                                        end_lsn)

    logical_streams.sort(key=lambda s: metadata.to_map(s['metadata']).get(()).get('database-name'))
    for dbname, streams in itertools.groupby(logical_streams,
                                             lambda s: metadata.to_map(s['metadata']).get(()).get('database-name')):
        conn_config['dbname'] = dbname
        state = sync_logical_streams(conn_config, list(streams), state, end_lsn, state_file)
    return state


def parse_args(required_config_keys):
    # fork function to be able to grab path of state file
    """Parse standard command-line args.

    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:

    -c,--config     config file
    -s,--state      state file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file

    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '-s', '--state',
        help='state file')

    parser.add_argument(
        '-p', '--properties',
        help='Property selections: DEPRECATED, Please use --catalog instead')

    parser.add_argument(
        '--catalog',
        help='Catalog file')

    parser.add_argument(
        '-d', '--discover',
        action='store_true',
        help='Do schema discovery')

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = utils.load_json(args.config)
    if args.state:
        setattr(args, 'state_path', args.state)
        args.state_file = args.state
        args.state = utils.load_json(args.state)
    else:
        args.state_file = None
        args.state = {}
    if args.properties:
        setattr(args, 'properties_path', args.properties)
        args.properties = utils.load_json(args.properties)
    if args.catalog:
        setattr(args, 'catalog_path', args.catalog)
        args.catalog = Catalog.load(args.catalog)

    utils.check_config(args.config, required_config_keys)

    return args


def main_impl():
    """
    Main method
    """
    args = parse_args(REQUIRED_CONFIG_KEYS)

    limit = args.config.get('limit')
    conn_config = {
        # Required config keys
        'host': args.config['host'],
        'user': args.config['user'],
        'password': args.config['password'],
        'port': args.config['port'],
        'dbname': args.config['dbname'],

        # Optional config keys
        'tap_id': args.config.get('tap_id'),
        'filter_schemas': args.config.get('filter_schemas'),
        'debug_lsn': args.config.get('debug_lsn') == 'true',
        'max_run_seconds': args.config.get('max_run_seconds', 43200),
        'break_at_end_lsn': args.config.get('break_at_end_lsn', True),
        'logical_poll_total_seconds': float(args.config.get('logical_poll_total_seconds', 0)),
        'use_secondary': args.config.get('use_secondary', False),
        'limit': int(limit) if limit else None
    }

    if conn_config['use_secondary']:
        try:
            conn_config.update({
                # Host and Port are mandatory.
                'secondary_host': args.config['secondary_host'],
                'secondary_port': args.config['secondary_port'],
            })
        except KeyError as exc:
            raise ValueError(
                "When 'use_secondary' enabled 'secondary_host' and 'secondary_port' must be defined."
            ) from exc

    if args.config.get('ssl') == 'true':
        conn_config['sslmode'] = 'require'

    post_db.CURSOR_ITER_SIZE = int(args.config.get('itersize', post_db.CURSOR_ITER_SIZE))

    if args.discover:
        do_discovery(conn_config)
    elif args.properties or args.catalog:
        state = args.state
        state_file = args.state_file
        do_sync(conn_config, args.catalog.to_dict() if args.catalog else args.properties,
                args.config.get('default_replication_method'), state, state_file)
    else:
        LOGGER.info("No properties were selected")


def main():
    """
    main
    """
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
