import argparse
import copy
import singer

from singer import utils, metadata
from singer.catalog import Catalog

import tap_yugabyte.db as post_db
import tap_yugabyte.sync_strategies.common as sync_common

from tap_yugabyte.sync_strategies import full_table
from tap_yugabyte.sync_strategies import incremental
from tap_yugabyte.discovery_utils import discover_db
from tap_yugabyte.stream_utils import (
    dump_catalog, clear_state_on_replication_change,
    is_selected_via_metadata, refresh_streams_schema)

LOGGER = singer.get_logger('tap_yugabyte')

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
    """Run a FULL_TABLE sync for one stream."""
    LOGGER.info("Stream %s is using full_table replication", stream['tap_stream_id'])
    sync_common.send_schema_message(stream, [])
    if md_map.get((), {}).get('is-view'):
        state = full_table.sync_view(conn_config, stream, state, desired_columns, md_map)
    else:
        state = full_table.sync_table(conn_config, stream, state, desired_columns, md_map)
    return state


def do_sync_incremental(conn_config, stream, state, desired_columns, md_map):
    """Run an INCREMENTAL sync for one stream."""
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


def sync_stream(conn_config, stream, state, default_replication_method):
    """Sync one already-validated FULL_TABLE or INCREMENTAL stream and flush the resulting state."""
    md_map = metadata.to_map(stream['metadata'])
    replication_method = md_map.get((), {}).get('replication-method', default_replication_method)
    desired_columns = [c for c in stream['schema']['properties'].keys() if
                       sync_common.should_sync_column(md_map, c)]
    desired_columns.sort()

    if len(desired_columns) == 0:
        LOGGER.warning('There are no columns selected for stream %s, skipping it', stream['tap_stream_id'])
        return state

    state = singer.set_currently_syncing(state, stream['tap_stream_id'])
    if replication_method == 'INCREMENTAL':
        state = do_sync_incremental(conn_config, stream, state, desired_columns, md_map)
    else:
        state = do_sync_full_table(conn_config, stream, state, desired_columns, md_map)
    state = singer.set_currently_syncing(state, None)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state


def do_sync(conn_config, catalog, default_replication_method, state):
    """Orchestrate sync of every selected stream via FULL_TABLE or INCREMENTAL.

    Any other selected replication method raises NotImplementedError before
    any stream is synced.
    """
    currently_syncing = singer.get_currently_syncing(state)
    streams = list(filter(is_selected_via_metadata, catalog['streams']))
    streams.sort(key=lambda s: s['tap_stream_id'])
    LOGGER.info("Selected streams: %s ", [s['tap_stream_id'] for s in streams])

    refresh_streams_schema(conn_config, streams)

    for stream in streams:
        stream_metadata = metadata.to_map(stream['metadata'])
        replication_method = stream_metadata.get((), {}).get('replication-method', default_replication_method)
        replication_key = stream_metadata.get((), {}).get('replication-key')
        state = clear_state_on_replication_change(
            state, stream['tap_stream_id'], replication_key, replication_method)

        if replication_method not in {'FULL_TABLE', 'INCREMENTAL'}:
            raise NotImplementedError(
                f"replication method {replication_method} is not yet implemented for tap-yugabyte, "
                f"stream {stream['tap_stream_id']}")

    if currently_syncing:
        LOGGER.debug("Found currently_syncing: %s", currently_syncing)
        currently_syncing_stream = [s for s in streams if s['tap_stream_id'] == currently_syncing]
        other_streams = [s for s in streams if s['tap_stream_id'] != currently_syncing]
        streams = currently_syncing_stream + other_streams
    else:
        LOGGER.info("No streams marked as currently_syncing in state file")

    for stream in streams:
        state = sync_stream(conn_config, stream, state, default_replication_method)

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
        'limit': int(limit) if limit else None
    }

    if args.config.get('ssl') == 'true':
        conn_config['sslmode'] = 'require'

    post_db.CURSOR_ITER_SIZE = int(args.config.get('itersize', post_db.CURSOR_ITER_SIZE))

    if args.discover:
        do_discovery(conn_config)
    elif args.properties or args.catalog:
        state = args.state
        do_sync(conn_config, args.catalog.to_dict() if args.catalog else args.properties,
                args.config.get('default_replication_method'), state)
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
