import argparse
import singer

from singer import utils
from singer.catalog import Catalog

import tap_yugabyte.db as post_db

from tap_yugabyte.discovery_utils import discover_db
from tap_yugabyte.stream_utils import dump_catalog

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
        raise NotImplementedError('Sync mode is not implemented yet, only --discover is supported')
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
