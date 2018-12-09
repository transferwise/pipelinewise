import argparse
import json
import os
import time
import datetime


def log(message):
    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("{} - {}".format(st, message))


def load_json(path):
    with open(path) as fil:
        return json.load(fil)


def tablename_to_dict(table):
    ts = dict(enumerate(table.split('.')))
    return {
        'schema': ts.get(0, None),
        'name': ts.get(1, None),
        'temp_name': "{}_temp".format(ts.get(1, None))
    }


def parse_args(required_config_keys):
    '''Parse standard command-line args.

    --mysql-config      Config file
    --state             State file
    --properties        Properties file
    --snowflake-config  Snowflake Config file
    --transform-config  Transformations Config file
    --tables            Tables to sync. (Separated by comma)
    --export-dir        Directory to create temporary csv exports. Defaults to current work dir.

    Returns the parsed args object from argparse. For each argument that
    point to JSON files (mysql-config, state, properties, snowflake-config,
    transform-config), we will automatically load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--mysql-config',
        help='MySQL Config file',
        required=True)

    parser.add_argument(
        '--state',
        help='State file')

    parser.add_argument(
        '--properties',
        help='Properties file')

    parser.add_argument(
        '--snowflake-config',
        help='Snowflake Config discovery',
        required=True)

    parser.add_argument(
        '--transform-config',
        help='Transformations Config discovery')

    parser.add_argument(
        '--tables',
        help='Sync only specific tables',
        required=True)

    parser.add_argument(
        '--target-schema',
        help='Target schema in snowflake',
        required=True)

    parser.add_argument(
        '--export-dir',
        help='Temporary directory required for CSV exports')

    args = parser.parse_args()
    if args.mysql_config:
        args.mysql_config = load_json(args.mysql_config)
    if args.state:
        args.state = load_json(args.state)
    else:
        args.state = {}
    if args.properties:
        args.properties = load_json(args.properties)
    if args.snowflake_config:
        args.snowflake_config = load_json(args.snowflake_config)
    if args.transform_config:
        args.transform_config = load_json(args.transform_config)
    else:
        args.transform_config = {}
    if args.tables:
        args.tables = args.tables.split(',')
    if args.export_dir:
        args.export_dir = args.export_dir
    else:
        args.export_dir = os.path.realpath('.')

    check_config(args.mysql_config, required_config_keys['mysql'])
    check_config(args.snowflake_config, required_config_keys['snowflake'])

    return args


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception("Config is missing required keys: {}".format(missing_keys))
