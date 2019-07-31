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


def save_dict_to_json(path, data):
    log("Saving new state file to {}".format(path))
    with open(path, "w") as fil:
        fil.write(json.dumps(data))


def tablename_to_dict(table):
    ts = dict(enumerate(table.split('.')))
    return {
        'schema': ts.get(0, None),
        'name': ts.get(1, None),
        'temp_name': "{}_temp".format(ts.get(1, None))
    }


def get_tables_from_properties(properties):
    """Get list of enabled tables with schema names from properties json
    The outhput is useful to generate list of tables to sync
    """
    tables = []

    for stream in properties.get("streams", tables):
        metadata = stream.get("metadata", [])
        table_name = stream.get("table_name")

        table_meta = next((i for i in metadata if type(i) == dict and len(i.get("breadcrumb", [])) == 0), {}).get("metadata")
        db_name = table_meta.get("database-name")
        selected = table_meta.get("selected")

        if table_name and db_name and selected:
            tables.append("{}.{}".format(db_name, table_name))

    return tables


def get_bookmark_for_table(table, properties, mysql):
    """Get actual bookmark for a specific table used for LOG_BASED or INCREMENTAL
    replications
    """
    bookmark = {}

    # Find table from properties and get bookmark based on replication method
    for stream in properties.get("streams", []):
        metadata = stream.get("metadata", [])
        table_name = stream.get("table_name")

        # Get table specific metadata i.e. replication method, replication key, etc.
        table_meta = next((i for i in metadata if type(i) == dict and len(i.get("breadcrumb", [])) == 0), {}).get("metadata")
        db_name = table_meta.get("database-name")
        replication_method = table_meta.get("replication-method")
        replication_key = table_meta.get("replication-key")

        fully_qualified_table_name = "{}.{}".format(db_name, table_name)
        if fully_qualified_table_name == table:
            # Log based replication: get mysql binlog position
            if replication_method == "LOG_BASED":
                binlog_pos = mysql.fetch_current_log_file_and_pos()
                bookmark = {
                    "log_file": binlog_pos.get('File'),
                    "log_pos": binlog_pos.get('Position'),
                    "version": binlog_pos.get('version', 1)
                }

            # Key based incremental replication: Get max replication key from source
            elif replication_method == "INCREMENTAL":
                incremental_pos = mysql.fetch_current_incremental_key_pos(fully_qualified_table_name, replication_key)
                bookmark = {
                    "replication_key": replication_key,
                    "replication_key_value": incremental_pos.get('key_value'),
                    "version": incremental_pos.get('version', 1)
                }

            break

    return bookmark


def save_state_file(path, table, bookmark):
    table_dict = tablename_to_dict(table)
    stream_id = "{}-{}".format(table_dict.get('schema'), table_dict.get('name'))

    # Do nothing if state path not defined
    if not path:
        return

    # Load the current state file
    state = {}
    if os.path.exists(path):
        state = load_json(path)

    # Find the current table position
    bookmarks = state.get('bookmarks', {})

    # Update the state file with the new values at the right place
    state['currently_syncing'] = None
    state['bookmarks'] = bookmarks
    state['bookmarks'][stream_id] = bookmark

    # Save the new state file
    save_dict_to_json(path, state)


def parse_args(required_config_keys):
    '''Parse standard command-line args.

    --tap               MySQL Config file
    --state             State file
    --properties        Properties file
    --target            Snowflake Config file
    --transform         Transformations Config file
    --tables            Tables to sync. (Separated by comma)
    --export-dir        Directory to create temporary csv exports. Defaults to current work dir.

    Returns the parsed args object from argparse. For each argument that
    point to JSON files (tap, state, properties, target, transform),
    we will automatically load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--tap',
        help='MySQL Config file',
        required=True)

    parser.add_argument(
        '--state',
        help='State file')

    parser.add_argument(
        '--properties',
        help='Properties file')

    parser.add_argument(
        '--target',
        help='Snowflake Config file',
        required=True)

    parser.add_argument(
        '--transform',
        help='Transformations Config file')

    parser.add_argument(
        '--tables',
        help='Sync only specific tables')

    parser.add_argument(
        '--export-dir',
        help='Temporary directory required for CSV exports')

    args = parser.parse_args()
    if args.tap:
        args.tap = load_json(args.tap)
    if args.properties:
        args.properties = load_json(args.properties)
    if args.target:
        args.target = load_json(args.target)
    if args.transform:
        args.transform = load_json(args.transform)
    else:
        args.transform = {}
    if args.tables:
        args.tables = args.tables.split(',')
    else:
        args.tables = get_tables_from_properties(args.properties)
    if args.export_dir:
        args.export_dir = args.export_dir
    else:
        args.export_dir = os.path.realpath('/tmp')

    check_config(args.tap, required_config_keys['tap'])
    check_config(args.target, required_config_keys['target'])

    return args


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception("Config is missing required keys: {}".format(missing_keys))
