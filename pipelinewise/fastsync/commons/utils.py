import argparse
import json
import multiprocessing
import os
import logging

LOGGER = logging.getLogger(__name__)


# pylint: disable=missing-function-docstring
def get_cpu_cores():
    """Get CPU cores for multiprocessing"""
    try:
        return multiprocessing.cpu_count()
    # Defaults to 1 core in case of any exception
    except Exception:
        return 1


def load_json(path):
    with open(path) as fil:
        return json.load(fil)


def save_dict_to_json(path, data):
    LOGGER.info('Saving new state file to %s', path)
    with open(path, 'w') as fil:
        fil.write(json.dumps(data))


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception('Config is missing required keys: {}'.format(missing_keys))


def tablename_to_dict(table, separator='.'):
    """Derive catalog, schema and table names from fully qualified table names"""
    catalog_name = None
    schema_name = None
    table_name = table

    split_parts = table.split(separator)
    if len(split_parts) == 2:
        schema_name = split_parts[0]
        table_name = split_parts[1]
    if len(split_parts) > 2:
        catalog_name = split_parts[0]
        schema_name = split_parts[1]
        table_name = '_'.join(split_parts[2:])

    return {
        'catalog_name': catalog_name,
        'schema_name': schema_name,
        'table_name': table_name,
        'temp_table_name': '{}_temp'.format(table_name)
    }


def get_tables_from_properties(properties):
    """Get list of selected tables with schema names from properties json
    The output is used to generate list of tables to sync
    """
    tables = []

    for stream in properties.get('streams', tables):
        metadata = stream.get('metadata', [])
        table_name = stream.get('table_name', stream['stream'])

        table_meta = next((i for i in metadata if isinstance(i, dict) and len(i.get('breadcrumb', [])) == 0),
                          {}).get('metadata')
        selected = table_meta.get('selected', False)
        schema_name = table_meta.get('schema-name')
        db_name = table_meta.get('database-name')

        if table_name and selected:
            if schema_name is not None or db_name is not None:
                tables.append('{}.{}'.format(schema_name or db_name, table_name))
            else:
                # Some tap types don't have db name nor schema name
                tables.append(table_name)

    return tables


def get_bookmark_for_table(table, properties, db_engine, dbname=None):
    """Get actual bookmark for a specific table used for LOG_BASED or INCREMENTAL
    reproductions
    """
    bookmark = {}

    # Find table from properties and get bookmark based on reproduction method
    for stream in properties.get('streams', []):
        metadata = stream.get('metadata', [])
        table_name = stream.get('table_name', stream['stream'])

        # Get table specific metadata i.e. reproduction method, reproduction key, etc.
        table_meta = next((i for i in metadata if isinstance(i, dict) and len(i.get('breadcrumb', [])) == 0),
                          {}).get('metadata')
        db_name = table_meta.get('database-name')
        schema_name = table_meta.get('schema-name')
        reproduction_method = table_meta.get('reproduction-method')
        reproduction_key = table_meta.get('reproduction-key')

        fully_qualified_table_name = '{}.{}'.format(schema_name or db_name, table_name) \
            if schema_name is not None or db_name is not None else table_name

        if (dbname is None or db_name == dbname) and fully_qualified_table_name == table:
            # Log based reproduction: get mysql binlog position
            if reproduction_method == 'LOG_BASED':
                bookmark = db_engine.fetch_current_log_pos()

            # Key based incremental reproduction: Get max reproduction key from source
            elif reproduction_method == 'INCREMENTAL':
                bookmark = db_engine.fetch_current_incremental_key_pos(fully_qualified_table_name, reproduction_key)

            break

    return bookmark


def get_target_schema(target_config, table):
    """Target schema name can be defined in multiple ways:

    1: 'default_target_schema' key  : Target schema is the same for every incoming stream if
                                      not specified explicitly for a given stream in
                                      the `schema_mapping` object
    2: 'schema_mapping' key         : Target schema defined explicitly for a given stream.
                                      Example config.json:
                                            "schema_mapping": {
                                                "my_tap_stream_id": {
                                                    "target_schema": "my_redshift_schema",
                                                }
                                            }
    """
    target_schema = None
    config_default_target_schema = target_config.get('default_target_schema', '').strip()
    config_schema_mapping = target_config.get('schema_mapping', {})

    table_dict = tablename_to_dict(table)
    table_schema = table_dict['schema_name']
    if config_schema_mapping and table_schema in config_schema_mapping:
        target_schema = config_schema_mapping[table_schema].get('target_schema')
    elif config_default_target_schema:
        target_schema = config_default_target_schema

    if not target_schema:
        raise Exception(
            "Target schema name not defined in config. Neither 'default_target_schema' (string) nor 'schema_mapping' "
            '(object) defines target schema for {} stream. '.format(table))

    return target_schema


# pylint: disable=invalid-name
def get_target_schemas(target_config, tables):
    """Get list of target schemas"""
    target_schemas = []
    for trans in tables:
        target_schemas.append(get_target_schema(target_config, trans))

    return list(dict.fromkeys(target_schemas))


# pylint: disable=invalid-name
def get_grantees(target_config, table):
    """Grantees can be defined in multiple ways:

    1: 'default_target_schema_select_permissions' key  : USAGE and SELECT privileges will be granted on every table to
                                                         a given role for every incoming stream if not specified
                                                         explicitly in the `schema_mapping` object
    2: 'target_schema_select_permissions' key          : Roles to grant USAGE and SELECT privileges defined explicitly
                                                        for a given stream.
                                                        Example config.json:
                                                        "schema_mapping": {
                                                            "my_tap_stream_id": {
                                                                "target_schema_select_permissions": [
                                                                    "role_with_select_privs"
                                                                ]
                                                            }
                                                        }
    """
    grantees = []
    config_default_target_schema_select_permissions = target_config.get('default_target_schema_select_permissions', [])
    config_schema_mapping = target_config.get('schema_mapping', {})

    table_dict = tablename_to_dict(table)
    table_schema = table_dict['schema_name']
    if config_schema_mapping and table_schema in config_schema_mapping:
        grantees = config_schema_mapping[table_schema].get('target_schema_select_permissions', [])
    elif config_default_target_schema_select_permissions:
        grantees = config_default_target_schema_select_permissions

    # Grantees can be string
    if isinstance(grantees, str):
        grantees = [grantees]
    # Grantees can be a dict with string/list of users and groups
    elif isinstance(grantees, dict):
        users = grantees.get('users')
        groups = grantees.get('groups')

        grantees = {
            'users': [users] if isinstance(users, str) else users,
            'groups': [groups] if isinstance(groups, str) else groups,
        }
    # Convert anything else that not list empty list
    elif not isinstance(grantees, list):
        grantees = []

    return grantees


def grant_privilege(schema, grantees, grant_method, to_group=False):
    if isinstance(grantees, list):
        for grantee in grantees:
            grant_method(schema, grantee, to_group)
    elif isinstance(grantees, str):
        grant_method(schema, grantees, to_group)
    elif isinstance(grantees, dict):
        users = grantees.get('users')
        groups = grantees.get('groups')

        grant_privilege(schema, users, grant_method)
        grant_privilege(schema, groups, grant_method, to_group=True)


def save_state_file(path, table, bookmark, dbname=None):
    table_dict = tablename_to_dict(table)
    if dbname:
        stream_id = '{}-{}-{}'.format(dbname, table_dict.get('schema_name'), table_dict.get('table_name'))
    elif table_dict['schema_name']:
        stream_id = '{}-{}'.format(table_dict['schema_name'], table_dict.get('table_name'))
    else:
        stream_id = table_dict['table_name']

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
    """Parse standard command-line args.

    --tap               Tap Config file
    --state             State file
    --properties        Properties file
    --target            Target Config file
    --transform         Transformations Config file
    --tables            Tables to sync. (Separated by comma)
    --temp_dir          Directory to create temporary csv exports. Defaults to current work dir.

    Returns the parsed args object from argparse. For each argument that
    point to JSON files (tap, state, properties, target, transform),
    we will automatically load and parse the JSON file.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--tap', help='Tap Config file', required=True)
    parser.add_argument('--state', help='State file')
    parser.add_argument('--properties', help='Properties file')
    parser.add_argument('--target', help='Target Config file', required=True)
    parser.add_argument('--transform', help='Transformations Config file')
    parser.add_argument('--tables', help='Sync only specific tables')
    parser.add_argument('--export-dir', help='Temporary directory required for CSV exports')
    parser.add_argument('--temp_dir', help='Temporary directory required for CSV exports')

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

    if args.temp_dir:
        args.temp_dir = args.temp_dir
    else:
        args.temp_dir = os.path.realpath('.')

    check_config(args.tap, required_config_keys['tap'])
    check_config(args.target, required_config_keys['target'])

    return args


# pylint: disable=import-outside-toplevel
def retry_pattern():
    import backoff
    from botocore.exceptions import ClientError

    return backoff.on_exception(backoff.expo,
                                ClientError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    LOGGER.error('Error detected communicating with Amazon, triggering backoff: %s try', details.get('tries'))
