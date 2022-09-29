import argparse
import json
import multiprocessing
import os
import logging
import datetime

from typing import Dict
from functools import reduce

import pandas

from pipelinewise.cli.utils import generate_random_string

LOGGER = logging.getLogger(__name__)

SDC_EXTRACTED_AT = '_SDC_EXTRACTED_AT'
SDC_BATCHED_AT = '_SDC_BATCHED_AT'
SDC_DELETED_AT = '_SDC_DELETED_AT'


class NotSelectedTableException(Exception):
    """
    Exception to raise when a table is not selected for resync
    """

    def __init__(self, table_name, selected_tables):
        self.message = f'Cannot Resync unselected table "{table_name}"! Selected tables are: {selected_tables}'
        super().__init__(self, self.message)


# pylint: disable=missing-function-docstring
def get_cpu_cores():
    """Get CPU cores for multiprocessing"""
    try:
        return multiprocessing.cpu_count()
    # Defaults to 1 core in case of any exception
    except Exception:
        return 1


def load_json(path):
    with open(path, encoding='utf-8') as fil:
        return json.load(fil)


def save_dict_to_json(path, data):
    LOGGER.info('Saving new state file to %s', path)
    with open(path, 'w', encoding='utf-8') as fil:
        fil.write(json.dumps(data, indent=4, sort_keys=True))


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
        'temp_table_name': '{}_temp'.format(table_name),
    }


def get_tables_from_properties(properties: Dict) -> set:
    """Get list of selected tables with schema names from properties json
    The output is used to generate list of tables to sync
    """
    tables = set()

    for stream in properties.get('streams', tables):
        metadata = stream.get('metadata', [])
        table_name = stream.get('table_name', stream['stream'])

        table_meta = next(
            (
                i
                for i in metadata
                if isinstance(i, dict) and len(i.get('breadcrumb', [])) == 0
            ),
            {},
        ).get('metadata')
        selected = table_meta.get('selected', False)
        schema_name = table_meta.get('schema-name')
        db_name = table_meta.get('database-name')

        if table_name and selected:
            if schema_name is not None or db_name is not None:
                tables.add('{}.{}'.format(schema_name or db_name, table_name))
            else:
                # Some tap types don't have db name nor schema name
                tables.add(table_name)

    return tables


def get_bookmark_for_table(table, properties, db_engine, dbname=None):
    """Get actual bookmark for a specific table used for LOG_BASED or INCREMENTAL
    replications
    """
    bookmark = {}

    # Find table from properties and get bookmark based on replication method
    for stream in properties.get('streams', []):
        metadata = stream.get('metadata', [])
        table_name = stream.get('table_name', stream['stream'])

        # Get table specific metadata i.e. replication method, replication key, etc.
        table_meta = next(
            (
                i
                for i in metadata
                if isinstance(i, dict) and len(i.get('breadcrumb', [])) == 0
            ),
            {},
        ).get('metadata')
        db_name = table_meta.get('database-name')
        schema_name = table_meta.get('schema-name')
        replication_method = table_meta.get('replication-method')
        replication_key = table_meta.get('replication-key')

        fully_qualified_table_name = (
            '{}.{}'.format(schema_name or db_name, table_name)
            if schema_name is not None or db_name is not None
            else table_name
        )

        if (
            dbname is None or db_name == dbname
        ) and fully_qualified_table_name == table:
            # Log based replication: get mysql binlog position
            if replication_method == 'LOG_BASED':
                bookmark = db_engine.fetch_current_log_pos()

            # Key based incremental replication: Get max replication key from source
            elif replication_method == 'INCREMENTAL':
                bookmark = db_engine.fetch_current_incremental_key_pos(
                    fully_qualified_table_name, replication_key
                )

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
    config_default_target_schema = target_config.get(
        'default_target_schema', ''
    ).strip()
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
            '(object) defines target schema for {} stream. '.format(table)
        )

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
    config_default_target_schema_select_permissions = target_config.get(
        'default_target_schema_select_permissions', []
    )
    config_schema_mapping = target_config.get('schema_mapping', {})

    table_dict = tablename_to_dict(table)
    table_schema = table_dict['schema_name']
    if config_schema_mapping and table_schema in config_schema_mapping:
        grantees = config_schema_mapping[table_schema].get(
            'target_schema_select_permissions', []
        )
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
        stream_id = '{}-{}-{}'.format(
            dbname, table_dict.get('schema_name'), table_dict.get('table_name')
        )
    elif table_dict['schema_name']:
        stream_id = '{}-{}'.format(
            table_dict['schema_name'], table_dict.get('table_name')
        )
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


def parse_args(required_config_keys: Dict) -> argparse.Namespace:
    """Parse standard command-line args.

    --tap               Tap Config file
    --state             State file
    --properties        Properties file
    --target            Target Config file
    --transform         Transformations Config file
    --tables            Tables to sync. (Separated by comma)
    --temp_dir          Directory to create temporary csv exports. Defaults to current work dir.
    --drop_pg_slot      flag to drop or not the Postgres replication slot before starting the resync

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
    parser.add_argument(
        '--temp_dir', help='Temporary directory required for CSV exports'
    )
    parser.add_argument(
        '--drop_pg_slot',
        help='Drop pg replication slot before starting resync',
        action='store_true',
    )

    args: argparse.Namespace = parser.parse_args()

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

    # get all selected tables from json schema
    all_selected_tables = get_tables_from_properties(args.properties)

    if args.tables:
        # prevent duplicates
        unique_tables_list = set(args.tables.split(','))

        # check if all the given tables are actually selected
        for table in unique_tables_list:
            if table not in all_selected_tables:
                raise NotSelectedTableException(table, all_selected_tables)

        args.tables = unique_tables_list
    else:
        args.tables = all_selected_tables

    if not args.temp_dir:
        args.temp_dir = os.path.realpath('.')

    check_config(args.tap, required_config_keys['tap'])
    check_config(args.target, required_config_keys['target'])

    return args


# pylint: disable=import-outside-toplevel
def retry_pattern():
    import backoff
    from botocore.exceptions import ClientError

    return backoff.on_exception(
        backoff.expo,
        ClientError,
        max_tries=5,
        on_backoff=log_backoff_attempt,
        factor=10,
    )


def log_backoff_attempt(details):
    LOGGER.error(
        'Error detected communicating with Amazon, triggering backoff: %s try',
        details.get('tries'),
    )


def get_pool_size(tap: Dict) -> int:
    """
    Get the pool size to use in FastSync
    Args:
        tap: tap config, a dictionary with optional key "fastsync_parallelism"

    Returns: pool size as int

    """
    cpu_cores = get_cpu_cores()
    fastsync_parallelism = tap.get('fastsync_parallelism', None)

    if fastsync_parallelism is None:
        return cpu_cores

    return min(fastsync_parallelism, cpu_cores)


def gen_export_filename(
    tap_id: str, table: str, suffix: str = None, postfix: str = None, ext: str = None, sync_type: str = 'fastsync'
) -> str:
    """
    Generates a unique filename used for exported fastsync data that avoids file name collision

    Default pattern:
        pipelinewise_<tap_id>_<table>_<timestamp_with_ms>_fastsync_<random_string>.csv.gz

    Args:
        tap_id: Unique tap id
        table: Name of the table to export
        suffix: Generated filename suffix. Defaults to current timestamp in milliseconds
        postfix: Generated filename postfix. Defaults to a random 8 character length string
        ext: Filename extension. Defaults to .csv.gz

    Returns:
        Unique filename as a string
    """
    if not suffix:
        suffix = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')

    if not postfix:
        postfix = generate_random_string()

    if not ext:
        ext = 'csv.gz'

    return f'pipelinewise_{tap_id}_{table}_{suffix}_{sync_type}_{postfix}.{ext}'


def remove_duplicate_rows_from_csv(file_path: str, primary_keys: list, chunk_size) -> None:
    pandas_obj = reduce(lambda df_i, df_j: pandas.concat([df_i, df_j]).drop_duplicates(
        subset=primary_keys, keep='last'), pandas.read_csv(file_path, sep=',', chunksize=chunk_size)
    )
    pandas_obj.to_csv(file_path, index=False)
