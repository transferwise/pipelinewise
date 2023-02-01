import json

import argparse
import os
import re

from datetime import datetime
from ast import literal_eval

from typing import Dict, Tuple, List

from pipelinewise.cli.errors import InvalidConfigException
from pipelinewise.fastsync.commons import utils as common_utils
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake


def upload_to_s3(snowflake: FastSyncTargetSnowflake, file_parts: List, temp_dir: str) -> Tuple[List, str]:
    """Upload exported data into S3"""

    s3_keys = []
    for file_part in file_parts:
        s3_keys.append(snowflake.upload_to_s3(file_part, tmp_dir=temp_dir))
        os.remove(file_part)

    # Create a pattern that match all file parts by removing multipart suffix
    s3_key_pattern = (
        re.sub(r'\.part\d*$', '', s3_keys[0])
        if len(s3_keys) > 0
        else 'NO_FILES_TO_LOAD'
    )
    return s3_keys, s3_key_pattern


def diff_source_target_columns(target_sf: dict, source_columns: list) -> dict:
    """Finding the diff between source and target columns"""
    target_column = target_sf['sf_object'].query(
        f'SHOW COLUMNS IN TABLE {target_sf["schema"]}."{target_sf["table"].upper()}"'
    )

    source_columns_dict = _get_source_columns_dict(source_columns)
    target_columns_info = _get_target_columns_info(target_column)
    added_columns = _get_added_columns(source_columns_dict, target_columns_info['columns_dict'])
    removed_columns = _get_removed_columns(source_columns_dict, target_columns_info['columns_dict'])

    return {
        'added_columns': added_columns,
        'removed_columns': removed_columns,
        'target_columns': target_columns_info['column_names'],
        'source_columns': source_columns_dict
    }


def load_into_snowflake(target, args, columns_diff, primary_keys, s3_key_pattern, size_bytes,
                        where_clause_sql):
    """Loading data from S3 to the temp table in snowflake and then merge it with the target table"""

    snowflake = target['sf_object']
    # Load into Snowflake temp table
    snowflake.copy_to_table(
        s3_key_pattern, target['schema'], args.table, size_bytes, is_temporary=True
    )
    # Obfuscate columns
    snowflake.obfuscate_columns(target['schema'], args.table)

    snowflake.add_columns(target['schema'], target['table'], columns_diff['added_columns'])
    added_metadata_columns = ['_SDC_EXTRACTED_AT', '_SDC_BATCHED_AT', '_SDC_DELETED_AT']
    if args.drop_target_table:
        snowflake.swap_tables(target['schema'], target['table'])
    else:
        snowflake.merge_tables(
            target['schema'], target['temp'], target['table'],
            list(columns_diff['source_columns'].keys()) + added_metadata_columns, primary_keys)

        if args.target['hard_delete'] is True:
            snowflake.partial_hard_delete(target['schema'], target['table'], where_clause_sql)
        snowflake.drop_table(target['schema'], target['temp'])


def update_state_file(args: argparse.Namespace, bookmark: Dict) -> None:
    """Update state file"""
    # Save bookmark to singer state file
    if not args.end_value:
        common_utils.save_state_file(args.state, args.table, bookmark)


def parse_args_for_partial_sync(required_config_keys: Dict) -> argparse.Namespace:
    """Parsing arguments for partial sync"""

    parser = _get_args_parser_for_partialsync()

    parser.add_argument('--table', help='Partial sync table')
    parser.add_argument('--column', help='Column for partial sync table')
    parser.add_argument('--start_value', help='Start value for partial sync table')
    parser.add_argument('--end_value', help='End value for partial sync table')
    parser.add_argument('--drop_target_table', help='Dropping target table before sync')

    args: argparse.Namespace = parser.parse_args()

    if args.tap:
        args.tap = common_utils.load_json(args.tap)

    if args.properties:
        args.properties = common_utils.load_json(args.properties)

    if args.target:
        args.target = common_utils.load_json(args.target)

    if args.transform:
        args.transform = common_utils.load_json(args.transform)
    else:
        args.transform = {}

    if not args.temp_dir:
        args.temp_dir = os.path.realpath('.')

    common_utils.check_config(args.tap, required_config_keys['tap'])
    common_utils.check_config(args.target, required_config_keys['target'])

    return args


def validate_boundary_value(string_to_check: str) -> str:
    """Validating if the boundary values are valid and there is no injection"""
    if not string_to_check:
        return string_to_check

    # Validating string and number format
    pattern = re.compile(r'[A-Za-z0-9\\.\\-]+')
    if re.fullmatch(pattern, string_to_check):
        return string_to_check

    # Validating timestamp format
    try:
        datetime.strptime(string_to_check, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            datetime.strptime(string_to_check, '%Y-%m-%d')
        except ValueError:
            raise InvalidConfigException(f'Invalid boundary value: {string_to_check}') from Exception

    return string_to_check


def get_sync_tables(args: argparse.Namespace) -> Dict:
    """
    getting all needed information of tables for using in partial sync.
    """
    table_names = args.table.split(',')
    column_names = args.column.split(',')
    start_values = args.start_value.split(',')
    if args.end_value:
        end_values = args.end_value.split(',')
    else:
        end_values = [None] * len(table_names)
    if args.drop_target_table:
        drop_target_tables = [literal_eval(x) for x in args.drop_target_table.split(',')]
    else:
        drop_target_tables = [False] * len(table_names)
    sync_tables = {}
    for ind, table in enumerate(table_names):
        sync_tables[table] = {
            'column': column_names[ind],
            'start_value': start_values[ind],
            'end_value': end_values[ind],
            'drop_target_table': drop_target_tables[ind],
        }
    return sync_tables


def _get_target_columns_info(target_column):
    target_columns_dict = {}
    list_of_target_column_names = []
    for column in target_column:
        list_of_target_column_names.append(column['column_name'])
        column_type_str = column['data_type']
        column_type_dict = json.loads(column_type_str)
        target_columns_dict[f'"{column["column_name"]}"'] = column_type_dict['type']
    return {
        'column_names': list_of_target_column_names,
        'columns_dict': target_columns_dict
    }


def _get_source_columns_dict(source_columns):
    source_columns_dict = {}
    for column in source_columns:
        column_info = column.split(' ')
        column_name = column_info[0]
        column_type = ' '.join(column_info[1:])
        source_columns_dict[column_name] = column_type
    return source_columns_dict


def _get_args_parser_for_partialsync():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tap', help='Tap Config file', required=True)
    parser.add_argument('--state', help='State file')
    parser.add_argument('--properties', help='Properties file')
    parser.add_argument('--target', help='Target Config file', required=True)
    parser.add_argument('--transform', help='Transformations Config file')
    parser.add_argument(
        '--temp_dir', help='Temporary directory required for CSV exports'
    )
    parser.add_argument(
        '--drop_pg_slot',
        help='Drop pg replication slot before starting resync',
        action='store_true',
    )

    return parser


def _get_removed_columns(source_columns_dict, target_columns_dict):
    # ignoring columns added by PPW
    default_columns_added_by_ppw = {'"_SDC_EXTRACTED_AT"', '"_SDC_BATCHED_AT"', '"_SDC_DELETED_AT"'}

    removed_columns = set(target_columns_dict) - set(source_columns_dict)
    removed_columns = removed_columns - default_columns_added_by_ppw
    removed_columns = {key: target_columns_dict[key] for key in removed_columns}
    return removed_columns


def _get_added_columns(source_columns_dict, target_columns_dict):
    added_columns = set(source_columns_dict) - set(target_columns_dict)
    added_columns = {key: source_columns_dict[key] for key in added_columns}
    return added_columns
