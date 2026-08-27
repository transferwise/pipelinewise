import json

import argparse
import os
import re

from datetime import datetime
from ast import literal_eval

import sqlparse

from typing import Dict, Tuple, List, Union

from pipelinewise.cli.errors import InvalidConfigException
from pipelinewise.fastsync.commons import utils as common_utils
from pipelinewise.fastsync.commons import snowflake_iceberg_routes as iceberg_routes
from pipelinewise.fastsync.commons.snowflake_types import (
    SNOWFLAKE_MAX_VARCHAR,
    SNOWFLAKE_MAX_VARCHAR_LENGTH,
)
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake


# A dynamic boundary query with no usable scalar is a successful no-op.
DYNAMIC_BOUNDARY_NOT_READY = object()
SNOWFLAKE_TEXT_TYPES = frozenset({
    'CHAR',
    'CHARACTER',
    'CHARACTER VARYING',
    'STRING',
    'TEXT',
    'VARCHAR',
})
SOURCE_COLUMN_DEFINITION = re.compile(
    r'^\s*(?P<name>"(?:[^"]|"")*"|\S+)\s+(?P<data_type>.+?)\s*$'
)


class NativePartialSyncCompatibilityError(RuntimeError):
    """Existing native target schema cannot safely accept PartialSync data."""


def upload_to_s3(
    snowflake: FastSyncTargetSnowflake,
    file_parts: List,
    temp_dir: str,
    planned_s3_keys=None,
) -> Tuple[List, str]:
    """Upload PartialSync staging through the shared FastSync implementation."""
    upload_options = (
        {'planned_s3_keys': planned_s3_keys}
        if planned_s3_keys is not None
        else {}
    )
    return common_utils.upload_files_to_s3(
        snowflake,
        file_parts,
        temp_dir,
        snowflake.connection_config.get('s3_bucket'),
        **upload_options,
    )


def delete_s3_objects(
    snowflake: FastSyncTargetSnowflake,
    s3_keys: List,
    bucket: str,
    cleanup_context='PartialSync staging cleanup after successful publication',
) -> None:
    """Delete every staged object before state can advance."""
    common_utils.delete_s3_objects(
        snowflake,
        s3_keys,
        bucket,
        cleanup_context=cleanup_context,
    )


def diff_source_target_columns(target_sf: dict, source_columns: list) -> dict:
    """Finding the diff between source and target columns"""
    target_column = target_sf['sf_object'].query(
        f'SHOW COLUMNS IN TABLE {target_sf["schema"]}."{target_sf["table"].upper()}"'
    )

    source_columns_dict = _get_source_columns_dict(source_columns)
    target_columns_info = _get_target_columns_info(target_column)
    added_columns = _get_added_columns(source_columns_dict, target_columns_info['columns_dict'])
    removed_columns = _get_removed_columns(source_columns_dict, target_columns_info['columns_dict'])
    varchar_columns_to_widen = _get_varchar_columns_to_widen(
        target_sf,
        source_columns_dict,
        target_columns_info,
    )

    return {
        'added_columns': added_columns,
        'removed_columns': removed_columns,
        'target_columns': target_columns_info['column_names'],
        'source_columns': source_columns_dict,
        'varchar_columns_to_widen': varchar_columns_to_widen,
    }


def load_into_snowflake(target, args, source_columns, primary_keys, s3_key_pattern, size_bytes,
                        where_clause_sql):
    """Load staging data before creating or modifying the live target table."""

    snowflake = target['sf_object']
    snowflake.copy_to_table(
        s3_key_pattern, target['schema'], args.table, size_bytes, is_temporary=True
    )
    snowflake.obfuscate_columns(target['schema'], args.table)

    if args.drop_target_table:
        common_utils.apply_snowflake_table_grants(
            snowflake,
            args.target,
            target['schema'],
            args.table,
            is_temporary=True,
        )

    snowflake.create_table(
        target_schema=target['schema'],
        table_name=target['table'],
        columns=source_columns,
        primary_key=primary_keys,
        is_temporary=False,
        sort_columns=False,
        allow_replace_table=False,
        normalize_primary_keys=(
            False if args.drop_target_table else 'if_created'
        ),
    )
    iceberg_routes.require_native_target_format(
        snowflake,
        args,
        target['schema'],
        args.table,
        allow_missing=False,
    )
    if args.drop_target_table:
        publication_status = target.get('publication_status')
        if publication_status is not None:
            publication_status['attempted'] = True
        snowflake.swap_tables(target['schema'], target['table'])
    else:
        columns_diff = diff_source_target_columns(target, source_columns=source_columns)
        # Snowflake DDL commits independently. Finish safe, monotonic schema changes before the atomic MERGE.
        if columns_diff['varchar_columns_to_widen']:
            try:
                snowflake.widen_varchar_columns(
                    target['schema'],
                    target['table'],
                    columns_diff['varchar_columns_to_widen'],
                )
            except Exception as exc:
                quoted_columns = ', '.join(
                    _quote_identifier(column)
                    for column in columns_diff['varchar_columns_to_widen']
                )
                raise NativePartialSyncCompatibilityError(
                    f'Failed to widen native PartialSync target {target["schema"]}.'
                    f'"{target["table"].upper()}" columns {quoted_columns} to '
                    f'{SNOWFLAKE_MAX_VARCHAR}: {exc}. Use a role authorized to alter '
                    'the table or widen these columns manually, then retry PartialSync; '
                    'the MERGE and state advancement did not run.'
                ) from exc
        snowflake.add_columns(
            target['schema'], target['table'], columns_diff['added_columns']
        )
        added_metadata_columns = ['_SDC_EXTRACTED_AT', '_SDC_BATCHED_AT', '_SDC_DELETED_AT']
        publication_status = target.get('publication_status')
        if publication_status is not None:
            publication_status['attempted'] = True
        snowflake.publish_partial_sync(
            target['schema'],
            target['temp'],
            target['table'],
            list(columns_diff['source_columns'].keys()) + added_metadata_columns,
            primary_keys,
            where_clause_sql,
            hard_delete=args.target['hard_delete'] is True,
        )
        snowflake.drop_table(
            target['schema'],
            target['table'],
            is_temporary=True,
            max_attempts=3,
        )


def update_state_file(
    args: argparse.Namespace,
    bookmark: Dict,
    state_lock=None,
) -> None:
    """Update state after an unbounded sync; the legacy lock argument is ignored."""
    del state_lock
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


def _validate_static_boundary_value(string_to_check: str) -> str:
    """Validating if the static boundary values are valid and there is no injection"""

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


def _validate_dynamic_boundary_value(query_object, string_to_check: str) -> object:
    """Validating if the dynamic boundary values are valid and there is no injection"""
    try:
        _check_for_allowed_query(string_to_check)
        return_value = query_object(string_to_check)
        if return_value == []:
            return DYNAMIC_BOUNDARY_NOT_READY
        if len(return_value) > 1 or len(return_value[0]) != 1:
            raise Exception

        if isinstance(return_value[0], dict):
            boundary_value = list(return_value[0].values())[0]
        else:
            boundary_value = return_value[0][0]
        if boundary_value is None:
            return DYNAMIC_BOUNDARY_NOT_READY
    except Exception:
        raise (InvalidConfigException(f'Invalid query for boundary value: {string_to_check}')) from Exception
    return boundary_value


def validate_boundary_value(query_object: object, string_to_check: Union[str, None]) -> object:
    """Validate and finding the boundary value"""
    if string_to_check:
        if string_to_check.startswith('<S>'):
            return _validate_static_boundary_value(string_to_check[3:])
        if string_to_check.startswith('<D>'):
            return _validate_dynamic_boundary_value(query_object, string_to_check[3:])
    return None


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


def quote_tag_to_char(value_string: Union[str, None]) -> Union[str, None]:
    """convert quote tag in a string to its original qoute character"""
    if value_string:
        return value_string.replace('<<quote>>', "'")

    return value_string


def _check_for_allowed_query(query_string):
    statements = sqlparse.split(query_string)
    if len(statements) != 1:
        raise Exception('More than one statement is not allowed!')

    sql_type = sqlparse.parse(statements[0])[0].get_type()
    if sql_type != 'SELECT':
        raise Exception('Not allowed statement!')


def _get_target_columns_info(target_column):
    target_columns_dict = {}
    character_maximum_lengths = {}
    raw_column_names = {}
    list_of_target_column_names = []
    for column in target_column:
        list_of_target_column_names.append(column['column_name'])
        column_type_str = column['data_type']
        column_type_dict = json.loads(column_type_str)
        quoted_column_name = _quote_identifier(column['column_name'])
        target_columns_dict[quoted_column_name] = column_type_dict['type']
        character_maximum_lengths[quoted_column_name] = column_type_dict.get(
            'length'
        )
        raw_column_names[quoted_column_name] = column['column_name']
    return {
        'character_maximum_lengths': character_maximum_lengths,
        'column_names': list_of_target_column_names,
        'columns_dict': target_columns_dict,
        'raw_column_names': raw_column_names,
    }


def _get_source_columns_dict(source_columns):
    source_columns_dict = {}
    for column in source_columns:
        match = SOURCE_COLUMN_DEFINITION.fullmatch(column)
        if match is None:
            raise NativePartialSyncCompatibilityError(
                f'Invalid native PartialSync source column definition: {column!r}'
            )
        source_columns_dict[match.group('name')] = match.group('data_type')
    return source_columns_dict


def _quote_identifier(identifier):
    escaped_identifier = identifier.replace('"', '""')
    return f'"{escaped_identifier}"'


def _normalized_data_type(data_type):
    return re.sub(r'\s+', '', data_type).upper()


def _native_target_name(target_sf):
    return f'{target_sf["schema"]}."{target_sf["table"].upper()}"'


def _get_varchar_columns_to_widen(
    target_sf,
    source_columns_dict,
    target_columns_info,
):
    columns_to_widen = []
    normalized_max_varchar = _normalized_data_type(SNOWFLAKE_MAX_VARCHAR)
    for source_column, source_type in source_columns_dict.items():
        if _normalized_data_type(source_type) != normalized_max_varchar:
            continue
        target_type = target_columns_info['columns_dict'].get(source_column)
        if target_type is None:
            continue
        if target_type.upper() not in SNOWFLAKE_TEXT_TYPES:
            raise NativePartialSyncCompatibilityError(
                f'Native PartialSync cannot safely publish {source_column} as '
                f'{SNOWFLAKE_MAX_VARCHAR}: existing target '
                f'{_native_target_name(target_sf)} has type {target_type}. Run a '
                'FullSync or alter the target column to a compatible text type, '
                'then retry PartialSync.'
            )
        target_length = target_columns_info['character_maximum_lengths'].get(
            source_column
        )
        if not isinstance(target_length, int):
            raise NativePartialSyncCompatibilityError(
                f'Native PartialSync cannot verify CHARACTER_MAXIMUM_LENGTH for '
                f'{_native_target_name(target_sf)}.{source_column}. Widen the '
                f'target column to {SNOWFLAKE_MAX_VARCHAR} manually or run a '
                'FullSync, then retry PartialSync.'
            )
        if target_length < SNOWFLAKE_MAX_VARCHAR_LENGTH:
            columns_to_widen.append(
                target_columns_info['raw_column_names'][source_column]
            )
    return columns_to_widen


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
