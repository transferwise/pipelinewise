import json
import logging

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
    canonical_native_metadata_type,
    canonical_native_type,
)
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake
from pipelinewise.fastsync.commons.snowflake_column_versioning import (
    has_column_versions,
    is_retained_legacy_decimal_float,
    versioned_column_name,
)


LOGGER = logging.getLogger(__name__)
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


def diff_source_target_columns(
    target_sf: dict, source_columns: list, primary_keys=(), boundary_column=None, decimal_columns=(),
    version_legacy_float_columns=False,
) -> dict:
    """Finding the diff between source and target columns"""
    target_column = target_sf['sf_object'].query(
        f'SHOW COLUMNS IN TABLE {target_sf["schema"]}."{target_sf["table"].upper()}"'
    )

    source_columns_dict = _get_source_columns_dict(source_columns)
    target_columns_info = _get_target_columns_info(target_column)
    _reject_historical_boundary(boundary_column, source_columns_dict, target_columns_info['column_names'])
    added_columns = _get_added_columns(source_columns_dict, target_columns_info['columns_dict'])
    removed_columns = _get_removed_columns(source_columns_dict, target_columns_info['columns_dict'])
    varchar_columns_to_widen = _get_varchar_columns_to_widen(
        target_sf,
        source_columns_dict,
        target_columns_info,
        decimal_columns,
    )
    versions = _validate_existing_column_types(
        target_sf, source_columns_dict, target_columns_info, primary_keys, boundary_column, decimal_columns,
        version_legacy_float_columns,
    )
    staging_columns = _staging_source_columns(
        source_columns_dict,
        target_columns_info,
        primary_keys,
        decimal_columns,
        version_legacy_float_columns,
    )

    return {
        'added_columns': added_columns,
        'removed_columns': removed_columns,
        'target_columns': target_columns_info['column_names'],
        'source_columns': source_columns_dict,
        'staging_columns': staging_columns,
        'varchar_columns_to_widen': varchar_columns_to_widen,
        'column_versions': versions,
    }


def report_source_target_columns(
    target_sf: dict, source_columns: list, target_columns: list, primary_keys=(), boundary_column=None,
    decimal_columns=(), version_legacy_float_columns=False,
) -> list:
    """Report every mapped column using the same checks as native PartialSync."""
    rows_by_name = {}
    for row in target_columns:
        normalized = {key.lower(): value for key, value in row.items()}
        rows_by_name[_quote_identifier(normalized['column_name'])] = normalized
    results = []
    source_columns_dict = _get_source_columns_dict(source_columns)
    for name, source_type in source_columns_dict.items():
        row = rows_by_name.get(name)
        result = {'column': name, 'mapped_type': source_type, 'status': 'would_add', 'target_type': None}
        if boundary_column and name[1:-1].replace('""', '"').upper() == boundary_column.upper():
            try:
                _reject_historical_boundary(
                    boundary_column, source_columns_dict, [row['column_name'] for row in rows_by_name.values()],
                )
            except NativePartialSyncCompatibilityError as exc:
                result.update(status='incompatible', reason=str(exc))
                results.append(result)
                continue
        if row is not None:
            try:
                metadata = _get_target_columns_info([row])
                result['target_type'] = canonical_native_metadata_type(metadata['type_metadata'][name])
                widening = _get_varchar_columns_to_widen(
                    target_sf,
                    {name: source_type},
                    metadata,
                    decimal_columns,
                )
                versions = _validate_existing_column_types(
                    target_sf, {name: source_type}, metadata, primary_keys, boundary_column, decimal_columns,
                    version_legacy_float_columns,
                )
                result['status'] = 'would_version' if versions else 'would_widen' if widening else 'compatible'
            except (NativePartialSyncCompatibilityError, ValueError, TypeError, KeyError) as exc:
                result.update(status='incompatible', reason=str(exc))
        results.append(result)
    return results


def _reject_historical_boundary(boundary_column, source_columns, target_columns):
    if not boundary_column:
        return
    source_names = {name[1:-1].replace('""', '"') for name in source_columns}
    if has_column_versions(boundary_column.upper(), source_names, target_columns):
        raise NativePartialSyncCompatibilityError(
            f'PartialSync boundary column {boundary_column} has historical versions; '
            'run FullSync to recreate the target'
        )


def load_into_snowflake(target, args, source_columns, primary_keys, s3_key_pattern, size_bytes,
                        where_clause_sql, boundary_column=None, decimal_columns=(),
                        version_legacy_float_columns=None):
    """Load staging data before creating or modifying the live target table."""

    if version_legacy_float_columns is None:
        version_legacy_float_columns = args.target.get(
            'version_legacy_float_columns', False
        )

    snowflake = target['sf_object']
    snowflake.copy_to_table(
        s3_key_pattern, target['schema'], args.table, size_bytes, is_temporary=True, columns=source_columns,
    )

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
        columns_diff = diff_source_target_columns(
            target, source_columns=source_columns, primary_keys=primary_keys, boundary_column=boundary_column,
            decimal_columns=decimal_columns,
            version_legacy_float_columns=version_legacy_float_columns,
        )
        for name, data_type, archive in _plan_native_column_versions(columns_diff):
            snowflake.query(
                f'ALTER TABLE {_native_target_name(target)} RENAME COLUMN {name} TO {_quote_identifier(archive)}'
            )
            LOGGER.info('Column "%s" in table "%s" has been renamed to "%s"', name[1:-1].replace('""', '"'),
                        _native_target_name(target), archive)
            snowflake.add_columns(target['schema'], target['table'], {name: data_type})
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
        )
        snowflake.drop_table(
            target['schema'],
            target['table'],
            is_temporary=True,
            max_attempts=3,
        )


def _plan_native_column_versions(columns_diff):
    names = set(columns_diff['target_columns']) | {
        name[1:-1].replace('""', '"') for name in columns_diff['source_columns']
    }
    versions = []
    for name, data_type in columns_diff['column_versions'].items():
        archive = versioned_column_name(name[1:-1].replace('""', '"'))
        if archive in names:
            raise NativePartialSyncCompatibilityError(f'Historical column already exists: {archive}')
        names.add(archive)
        versions.append((name, data_type, archive))
    return versions


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
    type_metadata = {}
    list_of_target_column_names = []
    for column in target_column:
        column = {key.lower(): value for key, value in column.items()}
        list_of_target_column_names.append(column['column_name'])
        column_type_str = column['data_type']
        column_type_dict = json.loads(column_type_str)
        quoted_column_name = _quote_identifier(column['column_name'])
        target_columns_dict[quoted_column_name] = column_type_dict['type']
        character_maximum_lengths[quoted_column_name] = column_type_dict.get(
            'length'
        )
        raw_column_names[quoted_column_name] = column['column_name']
        type_metadata[quoted_column_name] = column_type_dict
    return {
        'character_maximum_lengths': character_maximum_lengths,
        'column_names': list_of_target_column_names,
        'columns_dict': target_columns_dict,
        'raw_column_names': raw_column_names,
        'type_metadata': type_metadata,
    }


def _get_source_columns_dict(source_columns):
    source_columns_dict = {}
    for column in source_columns:
        match = SOURCE_COLUMN_DEFINITION.fullmatch(column)
        if match is None:
            raise NativePartialSyncCompatibilityError(
                f'Invalid native PartialSync source column definition: {column!r}'
            )
        name = match.group('name')
        if not name.startswith('"'):
            name = _quote_identifier(name.upper())
        source_columns_dict[name] = match.group('data_type')
    return source_columns_dict


def _quote_identifier(identifier):
    escaped_identifier = identifier.replace('"', '""')
    return f'"{escaped_identifier}"'


def _normalized_data_type(data_type):
    return re.sub(r'\s+', '', data_type).upper()


def _native_target_name(target_sf):
    return f'{target_sf["schema"]}."{target_sf["table"].upper()}"'


def _validate_existing_column_types(
    target_sf, source_columns_dict, target_columns_info, primary_keys=(), boundary_column=None, decimal_columns=(),
    version_legacy_float_columns=False,
):
    versions = {}
    primary_keys = {str(key).strip('"').upper() for key in primary_keys or ()}
    decimal_columns = {str(name).strip('"').upper() for name in decimal_columns or ()}
    for name, source_type in source_columns_dict.items():
        metadata = target_columns_info['type_metadata'].get(name)
        if metadata is None:
            continue
        try:
            expected = canonical_native_type(source_type)
            actual = canonical_native_metadata_type(metadata)
        except ValueError as exc:
            raise NativePartialSyncCompatibilityError(
                f'Native PartialSync cannot verify the type of {_native_target_name(target_sf)}.{name}: {exc}'
            ) from exc
        column_name = name[1:-1].replace('""', '"').upper()
        if column_name in decimal_columns and actual != expected:
            if (
                (not version_legacy_float_columns or column_name in primary_keys)
                and is_retained_legacy_decimal_float(actual, expected)
            ):
                continue
            if column_name in primary_keys:
                raise NativePartialSyncCompatibilityError(f'Cannot version primary-key column {name}')
            if boundary_column and name[1:-1].replace('""', '"').upper() == boundary_column.upper():
                raise NativePartialSyncCompatibilityError(
                    f'Cannot version PartialSync boundary column {name}; run FullSync to recreate the target'
                )
            if actual.startswith('NUMBER(') or actual == 'FLOAT':
                versions[name] = source_type
                continue
        if not _native_type_accepts(actual, expected):
            raise NativePartialSyncCompatibilityError(
                f'Native PartialSync cannot safely publish {name} as {expected}: '
                f'existing target {_native_target_name(target_sf)} has type {actual}. '
                'Run a FullSync to recreate the target with the mapped column types, then retry PartialSync.'
            )
    return versions


def _staging_source_columns(
    source_columns_dict, target_columns_info, primary_keys=(), decimal_columns=(), version_legacy_float_columns=False,
):
    """Match retained legacy decimal FLOAT columns in the PartialSync staging table."""
    primary_keys = {str(key).strip('"').upper() for key in primary_keys or ()}
    decimal_columns = {str(name).strip('"').upper() for name in decimal_columns or ()}
    staging_columns = []
    for name, source_type in source_columns_dict.items():
        data_type = source_type
        metadata = target_columns_info['type_metadata'].get(name)
        if metadata is not None and name[1:-1].replace('""', '"').upper() in decimal_columns:
            expected = canonical_native_type(source_type)
            actual = canonical_native_metadata_type(metadata)
            column_name = name[1:-1].replace('""', '"').upper()
            if (
                (not version_legacy_float_columns or column_name in primary_keys)
                and is_retained_legacy_decimal_float(actual, expected)
            ):
                data_type = actual
        staging_columns.append(f'{name} {data_type}')
    return staging_columns


def _native_type_accepts(actual, expected):
    actual_base, expected_base = (value.split('(', maxsplit=1)[0] for value in (actual, expected))
    if actual_base != expected_base:
        return False
    if actual == expected or expected == SNOWFLAKE_MAX_VARCHAR:
        return True
    actual_dimensions, expected_dimensions = (
        tuple(int(value) for value in re.findall(r'\d+', data_type))
        for data_type in (actual, expected)
    )
    if actual_base == 'NUMBER':
        actual_precision, actual_scale = actual_dimensions
        expected_precision, expected_scale = expected_dimensions
        return actual_scale >= expected_scale and actual_precision - actual_scale >= expected_precision - expected_scale
    return bool(actual_dimensions and actual_dimensions[0] >= expected_dimensions[0])


def _get_varchar_columns_to_widen(
    target_sf,
    source_columns_dict,
    target_columns_info,
    decimal_columns=(),
):
    columns_to_widen = []
    decimal_columns = {str(name).strip('"').upper() for name in decimal_columns or ()}
    normalized_max_varchar = _normalized_data_type(SNOWFLAKE_MAX_VARCHAR)
    for source_column, source_type in source_columns_dict.items():
        if _normalized_data_type(source_type) != normalized_max_varchar:
            continue
        target_type = target_columns_info['columns_dict'].get(source_column)
        if target_type is None:
            continue
        if target_type.upper() not in SNOWFLAKE_TEXT_TYPES:
            metadata = target_columns_info['type_metadata'].get(source_column)
            if (
                metadata is not None
                and source_column[1:-1].replace('""', '"').upper() in decimal_columns
                and is_retained_legacy_decimal_float(
                    canonical_native_metadata_type(metadata),
                    canonical_native_type(source_type),
                )
            ):
                continue
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
