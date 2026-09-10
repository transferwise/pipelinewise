import glob
import hashlib
import json
import os
import re

from collections import Counter
from datetime import datetime
from typing import List, Set, Union
from pathlib import Path
from unittest import TestCase
from contextlib import contextmanager

from . import tasks
from . import db


def assert_run_tap_success(
        tap, target, sync_engines, profiling=False,
        expected_state_streams=None):
    """Run a specific tap and make sure that it's using the correct sync engine,
    finished successfully and state file created with the right content"""

    command = f'pipelinewise run_tap --tap {tap} --target {target}'

    if profiling:
        command = f'{command} --profiler'

    [return_code, stdout, stderr] = tasks.run_command(command)
    _assert_run_tap_command_success(return_code, stdout, stderr)
    tasks.assert_run_tap_log_engines(stdout, sync_engines)

    for sync_engine in sync_engines:
        log_file = tasks.find_run_tap_log_file(stdout, sync_engine)
        assert_command_success(return_code, stdout, stderr, log_file)
        if sync_engine == 'singer':
            assert_state_file_valid(
                target, tap, log_file, require_emitted_state=True
            )
            continue

        expected_streams, progress_streams = _state_expectations_for_engine(
            expected_state_streams, sync_engine
        )
        if sync_engine in ('fastsync', 'partialsync'):
            assert_fastsync_state_persisted(log_file, expected_streams)
        assert_state_file_valid(
            target,
            tap,
            log_file,
            expected_streams=expected_streams,
            expected_progress_streams=progress_streams,
        )

    if profiling:
        assert_profiling_stats_files_created(
            stdout, 'run_tap', sync_engines, tap, target
        )


def _assert_run_tap_command_success(return_code, stdout, stderr):
    """Expose all failed engine logs before asserting the expected engine set."""
    if return_code == 0 and stderr == '':
        return

    failed_logs = []
    for log_path in tasks.find_run_tap_log_files(stdout):
        failed_log_path = Path(f'{log_path}.failed')
        if failed_log_path.is_file():
            failed_logs.append(
                f'{failed_log_path}:\n{failed_log_path.read_text(encoding="utf-8")}'
            )

    print(
        f'STDOUT: {stdout}\nSTDERR: {stderr}\nFAILED LOGS: '
        + ('\n'.join(failed_logs) or '<none found>')
    )
    assert False


def assert_resync_tables_success(
        tap, target, profiling=False, expected_streams=None, tables=None,
        sync_engines=('fastsync',), expected_state_streams=None):
    """Resync a specific tap and make sure that it's using the correct sync engine,
    finished successfully and state file created with the right content"""

    command = f'pipelinewise fast_sync --tap {tap} --target {target}'

    if tables:
        command += ' --tables ' + (
            tables if isinstance(tables, str) else ','.join(tables)
        )

    if profiling:
        command = f'{command} --profiler'

    [return_code, stdout, stderr] = tasks.run_command(command)
    tasks.assert_run_tap_log_engines(stdout, sync_engines)

    if expected_state_streams is None and expected_streams is not None:
        expected_state_streams = {'fastsync': set(expected_streams)}

    for sync_engine in sync_engines:
        log_file = tasks.find_run_tap_log_file(stdout, sync_engine)
        assert_command_success(return_code, stdout, stderr, log_file)
        engine_streams, progress_streams = _state_expectations_for_engine(
            expected_state_streams, sync_engine
        )
        assert_fastsync_state_persisted(log_file, engine_streams)
        assert_state_file_valid(
            target,
            tap,
            log_file,
            expected_streams=engine_streams,
            expected_progress_streams=progress_streams,
        )

    if profiling:
        assert_profiling_stats_files_created(
            stdout, 'fast_sync', sync_engines, tap, target
        )


def assert_resync_populates_target(tap_parameters, primary_key):
    """Run FastSync once and prove the target contains the complete fixture."""
    source_table = tap_parameters.get('source_table', tap_parameters['table'])
    qualified_table = f'{tap_parameters["source_db"]}.{source_table}'
    assert_resync_tables_success(
        tap_parameters['tap'],
        tap_parameters['target'],
        expected_streams={_expected_fastsync_state_stream(tap_parameters)},
        tables=qualified_table,
    )

    return assert_source_target_rows_equal(
        tap_parameters,
        primary_key,
        operation='FastSync',
    )


def assert_source_target_rows_equal(
        tap_parameters, primary_key, where_clause=None, operation='sync'):
    """Prove selected deterministic source and Snowflake rows are identical."""

    comparison_columns = tap_parameters['comparison_columns']
    source_expressions = [
        column['source_expression'] for column in comparison_columns
    ]
    target_expressions = [
        column['target_expression'] for column in comparison_columns
    ]
    source_query = {
        'tap_type': tap_parameters['tap_type'],
        'source_db': tap_parameters['source_db'],
        'table': tap_parameters.get('source_table', tap_parameters['table']),
        'columns': source_expressions,
        'primary_key': primary_key,
    }
    target_query = {
        'tap_type': tap_parameters['tap_type'],
        'table': tap_parameters.get('target_table', tap_parameters['table']),
        'columns': target_expressions,
        'primary_key': primary_key,
    }
    if where_clause:
        source_query['where_clause'] = where_clause
        target_query['where_clause'] = where_clause

    expected_records = tap_parameters['env'].get_rows_from_source(
        **source_query
    )
    actual_records = tap_parameters['env'].get_rows_from_target_snowflake(
        **target_query
    )

    assert expected_records, (
        f'Source fixture {tap_parameters["source_db"]}.'
        f'{tap_parameters["table"]} is empty'
    )
    normalized_expected = _normalize_fixture_rows(
        expected_records, comparison_columns, 'source'
    )
    normalized_actual = _normalize_fixture_rows(
        actual_records, comparison_columns, 'target'
    )
    _assert_fixture_rows_equal(
        normalized_actual,
        normalized_expected,
        operation,
        tap_parameters['source_db'],
        tap_parameters['table'],
    )
    return normalized_actual


def _assert_fixture_rows_equal(actual_rows, expected_rows, operation, source_db, table):
    """Report one actionable mismatch without dumping an entire fixture."""
    if actual_rows == expected_rows:
        return

    mismatch_index = next(
        (
            index
            for index, (expected, actual) in enumerate(
                zip(expected_rows, actual_rows)
            )
            if expected != actual
        ),
        min(len(expected_rows), len(actual_rows)),
    )
    expected_row = (
        expected_rows[mismatch_index]
        if mismatch_index < len(expected_rows)
        else '<missing>'
    )
    actual_row = (
        actual_rows[mismatch_index]
        if mismatch_index < len(actual_rows)
        else '<missing>'
    )
    raise AssertionError(
        f'{operation} did not reproduce {source_db}.{table} in the Snowflake '
        f'target: first mismatch at ordered row {mismatch_index + 1}; '
        f'expected {expected_row!r}, got {actual_row!r}; '
        f'expected {len(expected_rows)} rows, got {len(actual_rows)}'
    )


def _normalize_fixture_rows(records, comparison_columns, side):
    """Normalize source/target driver values without weakening comparisons."""
    normalized_records = []
    for row_number, record in enumerate(records, start=1):
        if len(record) != len(comparison_columns):
            raise AssertionError(
                f'{side} fixture row {row_number} has {len(record)} values; '
                f'expected {len(comparison_columns)}'
            )

        normalized_records.append(tuple(
            _normalize_fixture_value(
                value,
                column.get(f'{side}_normalizer', column['normalizer']),
            )
            for value, column in zip(record, comparison_columns)
        ))

    return normalized_records


def _normalize_fixture_value(value, normalizer):
    """Apply one explicit normalization used by a FastSync fixture column."""
    if value is None:
        return None

    if normalizer.startswith('hash_skip_first_'):
        return _normalize_hashed_fixture_value(value, normalizer)

    normalizers = {
        'integer': int,
        'boolean': _normalize_boolean_fixture_value,
        'datetime': _normalize_datetime_fixture_value,
        'json': _normalize_json_fixture_value,
        'text': str,
    }
    try:
        normalize = normalizers[normalizer]
    except KeyError as exc:
        raise ValueError(f'Unsupported fixture normalizer: {normalizer}') from exc
    return normalize(value)


def _normalize_boolean_fixture_value(value):
    """Normalize boolean values returned differently by database drivers."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    normalized = str(value).strip().lower()
    if normalized in ('true', '1'):
        return True
    if normalized in ('false', '0'):
        return False
    raise ValueError(f'Cannot normalize {value!r} as boolean')


def _normalize_datetime_fixture_value(value):
    """Normalize datetime objects and ISO text to the same representation."""
    if isinstance(value, datetime):
        return value.isoformat(sep=' ')
    return str(value).replace('T', ' ')


def _normalize_json_fixture_value(value):
    """Normalize JSON objects and serialized Snowflake VARIANT values."""
    parsed_value = json.loads(value) if isinstance(value, str) else value
    return json.dumps(parsed_value, sort_keys=True, separators=(',', ':'))


def _normalize_hashed_fixture_value(value, normalizer):
    """Apply the configured HASH-SKIP-FIRST transform to source text."""
    if not isinstance(value, str):
        raise TypeError(f'Cannot hash non-string fixture value {value!r}')
    skip_first = int(normalizer.rsplit('_', 1)[-1])
    digest = hashlib.sha256(value[skip_first:].encode('utf-8')).hexdigest()
    return f'{value[:skip_first]}{digest}'


# pylint: disable=invalid-name
def assert_partial_sync_table_success(
        tap_parameters, start_value, end_value=None):
    """Partial sync a specific tap and make sure that it finished successfully and state file is created
    with the right content"""

    state_before = _load_persisted_state(
        tap_parameters['target'], tap_parameters['tap']
    )
    command = _get_command_for_partial_sync(tap_parameters, start_value, end_value)

    [return_code, stdout, stderr] = tasks.run_command(command)
    tasks.assert_run_tap_log_engines(stdout, ('partialsync',))
    log_file = tasks.find_run_tap_log_file(stdout, 'partialsync')
    assert_command_success(return_code, stdout, stderr, log_file)
    _assert_partial_sync_state(
        tap_parameters,
        end_value,
        state_before,
        log_file,
    )


def assert_partial_sync_table_with_target_additional_columns(
        tap_parameters, additional_column,
        start_value, end_value):
    """Assert partial sync table command with additional column in the target"""

    # Add a new column in the target
    tap_parameters['env'].add_column_into_target_sf(
        tap_type=tap_parameters['tap_type'],
        table=tap_parameters['table'],
        new_column=additional_column
    )

    state_before = _load_persisted_state(
        tap_parameters['target'], tap_parameters['tap']
    )
    command = _get_command_for_partial_sync(tap_parameters, start_value, end_value)

    [return_code, stdout, stderr] = tasks.run_command(command)
    tasks.assert_run_tap_log_engines(stdout, ('partialsync',))
    log_file = tasks.find_run_tap_log_file(stdout, 'partialsync')
    assert_command_success(return_code, stdout, stderr, log_file)
    _assert_partial_sync_state(
        tap_parameters,
        end_value,
        state_before,
        log_file,
    )


def assert_partial_sync_table_with_source_additional_columns(
        tap_parameters, additional_column,
        start_value, end_value):
    """Assert partial sync table command with additional columns in the source"""

    # Add a new column in the source
    tap_parameters['env'].add_column_into_source(
        tap_type=tap_parameters['tap_type'],
        table=tap_parameters['table'],
        new_column=additional_column
    )

    state_before = _load_persisted_state(
        tap_parameters['target'], tap_parameters['tap']
    )
    command = _get_command_for_partial_sync(tap_parameters, start_value, end_value)

    [return_code, stdout, stderr] = tasks.run_command(command)
    tasks.assert_run_tap_log_engines(stdout, ('partialsync',))
    log_file = tasks.find_run_tap_log_file(stdout, 'partialsync')
    assert_command_success(return_code, stdout, stderr, log_file)
    _assert_partial_sync_state(
        tap_parameters,
        end_value,
        state_before,
        log_file,
    )


def assert_partial_sync_rows_in_target(env, tap_type, table, column, primary_key, expected_column_values):
    """Assert only expected rows are synced in the target snowflake"""
    records = env.get_records_from_target_snowflake(
        tap_type=tap_type, table=table, column=column, primary_key=primary_key
    )
    list_of_column_values = [column[0] for column in records]
    assert expected_column_values == list_of_column_values


def assert_command_success(return_code, stdout, stderr, log_path=None):
    """Assert helper function to check if command finished successfully.
    In case of failure it logs stdout, stderr and content of the failed command log
    if exists"""
    if return_code != 0 or stderr != '':
        failed_log = ''
        failed_log_path = f'{log_path}.failed'
        # Load failed log file if exists
        if os.path.isfile(failed_log_path):
            with open(failed_log_path, 'r', encoding='utf-8') as file:
                failed_log = file.read()

        print(f'STDOUT: {stdout}\nSTDERR: {stderr}\nFAILED LOG: {failed_log}')
        assert False

    # check success log file if log path defined
    success_log_path = f'{log_path}.success'
    if log_path and not os.path.isfile(success_log_path):
        assert False
    else:
        assert True


def _expected_fastsync_state_stream(tap_parameters):
    """Return the state stream id written by FastSync for one source table."""
    table = tap_parameters.get('source_table', tap_parameters['table'])
    return f'{tap_parameters["source_db"]}-{table.strip(chr(34))}'


def _state_expectations_for_engine(expected_state_streams, sync_engine):
    """Return exact stream markers and the subset that require progress."""
    if expected_state_streams is None or sync_engine not in expected_state_streams:
        return None, None

    expectations = expected_state_streams[sync_engine]
    if isinstance(expectations, dict):
        return (
            set(expectations),
            {
                stream_id
                for stream_id, requires_progress in expectations.items()
                if requires_progress
            },
        )
    streams = set(expectations)
    return streams, streams


def _has_fastsync_progress(bookmark):
    """Recognize a usable log-based or incremental FastSync bookmark."""
    if not isinstance(bookmark, dict):
        return False
    if bookmark.get('gtid') or bookmark.get('lsn') or bookmark.get('modified_since'):
        return True
    if bookmark.get('log_file') and bookmark.get('log_pos') is not None:
        return True
    if bookmark.get('replication_key') and 'replication_key_value' in bookmark:
        return True
    token = bookmark.get('token')
    return isinstance(token, dict) and bool(token.get('_data'))


def assert_fastsync_state_persisted(log_path, expected_streams=None):
    """Prove this FastSync process completed its own atomic state write."""
    with open(f'{log_path}.success', encoding='utf-8') as log_file:
        persisted_streams = re.findall(
            r'^(?:INFO |.* log_level=INFO message=)'
            r'FastSync state updated for stream: (.+)$',
            log_file.read(),
            flags=re.MULTILINE,
        )

    assert persisted_streams
    persisted_counts = Counter(persisted_streams)
    if expected_streams is not None:
        assert persisted_counts == Counter({stream: 1 for stream in expected_streams})
    else:
        assert all(count == 1 for count in persisted_counts.values())


def _state_file_path(target_name, tap_name):
    return Path(
        f'{Path.home()}/.pipelinewise/{target_name}/{tap_name}/state.json'
    ).resolve()


def _load_persisted_state(target_name, tap_name):
    state_file = _state_file_path(target_name, tap_name)
    if not state_file.is_file():
        return None
    with state_file.open(encoding='utf-8') as state_handle:
        return json.load(state_handle)


def _last_emitted_state(log_path):
    """Parse the last real target state from bare JSON or legacy log text."""
    with open(f'{log_path}.success', 'r', encoding='utf-8') as log_file:
        log_content = log_file.read()

    parsed_states = []
    for line in log_content.splitlines():
        try:
            candidate = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict) and (
            'bookmarks' in candidate or 'currently_syncing' in candidate
        ):
            parsed_states.append(candidate)
    if parsed_states:
        return parsed_states[-1]

    emitted_states = re.findall(
        r'^(?:INFO |.* log_level=INFO message=)'
        r'STATE emitted from target: (.+)$',
        log_content,
        flags=re.MULTILINE,
    )
    return json.loads(emitted_states[-1]) if emitted_states else None


def _assert_partial_sync_state(
    tap_parameters,
    end_value,
    state_before,
    log_path,
):
    """Require an unbounded bookmark write or an unchanged bounded state."""
    target = tap_parameters['target']
    tap = tap_parameters['tap']
    if end_value is not None:
        assert _load_persisted_state(target, tap) == state_before
        return

    expected_streams = {_expected_fastsync_state_stream(tap_parameters)}
    assert_fastsync_state_persisted(log_path, expected_streams)
    assert_state_file_valid(
        target,
        tap,
        expected_streams=expected_streams,
        expected_progress_streams=expected_streams,
    )


def assert_state_file_valid(
        target_name, tap_name, log_path=None, expected_streams=None,
        expected_progress_streams=None, require_emitted_state=False):
    """Assert the persisted state is valid and matches emitted Singer state."""
    state_file = _state_file_path(target_name, tap_name)
    assert os.path.isfile(state_file)

    with open(state_file, encoding='utf-8') as state_f:
        persisted_state = json.load(state_f)

    assert isinstance(persisted_state, dict)

    # Check if state file content equals to last emitted state in log.
    # Targets emit state as a bare JSON line; retain the prefixed parser for
    # compatibility with older/custom targets.
    state_in_log = _last_emitted_state(log_path) if log_path else None

    if require_emitted_state:
        assert state_in_log is not None

    if state_in_log is not None:
        assert persisted_state == state_in_log

    bookmarks = persisted_state.get('bookmarks')
    assert isinstance(bookmarks, dict) and bookmarks
    assert all(
        isinstance(stream_id, str)
        and stream_id
        and isinstance(bookmark, dict)
        for stream_id, bookmark in bookmarks.items()
    )
    if expected_streams is not None:
        assert set(expected_streams).issubset(bookmarks)
    if expected_progress_streams:
        assert all(
            _has_fastsync_progress(bookmarks[stream_id])
            for stream_id in expected_progress_streams
        )


def assert_cols_in_table(
    query_runner_fn: callable, table_schema: str, table_name: str, columns: List[str], schema_postfix: str = ''
):
    """Fetches the given table's columns from information_schema and
    tests if every given column is in the result

    :param query_runner_fn: method to run queries
    :param table_schema: search table in this schema
    :param table_name: table with the columns
    :param columns: list of columns to check if there are in the table's columns
    :param schema_postfix: schema postfix for snowflake target
    """
    funcs = _map_tap_to_target_functions(None, query_runner_fn, schema_postfix)
    sql_get_columns_for_table_fn = funcs.get(
        'target_sql_get_table_cols_fn', db.sql_get_columns_for_table
    )
    sql = sql_get_columns_for_table_fn(table_schema, table_name)
    result = query_runner_fn(sql)
    cols = [res[0] for res in result]
    try:
        assert all(col in cols for col in columns)
    except AssertionError as ex:
        ex.args += (
            'Error',
            columns,
            f'One ore more columns not found in target table {table_name}',
        )
        raise


def _run_sql(query_runner_fn: callable, sql_query: str) -> List:
    """Run an SQL query by a query runner function"""
    return list(query_runner_fn(sql_query))


def _map_tap_to_target_functions(
    tap_query_runner_fn: callable, target_query_runner_fn: callable, schema_postfix: str = ''
) -> dict:
    """Takes two query runner methods and creates a map with the compatible database
    specific functions that required to run assertions.

    :param tap_query_runner_fn: method to run queries in the first connection
    :param target_query_runner_fn: method to run queries in the second connection
    :return: Dictionary of the functions to use for the tap-target pair
    """
    f_map = {
        # tap-mysql specific attributes and functions
        'run_query_tap_mysql': {
            'source_schemas': ['mysql_source_db'],
            'target_schemas': [f'ppw_e2e_tap_mysql{schema_postfix}'],
            'source_sql_get_cols_fn': db.sql_get_columns_mysql,
            'source_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_mysql,
        },
        # tap-mysql specific attributes and functions
        'run_query_tap_mysql_2': {
            'source_schemas': ['mysql_source_db_2'],
            'target_schemas': [f'ppw_e2e_tap_mysql_2{schema_postfix}'],
            'source_sql_get_cols_fn': db.sql_get_columns_mysql,
            'source_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_mysql,
        },
        # tap-postgres specific attributes and functions
        'run_query_tap_postgres': {
            'source_schemas': ['public', 'public2'],
            'target_schemas': [f'ppw_e2e_tap_postgres{schema_postfix}',
                               f'ppw_e2e_tap_postgres_public2{schema_postfix}'],
            'source_sql_get_cols_fn': db.sql_get_columns_postgres,
            'source_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_postgres,
        },
        # tap-yugabyte specific attributes and functions
        'run_query_tap_yugabyte': {
            'source_schemas': ['public', 'public2'],
            'target_schemas': [f'ppw_e2e_tap_yugabyte{schema_postfix}',
                                f'ppw_e2e_tap_yugabyte_public2{schema_postfix}'],
            'source_sql_get_cols_fn': db.sql_get_columns_postgres,
            'source_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_postgres,
        },
        # target-postgres specific attributes and functions
        'run_query_target_postgres': {
            'target_sql_get_cols_fn': db.sql_get_columns_postgres,
            'target_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_postgres,
        },
        # target-snowflake specific attributes and functions
        'run_query_target_snowflake': {
            'target_sql_get_cols_fn': db.sql_get_columns_snowflake,
            'target_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_snowflake,
        },
    }

    # Merge the keys into one dict by tap and target query runner names
    if tap_query_runner_fn:
        return {
            **f_map[tap_query_runner_fn.__name__],
            **f_map[target_query_runner_fn.__name__],
        }
    return {**f_map[target_query_runner_fn.__name__]}


def assert_row_counts_equal(
    tap_query_runner_fn: callable, target_query_runner_fn: callable, schema_postfix: str = ''
) -> None:
    """Takes two query runner methods, counts the row numbers in every table in both the
    source and target databases and tests if the row counts are matching.

    :param tap_query_runner_fn: method to run queries in the first connection
    :param target_query_runner_fn: method to run queries in the second connection
    :param schema_postfix: schema postfix for snowflake target"""
    # Generate a map of source and target specific functions
    funcs = _map_tap_to_target_functions(tap_query_runner_fn, target_query_runner_fn, schema_postfix)

    # Get source and target schemas
    source_schemas = funcs['source_schemas']
    target_schemas = funcs['target_schemas']

    # Generate a dynamic SQLs to count rows in source and target databases
    source_dynamic_sql_row_count = funcs['source_sql_dynamic_row_count_fn'](
        source_schemas
    )
    target_dynamic_sql_row_count = funcs['target_sql_dynamic_row_count_fn'](
        target_schemas
    )

    # Count rows
    source_sql_row_count = _run_sql(tap_query_runner_fn, source_dynamic_sql_row_count)[
        0
    ][0]
    target_sql_row_count = _run_sql(
        target_query_runner_fn, target_dynamic_sql_row_count
    )[0][0]

    # Run the generated SQLs
    row_counts_in_source = _run_sql(tap_query_runner_fn, source_sql_row_count)
    row_counts_in_target = _run_sql(target_query_runner_fn, target_sql_row_count)

    # Some sources and targets can't be compared directly (e.g. some targets don't accept spaces in table names)
    # we fix that by renaming the source tables to names that the target would accept
    if 'target_sql_safe_name_fn' in funcs:
        row_counts_in_source = [
            (funcs['target_sql_safe_name_fn'](table), row_count)
            for (table, row_count) in row_counts_in_source
        ]

    # Compare the two dataset
    assert row_counts_in_target == row_counts_in_source


# pylint: disable=too-many-locals
def assert_all_columns_exist(
    tap_query_runner_fn: callable,
    target_query_runner_fn: callable,
    column_type_mapper_fn: callable = None,
    ignore_cols: Union[Set, List] = None,
    schema_postfix: str = '',
) -> None:
    """Takes two query runner methods, gets the columns list for every table in both the
    source and target database and tests if every column in source exists in the target database.
    Some taps have unsupported column types and these are not part of the schemas published to the target thus
    target table doesn't have such columns.

    :param tap_query_runner_fn: method to run queries in the first connection
    :param target_query_runner_fn: method to run queries in the second connection
    :param column_type_mapper_fn: method to convert source to target column types
    :param ignore_cols: List or set of columns to ignore if we know target table won't have them
    :param schema_postfix: Schema postfix for Snowflake targets"""

    if ignore_cols is None:
        ignore_cols = []

    # Generate a map of source and target specific functions
    funcs = _map_tap_to_target_functions(
        tap_query_runner_fn, target_query_runner_fn, schema_postfix
    )

    # Get source and target schemas
    source_schemas = funcs['source_schemas']
    target_schemas = funcs['target_schemas']

    # Generate SQLs to get columns from source and target databases
    source_sql_get_cols = funcs['source_sql_get_cols_fn'](source_schemas)
    target_sql_get_cols = funcs['target_sql_get_cols_fn'](target_schemas)

    # Run the generated SQLs
    source_table_cols_raw = _run_sql(tap_query_runner_fn, source_sql_get_cols)
    target_table_cols_raw = _run_sql(target_query_runner_fn, target_sql_get_cols)

    def _cols_list_to_dict(cols: List) -> dict:
        """
        Converts list of columns with char separators to dictionary

        :param cols: list of ':' separated strings using the format of
                     column_name:column_type:column_type_extra
        :return: Dictionary of columns where key is the column_name
        """
        cols_dict = {}
        for col in cols:
            col_props = col.split(':')
            cols_dict[col_props[0]] = {
                'type': col_props[1],
                'type_extra': col_props[2],
            }

        return cols_dict

    # *_table_cols is a list of lists
    # each individual list is [table_name, table_columns_information]
    source_table_columns_map = {
        table[0].lower(): _cols_list_to_dict(table[1].lower().split(';'))
        for table in source_table_cols_raw
    }
    target_table_columns_map = {
        table[0].lower(): _cols_list_to_dict(table[1].lower().split(';'))
        for table in target_table_cols_raw
    }

    for source_table_name, source_table_columns in source_table_columns_map.items():

        # Some sources and targets can't be compared directly (e.g. some targets don't accept spaces in table names)
        # we fix that by renaming the source tables to names that the target would accept
        if 'target_sql_safe_name_fn' in funcs:
            source_table_name = funcs['target_sql_safe_name_fn'](source_table_name)

        if source_table_name not in target_table_columns_map:
            raise Exception(f'table "{source_table_name}" not found in target')

        target_table_columns = target_table_columns_map[source_table_name]

        for source_column_name, source_column_type_info in source_table_columns.items():
            if source_column_name in ignore_cols:
                continue

            if source_column_name not in target_table_columns:
                raise Exception(
                    f'"{source_column_name}" column not found in target table "{source_table_name}"'
                )

            target_column_type_info = target_table_columns[source_column_name]

            if column_type_mapper_fn is None:
                continue

            expected_target_column_type = (
                column_type_mapper_fn(
                    source_column_type_info['type'],
                    source_column_type_info['type_extra'],
                )
                .replace(' NULL', '')
                .lower()
            )

            actual_target_column_type = target_column_type_info['type'].lower()

            if actual_target_column_type != expected_target_column_type:
                raise Exception(
                    f'{source_column_name} column type is not as expected. '
                    f'Expected: {expected_target_column_type} '
                    f'Actual: {actual_target_column_type}'
                )


def assert_date_column_naive_in_target(
    target_query_runner_fn, column_name, full_table_name
):
    """
    Checks if all dates in the given column are naive,i.e no timezone
    Args:
        target_query_runner_fn: target query runner callable
        column_name: column of timestamp type
        full_table_name: fully qualified table name
    """
    dates = target_query_runner_fn(f'SELECT {column_name} FROM {full_table_name};')

    for date in dates:
        if date[0] is not None:
            assert date[0].tzinfo is None


def assert_profiling_stats_files_created(
    stdout: str,
    command: str,
    sync_engines: List = None,
    tap: Union[str, List[str]] = None,
    target: str = None,
):
    """
    Asserts that profiling pstat files were created by checking their existence
    Args:
        stdout: ppw command stdout
        command: ppw command name
        sync_engines: in case of run_tap or fast_sync, sync engines should be fastsync and/or singer
        tap: in case of run_tap or fast_sync, tap is the tap ID
        target: in case of run_tap or fast_sync, it is the target ID
    """
    # find profiling directory from output
    profiler_dir = tasks.find_profiling_folder(stdout)

    # crawl the folder looking for pstat files and strip the folder name from the file name
    pstat_files = {
        file[len(f'{profiler_dir}/'):]
        for file in glob.iglob(f'{profiler_dir}/*.pstat')
    }

    assert f'pipelinewise_{command}.pstat' in pstat_files

    if sync_engines is not None:
        if 'fastsync' in sync_engines:
            assert f'fastsync_{tap}_{target}.pstat' in pstat_files

        if 'singer' in sync_engines:
            assert f'tap_{tap}.pstat' in pstat_files
            assert f'target_{target}.pstat' in pstat_files

    if isinstance(tap, list):
        for tap_ in tap:
            assert f'tap_{tap_}.pstat' in pstat_files


# pylint: disable=raise-missing-from
@contextmanager
def assert_not_raises(exc_type):
    """Assert exception not raised"""
    try:
        yield None
    except exc_type:
        raise TestCase.failureException(f'{exc_type.__name__} raised!')


def assert_record_count_in_sf(env, tap_type, table, expected_records, where_clause=''):
    """Assert record count in target Snowflake"""
    result = env.run_query_target_snowflake(
        f'SELECT count(1) FROM ppw_e2e_{tap_type}{env.sf_schema_postfix}."{table.upper()}" {where_clause};'
    )[0][0]
    assert result == expected_records


def _get_command_for_partial_sync(tap_parameters, start_value, end_value=None):
    command = f'pipelinewise partial_sync_table --tap {tap_parameters["tap"]} --target {tap_parameters["target"]}' \
              f' --table {tap_parameters["source_db"]}.{tap_parameters["table"]} --column {tap_parameters["column"]}' \
              f' --start_value {start_value}'

    if end_value is not None:
        command += f' --end_value {end_value}'

    return command
