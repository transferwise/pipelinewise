import glob
import os
import re

from typing import List, Set, Union
from pathlib import Path

from . import tasks
from . import db


def assert_run_tap_success(tap, target, sync_engines, profiling=False):
    """Run a specific tap and make sure that it's using the correct sync engine,
    finished successfully and state file created with the right content"""

    command = f'pipelinewise run_tap --tap {tap} --target {target}'

    if profiling:
        command = f'{command} --profiler'

    [return_code, stdout, stderr] = tasks.run_command(command)

    for sync_engine in sync_engines:
        log_file = tasks.find_run_tap_log_file(stdout, sync_engine)
        assert_command_success(return_code, stdout, stderr, log_file)
        assert_state_file_valid(target, tap, log_file)

    if profiling:
        assert_profiling_stats_files_created(stdout, 'run_tap', sync_engines, tap, target)


def assert_resync_tables_success(tap, target, profiling=False):
    """Resync a specific tap and make sure that it's using the correct sync engine,
    finished successfully and state file created with the right content"""

    command = f'pipelinewise sync_tables --tap {tap} --target {target}'

    if profiling:
        command = f'{command} --profiler'

    [return_code, stdout, stderr] = tasks.run_command(command)

    log_file = tasks.find_run_tap_log_file(stdout, 'fastsync')
    assert_command_success(return_code, stdout, stderr, log_file)
    assert_state_file_valid(target, tap, log_file)

    if profiling:
        assert_profiling_stats_files_created(stdout, 'sync_tables', ['fastsync'], tap, target)


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


def assert_state_file_valid(target_name, tap_name, log_path=None):
    """Assert helper function to check if state file exists for
    a certain tap for a certain target"""
    state_file = Path(f'{Path.home()}/.pipelinewise/{target_name}/{tap_name}/state.json').resolve()
    assert os.path.isfile(state_file)

    # Check if state file content equals to last emitted state in log
    if log_path:
        success_log_path = f'{log_path}.success'
        state_in_log = None
        with open(success_log_path, 'r', encoding='utf-8') as log_f:
            state_log_pattern = re.search(r'\nINFO STATE emitted from target: (.+\n)', '\n'.join(log_f.readlines()))
            if state_log_pattern:
                state_in_log = state_log_pattern.groups()[-1]

        # If the emitted state message exists in the log then compare it to the actual state file
        if state_in_log:
            with open(state_file, 'r', encoding='utf-8') as state_f:
                assert state_in_log == ''.join(state_f.readlines())


def assert_cols_in_table(query_runner_fn: callable, table_schema: str, table_name: str, columns: List[str]):
    """Fetches the given table's columns from information_schema and
    tests if every given column is in the result

    :param query_runner_fn: method to run queries
    :param table_schema: search table in this schema
    :param table_name: table with the columns
    :param columns: list of columns to check if there are in the table's columns
    """
    funcs = _map_tap_to_target_functions(None, query_runner_fn)
    sql_get_columns_for_table_fn = funcs.get('target_sql_get_table_cols_fn', db.sql_get_columns_for_table)
    sql = sql_get_columns_for_table_fn(table_schema, table_name)
    result = query_runner_fn(sql)
    cols = [res[0] for res in result]
    try:
        assert all(col in cols for col in columns)
    except AssertionError as ex:
        ex.args += ('Error', columns, f'One ore more columns not found in target table {table_name}')
        raise


def _run_sql(query_runner_fn: callable, sql_query: str) -> List:
    """Run an SQL query by a query runner function"""
    return list(query_runner_fn(sql_query))


def _map_tap_to_target_functions(tap_query_runner_fn: callable, target_query_runner_fn: callable) -> dict:
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
            'target_schemas': ['ppw_e2e_tap_mysql'],
            'source_sql_get_cols_fn': db.sql_get_columns_mysql,
            'source_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_mysql
        },
        # tap-postgres specific attributes and functions
        'run_query_tap_postgres': {
            'source_schemas': ['public', 'public2'],
            'target_schemas': ['ppw_e2e_tap_postgres', 'ppw_e2e_tap_postgres_public2'],
            'source_sql_get_cols_fn': db.sql_get_columns_postgres,
            'source_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_postgres
        },
        # target-postgres specific attributes and functions
        'run_query_target_postgres': {
            'target_sql_get_cols_fn': db.sql_get_columns_postgres,
            'target_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_postgres
        },
        # target-snowflake specific attributes and functions
        'run_query_target_snowflake': {
            'target_sql_get_cols_fn': db.sql_get_columns_snowflake,
            'target_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_snowflake,
        },
        # target-bigquery specific attributes and functions
        'run_query_target_bigquery': {
            'target_sql_get_cols_fn': db.sql_get_columns_bigquery,
            'target_sql_get_table_cols_fn': db.sql_get_columns_for_table_bigquery,
            'target_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_bigquery,
            'target_sql_safe_name_fn': db.safe_name_bigquery,
        },
        # target-redshift specific attributes and functions
        'run_query_target_redshift': {
            'target_sql_get_cols_fn': db.sql_get_columns_redshift,
            'target_sql_dynamic_row_count_fn': db.sql_dynamic_row_count_redshift,
        }
    }

    # Merge the keys into one dict by tap and target query runner names
    if tap_query_runner_fn:
        return {**f_map[tap_query_runner_fn.__name__], **f_map[target_query_runner_fn.__name__]}
    return {**f_map[target_query_runner_fn.__name__]}


def assert_row_counts_equal(tap_query_runner_fn: callable, target_query_runner_fn: callable) -> None:
    """Takes two query runner methods, counts the row numbers in every table in both the
    source and target databases and tests if the row counts are matching.

    :param tap_query_runner_fn: method to run queries in the first connection
    :param target_query_runner_fn: method to run queries in the second connection"""
    # Generate a map of source and target specific functions
    funcs = _map_tap_to_target_functions(tap_query_runner_fn, target_query_runner_fn)

    # Get source and target schemas
    source_schemas = funcs['source_schemas']
    target_schemas = funcs['target_schemas']

    # Generate a dynamic SQLs to count rows in source and target databases
    source_dynamic_sql_row_count = funcs['source_sql_dynamic_row_count_fn'](source_schemas)
    target_dynamic_sql_row_count = funcs['target_sql_dynamic_row_count_fn'](target_schemas)

    # Count rows
    source_sql_row_count = _run_sql(tap_query_runner_fn, source_dynamic_sql_row_count)[0][0]
    target_sql_row_count = _run_sql(target_query_runner_fn, target_dynamic_sql_row_count)[0][0]

    # Run the generated SQLs
    row_counts_in_source = _run_sql(tap_query_runner_fn, source_sql_row_count)
    row_counts_in_target = _run_sql(target_query_runner_fn, target_sql_row_count)

    # Some sources and targets can't be compared directly (e.g. BigQuery doesn't accept spaces in table names)
    # we fix that by renaming the source tables to names that the target would accept
    if 'target_sql_safe_name_fn' in funcs:
        row_counts_in_source = [
          (
            funcs['target_sql_safe_name_fn'](table),
            row_count
          )
          for (table,row_count) in row_counts_in_source
        ]

    # Compare the two dataset
    assert row_counts_in_target == row_counts_in_source


# pylint: disable=too-many-locals
def assert_all_columns_exist(tap_query_runner_fn: callable,
                             target_query_runner_fn: callable,
                             column_type_mapper_fn: callable = None,
                             ignore_cols: Union[Set, List] = None) -> None:
    """Takes two query runner methods, gets the columns list for every table in both the
    source and target database and tests if every column in source exists in the target database.
    Some taps have unsupported column types and these are not part of the schemas published to the target thus
    target table doesn't have such columns.

    :param tap_query_runner_fn: method to run queries in the first connection
    :param target_query_runner_fn: method to run queries in the second connection
    :param column_type_mapper_fn: method to convert source to target column types
    :param ignore_cols: List or set of columns to ignore if we know target table won't have them"""
    # Generate a map of source and target specific functions
    funcs = _map_tap_to_target_functions(tap_query_runner_fn, target_query_runner_fn)

    # Get source and target schemas
    source_schemas = funcs['source_schemas']
    target_schemas = funcs['target_schemas']

    # Generate SQLs to get columns from source and target databases
    source_sql_get_cols = funcs['source_sql_get_cols_fn'](source_schemas)
    target_sql_get_cols = funcs['target_sql_get_cols_fn'](target_schemas)

    # Run the generated SQLs
    source_table_cols = _run_sql(tap_query_runner_fn, source_sql_get_cols)
    target_table_cols = _run_sql(target_query_runner_fn, target_sql_get_cols)

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
                'type_extra': col_props[2]
            }

        return cols_dict

    # Compare the two dataset
    for table_cols in source_table_cols:
        table_to_check = table_cols[0].lower()

        # Some sources and targets can't be compared directly (e.g. BigQuery doesn't accept spaces in table names)
        # we fix that by renaming the source tables to names that the target would accept
        if 'target_sql_safe_name_fn' in funcs:
            table_to_check = funcs['target_sql_safe_name_fn'](table_to_check)

        source_cols = table_cols[1].lower().split(';')

        try:
            target_cols = next(t[1] for t in target_table_cols if t[0].lower() == table_to_check).lower().split(';')
        except StopIteration as ex:
            ex.args += ('Error', f'{table_to_check} table not found in target')
            raise

        source_cols_dict = _cols_list_to_dict(source_cols)
        target_cols_dict = _cols_list_to_dict(target_cols)
        print(target_cols_dict)
        for col_name, col_props in source_cols_dict.items():
            # Check if column exists in the target table

            if ignore_cols and col_name in ignore_cols:
                continue

            try:
                assert col_name in target_cols_dict
            except AssertionError as ex:
                ex.args += ('Error', f'{col_name} column not found in target table {table_to_check}')
                raise

            # Check if column type is expected in the target table, if mapper function provided
            if column_type_mapper_fn:
                try:
                    target_col = target_cols_dict[col_name]
                    exp_col_type = column_type_mapper_fn(col_props['type'], col_props['type_extra']) \
                        .replace(' NULL', '').lower()
                    act_col_type = target_col['type'].lower()
                    assert act_col_type == exp_col_type
                except AssertionError as ex:
                    ex.args += ('Error', f'{col_name} column type is not as expected. '
                                         f'Expected: {exp_col_type} '
                                         f'Actual: {act_col_type}')
                    raise


def assert_date_column_naive_in_target(target_query_runner_fn, column_name, full_table_name):
    """
    Checks if all dates in the given column are naive,i.e no timezone
    Args:
        target_query_runner_fn: target query runner callable
        column_name: column of timestamp type
        full_table_name: fully qualified table name
    """
    dates = target_query_runner_fn(
        f'SELECT {column_name} FROM {full_table_name};')

    for date in dates:
        if date[0] is not None:
            assert date[0].tzinfo is None


def assert_profiling_stats_files_created(stdout: str,
                                         command: str,
                                         sync_engines: List = None,
                                         tap: Union[str, List[str]] = None,
                                         target: str = None):
    """
    Asserts that profiling pstat files were created by checking their existence
    Args:
        stdout: ppw command stdout
        command: ppw command name
        sync_engines: in case of run_tap or sync_tables, sync engines should be fastsync and/or singer
        tap: in case of run_tap or sync_tables, tap is the tap ID
        target: in case of run_tap or sync_tables, it is the target ID
    """
    # find profiling directory from output
    profiler_dir = tasks.find_profiling_folder(stdout)

    # crawl the folder looking for pstat files and strip the folder name from the file name
    pstat_files = {file[len(f'{profiler_dir}/'):] for file in glob.iglob(f'{profiler_dir}/*.pstat')}

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
