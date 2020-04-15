import os
import re

from typing import List
from pathlib import Path

from . import tasks


def assert_run_tap_success(tap, target, sync_engines):
    """Run a specific tap and make sure that it's using the correct sync engine,
    finished successfully and state file created with the right content"""
    [return_code, stdout, stderr] = tasks.run_command(f'pipelinewise run_tap --tap {tap} --target {target}')
    for sync_engine in sync_engines:
        log_file = tasks.find_run_tap_log_file(stdout, sync_engine)
        assert_command_success(return_code, stdout, stderr, log_file)
        assert_state_file_valid(target, tap, log_file)

def assert_command_success(return_code, stdout, stderr, log_path=None):
    """Assert helper function to check if command finished successfully.
    In case of failure it logs stdout, stderr and content of the failed command log
    if exists"""
    if return_code != 0 or stderr != '':
        failed_log = ''
        failed_log_path = f'{log_path}.failed'
        # Load failed log file if exists
        if os.path.isfile(failed_log_path):
            with open(failed_log_path, 'r') as file:
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
        with open(success_log_path, 'r') as log_f:
            state_log_pattern = re.search(r'\nINFO STATE emitted from target: (.+\n)', '\n'.join(log_f.readlines()))
            if state_log_pattern:
                state_in_log = state_log_pattern.groups()[-1]

        # If the emitted state message exists in the log then compare it to the actual state file
        if state_in_log:
            with open(state_file, 'r') as state_f:
                assert state_in_log == ''.join(state_f.readlines())


def assert_cols_in_table(query_runner_fn: callable, table_name: str, columns: List[str]):
    """
    fetches the given table's columns from pipelinewise.columns and tests if every given column
    is in the result
    Args:
        run_query_fn: callable function to run query
        table_name: table whose columns are to be fetched
        columns: list of columns to check if there are in the table's columns

    Returns:
        None
    """
    sql = f'SELECT COLUMN_NAME from information_schema.columns where table_name=\'{table_name.upper()}\''

    result = query_runner_fn(sql)
    cols = [res[0] for res in result]
    assert all([col in cols for col in columns])

def assert_tap_mysql_row_count_equals(tap_mysql_query_runner_fn: callable, target_query_runner_fn: callable):
    """Count the rows in tap mysql and in a target database
    and compare row counts"""
    row_counts_in_tap_mysql = tap_mysql_query_runner_fn("""
    SELECT tbl, row_count
      FROM (      SELECT 'address'     AS tbl, COUNT(*) AS row_count FROM address
            UNION SELECT 'area_code'   AS tbl, COUNT(*) AS row_count FROM area_code
            UNION SELECT 'order'       AS tbl, COUNT(*) AS row_count FROM `order`
            UNION SELECT 'weight_unit' AS tbl, COUNT(*) AS row_count FROM weight_unit) x
     ORDER BY tbl, row_count
    """)

    row_counts_in_target_postgres = target_query_runner_fn("""
    SELECT tbl, row_count
      FROM (      SELECT 'address'     AS tbl, COUNT(*) AS row_count FROM mysql_grp24.address
            UNION SELECT 'area_code'   AS tbl, COUNT(*) AS row_count FROM mysql_grp24.area_code
            UNION SELECT 'order'       AS tbl, COUNT(*) AS row_count FROM mysql_grp24.order
            UNION SELECT 'weight_unit' AS tbl, COUNT(*) AS row_count FROM mysql_grp24.weight_unit) x
     ORDER BY tbl, row_count
    """)

    # Compare the results from source and target databases
    assert row_counts_in_target_postgres == row_counts_in_tap_mysql
