import os
import glob
import shutil
import pytest
import re

from typing import List

from . import e2e_utils

from pathlib import Path

DIR = os.path.dirname(__file__)
USER_HOME = os.path.expanduser('~')
CONFIG_DIR = os.path.join(USER_HOME, '.pipelinewise')


# pylint: disable=no-self-use,attribute-defined-outside-init
class TestE2E:
    """
    End to end tests
    """

    def setup_method(self):
        """Init test project and test database"""
        self.env = e2e_utils.load_env()
        self.project_dir = os.path.join(DIR, 'test-project')
        self.init_test_project_dir()

    def teardown_method(self):
        """Delete test directories and database objects"""


    def init_test_project_dir(self):
        """Load every YML template from test-project directory, replace the environment
        variables to real values and save as consumable YAML files"""
        yml_templates = glob.glob(f'{self.project_dir}/*.yml.template')
        for template_path in yml_templates:
            with open(template_path, 'r') as file:
                yaml = file.read()

                # Replace environment variables with string replace. PyYAML can't do it automatically
                for env_var in self.env:
                    yaml = yaml.replace(f'${{{env_var}}}', self.env[env_var])

            yaml_path = template_path.replace('.template', '')
            with open(yaml_path, 'w+') as file:
                file.write(yaml)

    def clean_tap_mysql(self):
        """Clean mysql source"""
        # Delete extra rows added by previous test
        e2e_utils.run_query_tap_mysql(self.env, 'DELETE FROM address where address_id >= 10000')
        e2e_utils.run_query_tap_mysql(self.env, 'DELETE FROM weight_unit where weight_unit_id >= 100')
        e2e_utils.run_query_tap_mysql(self.env, 'DELETE FROM area_code where area_code_id >= 100')

    def clean_target_postgres(self):
        """Clean postgres_dwh"""
        # Drop target schemas if exists
        e2e_utils.run_query_target_postgres(self.env, 'DROP SCHEMA IF EXISTS mysql_grp24 CASCADE')
        e2e_utils.run_query_target_postgres(self.env, 'DROP SCHEMA IF EXISTS postgres_world CASCADE')

        # Create groups required for tests
        e2e_utils.run_query_target_postgres(self.env, 'DROP GROUP IF EXISTS group1')
        e2e_utils.run_query_target_postgres(self.env, 'CREATE GROUP group1')

        # Clean config directory
        shutil.rmtree(os.path.join(CONFIG_DIR, 'postgres_dwh'), ignore_errors=True)

    def clean_target_snowflake(self):
        """Clean snowflake"""
        e2e_utils.run_query_target_snowflake(self.env, 'DROP SCHEMA IF EXISTS mysql_grp24')
        e2e_utils.run_query_target_snowflake(self.env, 'DROP SCHEMA IF EXISTS postgres_world_sf')
        e2e_utils.run_query_target_snowflake(self.env, 'DROP SCHEMA IF EXISTS s3_feeds')

        # Clean config directory
        shutil.rmtree(os.path.join(CONFIG_DIR, 'snowflake'), ignore_errors=True)

    @classmethod
    def assert_command_success(cls, return_code, stdout, stderr, log_path=None):
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

    @classmethod
    def assert_state_file_valid(cls, target_name, tap_name, log_path=None):
        """Assert helper function to check if state file exists for a certain tap
        for a certain target"""
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

    def assert_run_tap_success(self, tap, target, sync_engines):
        """Run a specific tap and make sure that it's using the correct sync engine,
        finished successfully and state file created with the right content"""
        [return_code, stdout, stderr] = e2e_utils.run_command(f'pipelinewise run_tap --tap {tap} --target {target}')
        for sync_engine in sync_engines:
            log_file = e2e_utils.find_run_tap_log_file(stdout, sync_engine)
            self.assert_command_success(return_code, stdout, stderr, log_file)
            self.assert_state_file_valid(target, tap, log_file)

    def assert_columns_are_in_table(self, table_name: str, columns: List[str]):
        """
        fetches the given table's columns from pipelinewise.columns and tests if every given column
        is in the result
        Args:
            table_name: table whose columns are to be fetched
            columns: list of columns to check if there are in the table's columns

        Returns:
            None
        """
        sql = f'SELECT COLUMN_NAME from information_schema.columns where table_name=\'{table_name.upper()}\''

        result = e2e_utils.run_query_target_snowflake(self.env, sql)
        cols = [res[0] for res in result]
        assert all([col in cols for col in columns])

    @pytest.mark.dependency(name='import_config')
    def test_import_project(self):
        """Import the YAML project with taps and target and do discovery mode to write the JSON files for singer
        connectors """
        self.clean_tap_mysql()
        self.clean_target_postgres()
        self.clean_target_snowflake()
        [return_code, stdout, stderr] = e2e_utils.run_command(f'pipelinewise import_config --dir {self.project_dir}')
        self.assert_command_success(return_code, stdout, stderr)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_pg(self):
        """Replicate data from MariaDB to Postgres DWH, check if return code is zero and success log file created"""

        # Internal helper to compare row counts in source mysql and target postgres
        def assert_row_count_equals_source_to_target():
            row_counts_in_mysql = e2e_utils.run_query_tap_mysql(self.env, """
            SELECT tbl, row_count
              FROM (      SELECT 'address'     AS tbl, COUNT(*) AS row_count FROM address
                    UNION SELECT 'area_code'   AS tbl, COUNT(*) AS row_count FROM area_code
                    UNION SELECT 'order'       AS tbl, COUNT(*) AS row_count FROM `order`
                    UNION SELECT 'weight_unit' AS tbl, COUNT(*) AS row_count FROM weight_unit) x
             ORDER BY tbl, row_count
            """)

            row_counts_in_postgres = e2e_utils.run_query_target_postgres(self.env, """
            SELECT tbl, row_count
              FROM (      SELECT 'address'     AS tbl, COUNT(*) AS row_count FROM mysql_grp24.address
                    UNION SELECT 'area_code'   AS tbl, COUNT(*) AS row_count FROM mysql_grp24.area_code
                    UNION SELECT 'order'       AS tbl, COUNT(*) AS row_count FROM mysql_grp24.order
                    UNION SELECT 'weight_unit' AS tbl, COUNT(*) AS row_count FROM mysql_grp24.weight_unit) x
             ORDER BY tbl, row_count
            """)

            # Compare the results from source and target databases
            assert row_counts_in_postgres == row_counts_in_mysql

        # Run tap in the first time - Only singer should be triggered. Fastsync not available for target postgres
        self.assert_run_tap_success('mariadb_source', 'postgres_dwh', ['singer'])
        assert_row_count_equals_source_to_target()

        # Insert new rows to source table replicated by INCREMENTAL method
        e2e_utils.run_query_tap_mysql(self.env, """
        INSERT INTO address (
            address_id, isActive, street_number, date_created, date_updated, supplier_supplier_id, zip_code_zip_code_id)
          VALUES (100001, 1, '1234', now(), now(), 999, 999)
        """)

        # Insert new rows to source table replicated by FULL_TABLE method
        e2e_utils.run_query_tap_mysql(self.env, """
        INSERT INTO area_code (area_code_id, area_code, isActive, date_created, date_updated, provance_provance_id)
          VALUES (101, '101', 1, now(), now(), 101)
        """)

        # Insert and delete rows to source table replicated by LOG_BASED method
        e2e_utils.run_query_tap_mysql(self.env, """
        INSERT INTO weight_unit (weight_unit_id, weight_unit_name, isActive, date_created, date_updated)
             VALUES (101, 'New weight unit', 1, now(), now())
        """)

        # Run the tap again, check row counts to sure the new row loaded correctly
        self.assert_run_tap_success('mariadb_source', 'postgres_dwh', ['singer'])
        assert_row_count_equals_source_to_target()

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_pg_to_pg(self):
        """Replicate data from Postgres to Postgres DWH, check if return code is zero and success log file created"""
        self.assert_run_tap_success('postgres_source', 'postgres_dwh', ['singer'])

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_sf(self):
        """Replicate data from MariaDB to Snowflake DWH, check if return code is zero and success log file created"""
        tap, target = 'mariadb_to_sf', 'snowflake'

        # Run tap first time - both fastsync and a singer should be triggered
        self.assert_run_tap_success(tap, target, ['fastsync', 'singer'])

        # Run tap second time - only singer should be triggered
        self.assert_run_tap_success(tap, target, ['singer'])
        self.assert_columns_are_in_table('edgydata', ['C_VARCHAR', 'CASE', 'GROUP', 'ORDER'])

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_pg_to_sf(self):
        """Replicate data from Postgres to Snowflake, check if return code is zero and success log file created"""
        tap, target = 'postgres_source_sf', 'snowflake'

        # Run tap first time - both fastsync and a singer should be triggered
        self.assert_run_tap_success(tap, target, ['fastsync', 'singer'])

        # Run tap second time - only singer should be triggered
        self.assert_run_tap_success(tap, target, ['singer'])

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_s3_to_sf(self):
        """Replicate csv files from s3 to Snowflake, check if return code is zero and success log file created"""
        tap, target = 'csv_on_s3', 'snowflake'

        # Run tap first time - both fastsync and a singer should be triggered
        self.assert_run_tap_success(tap, target, ['fastsync', 'singer'])

        # Run tap second time - only singer should be triggered
        self.assert_run_tap_success(tap, target, ['singer'])
