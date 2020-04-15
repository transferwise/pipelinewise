import os
import pytest

from .env import E2EEnv
from . import tasks
from . import assertions

DIR = os.path.dirname(__file__)


# pylint: disable=attribute-defined-outside-init
class TestTargetSnowflake:
    """
    End to end tests for Target Snowflake
    """

    def setup_method(self):
        """Initialise test project by generating YAML files from
        templates for all the configured connectors"""
        self.project_dir = os.path.join(DIR, 'test-project')
        self.e2e = E2EEnv(self.project_dir)

    def teardown_method(self):
        """Delete test directories and database objects"""

    @pytest.mark.dependency(name='import_config')
    def test_import_project(self):
        """Import the YAML project with taps and target and do discovery mode
        to write the JSON files for singer connectors """

        # Skip every target_postgres related test if required env vars not provided
        if not self.e2e.env['TARGET_SNOWFLAKE']['is_configured']:
            pytest.skip('Target Snowflake environment variables are not provided')

        # Setup and clean source and target databases
        self.e2e.setup_tap_mysql()
        self.e2e.setup_tap_postgres()
        self.e2e.setup_target_snowflake()

        # Import project
        [return_code, stdout, stderr] = tasks.run_command(f'pipelinewise import_config --dir {self.project_dir}')
        assertions.assert_command_success(return_code, stdout, stderr)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_sf(self):
        """Replicate data from MariaDB to Snowflake
        Check if return code is zero and success log file created"""
        tap, target = 'mariadb_to_sf', 'snowflake'

        # Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(tap, target, ['fastsync', 'singer'])
        assertions.assert_cols_in_table(self.e2e.run_query_target_snowflake,
                                        'edgydata', ['C_VARCHAR', 'CASE', 'GROUP', 'ORDER'])

        # Insert new rows to source table replicated by INCREMENTAL method
        self.e2e.run_query_tap_mysql("""
            INSERT INTO `full`(end) VALUES (33),(34),(35),(36),(66),(20),(1);
        """)

        # Run tap second time - only singer should be triggered
        assertions.assert_run_tap_success(tap, target, ['singer'])
        assertions.assert_cols_in_table(self.e2e.run_query_target_snowflake,
                                        'edgydata', ['C_VARCHAR', 'CASE', 'GROUP', 'ORDER'])

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_pg_to_sf(self):
        """Replicate data from Postgres to Snowflake
        Check if return code is zero and success log file created"""
        tap, target = 'postgres_to_sf', 'snowflake'

        # Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(tap, target, ['fastsync', 'singer'])
        assertions.assert_cols_in_table(self.e2e.run_query_target_snowflake,
                                        'table_with_reserved_words', ['ORDER'])

        # Run tap second time - only singer should be triggered
        assertions.assert_run_tap_success(tap, target, ['singer'])
        assertions.assert_cols_in_table(self.e2e.run_query_target_snowflake,
                                        'table_with_reserved_words', ['ORDER'])

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_s3_to_sf(self):
        """Replicate csv files from s3 to Snowflake, check if return code is zero and success log file created"""
        tap, target = 's3_csv_to_sf', 'snowflake'

        # Skip tap_s3_csv related test if required env vars not provided
        if not self.e2e.env['TAP_S3_CSV']['is_configured']:
            pytest.skip('Tap S3 CSV environment variables are not provided')

        # Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(tap, target, ['fastsync', 'singer'])

        # Run tap second time - only singer should be triggered
        assertions.assert_run_tap_success(tap, target, ['singer'])
