import os
import pytest

from .env import E2EEnv
from . import tasks
from . import assertions

DIR = os.path.dirname(__file__)


# pylint: disable=attribute-defined-outside-init
class TestTargetPostgres:
    """
    End to end tests for Target Postgres
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
        if not self.e2e.env['TARGET_POSTGRES']['is_configured']:
            pytest.skip('Target Postgres environment variables are not provided')

        # Setup and clean source and target databases
        self.e2e.setup_tap_mysql()
        self.e2e.setup_tap_postgres()
        self.e2e.setup_target_postgres()

        # Import project
        [return_code, stdout, stderr] = tasks.run_command(f'pipelinewise import_config --dir {self.project_dir}')
        assertions.assert_command_success(return_code, stdout, stderr)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_pg(self):
        """Replicate data from MariaDB to Postgres DWH
        Check if return code is zero and success log file created"""
        run_query_tap_mysql = self.e2e.run_query_tap_mysql
        run_query_target_postgres = self.e2e.run_query_target_postgres

        # Run tap in the first time - Only singer should be triggered. Fastsync not available for target postgres
        assertions.assert_run_tap_success('mariadb_to_pg', 'postgres_dwh', ['singer'])
        assertions.assert_tap_mysql_row_count_equals(run_query_tap_mysql, run_query_target_postgres)

        # Insert new rows to source table replicated by INCREMENTAL method
        self.e2e.run_query_tap_mysql("""
        INSERT INTO address (
            address_id, isActive, street_number, date_created, date_updated, supplier_supplier_id, zip_code_zip_code_id)
          VALUES (100001, 1, '1234', now(), now(), 999, 999)
        """)

        # Insert new rows to source table replicated by FULL_TABLE method
        self.e2e.run_query_tap_mysql("""
        INSERT INTO area_code (area_code_id, area_code, isActive, date_created, date_updated, provance_provance_id)
          VALUES (101, '101', 1, now(), now(), 101)
        """)

        # Insert and delete rows to source table replicated by LOG_BASED method
        self.e2e.run_query_tap_mysql("""
        INSERT INTO weight_unit (weight_unit_id, weight_unit_name, isActive, date_created, date_updated)
             VALUES (101, 'New weight unit', 1, now(), now())
        """)

        # Run the tap again, check row counts to sure the new row loaded correctly
        assertions.assert_run_tap_success('mariadb_to_pg', 'postgres_dwh', ['singer'])
        assertions.assert_tap_mysql_row_count_equals(run_query_tap_mysql, run_query_target_postgres)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_pg_to_pg(self):
        """Replicate data from Postgres to Postgres DWH, check if return code is zero and success log file created"""
        assertions.assert_run_tap_success('postgres_to_pg', 'postgres_dwh', ['singer'])
        # Add an object reference to avoid to use classmethod. TODO: Add more real tests
        assert self.e2e == self.e2e
