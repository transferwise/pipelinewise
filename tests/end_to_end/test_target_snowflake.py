import os
import pytest

from .helpers import tasks
from .helpers import assertions
from .helpers.env import E2EEnv

DIR = os.path.dirname(__file__)
TAP_MARIADB_ID = 'mariadb_to_sf'
TAP_POSTGRES_ID = 'postgres_to_sf'
TAP_S3_CSV_ID = 's3_csv_to_sf'
TARGET_ID = 'snowflake'


# pylint: disable=attribute-defined-outside-init
class TestTargetSnowflake:
    """
    End to end tests for Target Snowflake
    """

    def setup_method(self):
        """Initialise test project by generating YAML files from
        templates for all the configured connectors"""
        self.project_dir = os.path.join(DIR, 'test-project')

        # Init query runner methods
        self.e2e = E2EEnv(self.project_dir)
        self.run_query_tap_mysql = self.e2e.run_query_tap_mysql
        self.run_query_tap_postgres = self.e2e.run_query_tap_postgres
        self.run_query_target_snowflake = self.e2e.run_query_target_snowflake

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
        self.e2e.setup_tap_s3_csv()
        self.e2e.setup_target_snowflake()

        # Import project
        [return_code, stdout, stderr] = tasks.run_command(f'pipelinewise import_config --dir {self.project_dir}')
        assertions.assert_command_success(return_code, stdout, stderr)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_sf(self):
        """Replicate data from MariaDB to Snowflake"""
        # Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(TAP_MARIADB_ID, TARGET_ID, ['fastsync', 'singer'])
        assertions.assert_row_counts_equal(self.run_query_tap_mysql, self.run_query_target_snowflake)
        assertions.assert_all_columns_exist(self.run_query_tap_mysql, self.e2e.run_query_target_snowflake)

        # 2. Make changes in MariaDB source database
        #  LOG_BASED
        self.run_query_tap_mysql('UPDATE weight_unit SET isactive = 0 WHERE weight_unit_id IN (2, 3, 4)')
        #  INCREMENTAL
        self.run_query_tap_mysql('INSERT INTO address(isactive, street_number, date_created, date_updated,'
                                 ' supplier_supplier_id, zip_code_zip_code_id)'
                                 'VALUES (1, 1234, NOW(), NOW(), 0, 1234)')
        self.run_query_tap_mysql('UPDATE address SET street_number = 9999, date_updated = NOW()'
                                 ' WHERE address_id = 1')
        #  FULL_TABLE
        self.run_query_tap_mysql('DELETE FROM no_pk_table WHERE id > 10')

        # 3. Run tap second time - both fastsync and a singer should be triggered, there are some FULL_TABLE
        assertions.assert_run_tap_success(TAP_POSTGRES_ID, TARGET_ID, ['fastsync', 'singer'])
        assertions.assert_row_counts_equal(self.run_query_tap_postgres, self.run_query_target_snowflake)
        assertions.assert_all_columns_exist(self.run_query_tap_mysql, self.e2e.run_query_target_snowflake)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_pg_to_sf(self):
        """Replicate data from Postgres to Snowflake"""
        # Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(TAP_POSTGRES_ID, TARGET_ID, ['fastsync', 'singer'])
        assertions.assert_row_counts_equal(self.run_query_tap_postgres, self.run_query_target_snowflake)
        assertions.assert_all_columns_exist(self.run_query_tap_postgres, self.e2e.run_query_target_snowflake)

        # 2. Make changes in MariaDB source database
        #  LOG_BASED - Missing due to some changes that's required in tap-postgres to test it automatically
        #  INCREMENTAL
        self.run_query_tap_postgres('INSERT INTO public.city (id, name, countrycode, district, population) '
                                    "VALUES (4080, 'Bath', 'GBR', 'England', 88859)")
        self.run_query_tap_postgres("UPDATE public.edgydata SET cvarchar = 'Liewe Maatjies UPDATED' WHERE cid = 23")
        #  FULL_TABLE
        self.run_query_tap_postgres("DELETE FROM public.country WHERE code = 'UMI'")

        # 3. Run tap second time - both fastsync and a singer should be triggered, there are some FULL_TABLE
        assertions.assert_run_tap_success(TAP_POSTGRES_ID, TARGET_ID, ['fastsync', 'singer'])
        assertions.assert_row_counts_equal(self.run_query_tap_postgres, self.run_query_target_snowflake)
        assertions.assert_all_columns_exist(self.run_query_tap_postgres, self.e2e.run_query_target_snowflake)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_s3_to_sf(self):
        """Replicate csv files from s3 to Snowflake, check if return code is zero and success log file created"""
        # Skip tap_s3_csv related test if required env vars not provided
        if not self.e2e.env['TAP_S3_CSV']['is_configured']:
            pytest.skip('Tap S3 CSV environment variables are not provided')

        def assert_columns_exist():
            """Helper inner function to test if every table and column exists in target snowflake"""
            assertions.assert_cols_in_table(self.run_query_target_snowflake, 'ppw_e2e_tap_s3_csv', 'countries',
                                            ['CITY', 'COUNTRY', 'CURRENCY', 'ID', 'LANGUAGE'])
            assertions.assert_cols_in_table(self.run_query_target_snowflake, 'ppw_e2e_tap_s3_csv', 'people',
                                            ['BIRTH_DATE', 'EMAIL', 'FIRST_NAME', 'GENDER', 'GROUP', 'ID',
                                             'IP_ADDRESS', 'IS_PENSIONEER', 'LAST_NAME'])

        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(TAP_S3_CSV_ID, TARGET_ID, ['fastsync', 'singer'])
        assert_columns_exist()

        # 2. Run tap second time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(TAP_S3_CSV_ID, TARGET_ID, ['fastsync', 'singer'])
        assert_columns_exist()
