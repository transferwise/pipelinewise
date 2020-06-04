import os
import pytest

from .helpers import tasks
from .helpers import assertions
from .helpers.env import E2EEnv

DIR = os.path.dirname(__file__)
TAP_MARIADB_ID = 'mariadb_to_pg'
TAP_MONGODB_ID = 'tap_mongo'
TAP_POSTGRES_ID = 'postgres_to_pg'
TAP_S3_CSV_ID = 's3_csv_to_pg'
TARGET_ID = 'postgres_dwh'


# pylint: disable=attribute-defined-outside-init
class TestTargetPostgres:
    """
    End to end tests for Target Postgres
    """

    def setup_method(self):
        """Initialise test project by generating YAML files from
        templates for all the configured connectors"""
        self.project_dir = os.path.join(DIR, 'test-project')

        # Init query runner methods
        self.e2e = E2EEnv(self.project_dir)
        self.run_query_tap_mysql = self.e2e.run_query_tap_mysql
        self.run_query_tap_postgres = self.e2e.run_query_tap_postgres
        self.run_query_target_postgres = self.e2e.run_query_target_postgres

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
        if self.e2e.env['TAP_S3_CSV']['is_configured']:
            self.e2e.setup_tap_s3_csv()
        self.e2e.setup_tap_mongodb()
        self.e2e.setup_target_postgres()

        # Import project
        [return_code, stdout, stderr] = tasks.run_command(f'pipelinewise import_config --dir {self.project_dir}')
        assertions.assert_command_success(return_code, stdout, stderr)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mariadb_to_pg(self):
        """Replicate data from MariaDB to Postgres DWH"""
        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(TAP_MARIADB_ID, TARGET_ID, ['fastsync', 'singer'])
        assertions.assert_row_counts_equal(self.run_query_tap_mysql, self.run_query_target_postgres)
        assertions.assert_all_columns_exist(self.run_query_tap_mysql, self.e2e.run_query_target_postgres)

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
        assertions.assert_run_tap_success(TAP_MARIADB_ID, TARGET_ID, ['fastsync', 'singer'])
        assertions.assert_row_counts_equal(self.run_query_tap_mysql, self.run_query_target_postgres)
        assertions.assert_all_columns_exist(self.run_query_tap_mysql, self.e2e.run_query_target_postgres)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_pg_to_pg(self):
        """Replicate data from Postgres to Postgres DWH"""
        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(TAP_POSTGRES_ID, TARGET_ID, ['fastsync', 'singer'])
        assertions.assert_row_counts_equal(self.run_query_tap_postgres, self.run_query_target_postgres)
        assertions.assert_all_columns_exist(self.run_query_tap_postgres, self.e2e.run_query_target_postgres)

        # 2. Make changes in MariaDB source database
        #  LOG_BASED - Missing due to some changes that's required in tap-postgres to test it automatically
        #  INCREMENTAL
        self.run_query_tap_postgres('INSERT INTO public.city (id, name, countrycode, district, population) '
                                    "VALUES (4080, 'Bath', 'GBR', 'England', 88859)")
        self.run_query_tap_postgres('UPDATE public.edgydata SET '
                                    "cjson = json '{\"data\": 1234}', "
                                    "cjsonb = jsonb '{\"data\": 2345}', "
                                    "cvarchar = 'Liewe Maatjies UPDATED' WHERE cid = 23")
        #  FULL_TABLE
        self.run_query_tap_postgres("DELETE FROM public.country WHERE code = 'UMI'")

        # 3. Run tap second time - both fastsync and a singer should be triggered, there are some FULL_TABLE
        assertions.assert_run_tap_success(TAP_POSTGRES_ID, TARGET_ID, ['fastsync', 'singer'])
        assertions.assert_row_counts_equal(self.run_query_tap_postgres, self.run_query_target_postgres)
        assertions.assert_all_columns_exist(self.run_query_tap_postgres, self.e2e.run_query_target_postgres)

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_s3_to_pg(self):
        """Replicate csv files from s3 to Postgres"""
        # Skip tap_s3_csv related test if required env vars not provided
        if not self.e2e.env['TAP_S3_CSV']['is_configured']:
            pytest.skip('Tap S3 CSV environment variables are not provided')

        def assert_columns_exist():
            """Helper inner function to test if every table and column exists in target snowflake"""
            assertions.assert_cols_in_table(self.run_query_target_postgres, 'ppw_e2e_tap_s3_csv', 'countries',
                                            ['city', 'country', 'currency', 'id', 'language'])
            assertions.assert_cols_in_table(self.run_query_target_postgres, 'ppw_e2e_tap_s3_csv', 'people',
                                            ['birth_date', 'email', 'first_name', 'gender', 'group', 'id',
                                             'ip_address', 'is_pensioneer', 'last_name'])

        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(TAP_S3_CSV_ID, TARGET_ID, ['fastsync', 'singer'])
        assert_columns_exist()

        # 2. Run tap second time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(TAP_S3_CSV_ID, TARGET_ID, ['fastsync', 'singer'])
        assert_columns_exist()

    @pytest.mark.dependency(depends=['import_config'])
    def test_replicate_mongodb_to_pg(self):
        """Replicate mongodb to Postgres"""

        def assert_columns_exist():
            """Helper inner function to test if every table and column exists in target snowflake"""
            assertions.assert_cols_in_table(self.run_query_target_postgres, 'ppw_e2e_tap_mongodb', 'listings',
                                            ['_id', 'document', '_sdc_deleted_at'])

        # Run tap first time - singer should be triggered
        assertions.assert_run_tap_success(TAP_MONGODB_ID, TARGET_ID, ['singer'])
        assert_columns_exist()
