import decimal
import gzip
import os
import tempfile
import uuid
from datetime import datetime
from random import randint

import bson
import pytest
from bson import Timestamp
from pipelinewise.fastsync import mysql_to_snowflake, postgres_to_snowflake

from .helpers import assertions, tasks
from .helpers.env import E2EEnv

DIR = os.path.dirname(__file__)
TAP_MARIADB_ID = "mariadb_to_sf"
TAP_MARIADB_SPLIT_LARGE_FILES_ID = "mariadb_to_sf_split_large_files"
TAP_MARIADB_BUFFERED_STREAM_ID = "mariadb_to_sf_buffered_stream"
TAP_MARIADB_REPLICA_ID = "mariadb_replica_to_sf"
TAP_POSTGRES_ID = "postgres_to_sf"
TAP_POSTGRES_SPLIT_LARGE_FILES_ID = "postgres_to_sf_split_large_files"
TAP_POSTGRES_ARCHIVE_LOAD_FILES_ID = "postgres_to_sf_archive_load_files"
TAP_MONGODB_ID = "mongo_to_sf"
TAP_S3_CSV_ID = "s3_csv_to_sf"
TARGET_ID = "snowflake"


# pylint: disable=attribute-defined-outside-init,too-many-instance-attributes
class TestTargetSnowflake:
    """
    End to end tests for Target Snowflake
    """

    def setup_method(self):
        """Initialise test project by generating YAML files from
        templates for all the configured connectors"""

        # Init query runner methods
        self.run_query_tap_mysql = self.e2e.run_query_tap_mysql
        self.run_query_tap_mysql_2 = self.e2e.run_query_tap_mysql_2
        self.run_query_tap_postgres = self.e2e.run_query_tap_postgres
        self.run_query_target_snowflake = self.e2e.run_query_target_snowflake
        self.mongodb_con = self.e2e.get_tap_mongodb_connection()
        self.snowflake_schema_postfix = self.e2e.sf_schema_postfix

    def setup_class(self):
        """Initialise test suite"""
        self.project_dir = os.path.join(DIR, "test-project")
        self.e2e = E2EEnv(self.project_dir)

    def teardown_class(self):
        """Teardown test suite"""
        # Cleanup the Snowflake test schemas
        self.e2e.setup_target_snowflake()

    def teardown_method(self):
        """Delete test directories and database objects"""

    @pytest.fixture(autouse=True)
    def skip_if_no_sf_credentials(self):
        """
        Test fixture to be used by all tests to check if SF is configured to decide whether to run the test or not.
        """
        # Skip every test if required env vars not provided
        print("skip_if_no_sf_credentials executed")
        if self.e2e.env["TARGET_SNOWFLAKE"]["is_configured"]:
            yield
        else:
            pytest.skip("Target Snowflake environment variables are not provided")

    @pytest.mark.dependency(name="validate")
    def test_validate(self):
        """Validate the YAML project with taps and target"""

        # validate project
        return_code, stdout, stderr = tasks.run_command(
            f"pipelinewise validate --dir {self.project_dir}"
        )

        print("--------------- stdout ----------", stdout)
        print("--------------- stderr ----------", stderr)
        assert return_code == 0

    @pytest.mark.dependency(depends=["validate"])
    @pytest.mark.dependency(name="import_config")
    def test_import_project(self):
        """Import the YAML project with taps and target and do discovery mode
        to write the JSON files for singer connectors"""

        # Setup and clean source and target databases
        self.e2e.setup_tap_mysql()
        self.e2e.setup_tap_postgres()
        if self.e2e.env["TAP_S3_CSV"]["is_configured"]:
            self.e2e.setup_tap_s3_csv()
        self.e2e.setup_tap_mongodb()
        self.e2e.setup_target_snowflake()

        # Import project
        [return_code, stdout, stderr] = tasks.run_command(
            f"pipelinewise import_config --dir {self.project_dir}"
        )
        assertions.assert_command_success(return_code, stdout, stderr)

    # pylint: disable=invalid-name
    @pytest.mark.dependency(depends=["import_config"])
    def test_resync_mariadb_to_sf_with_split_large_files(
        self, tap_mariadb_id=TAP_MARIADB_SPLIT_LARGE_FILES_ID
    ):
        """Resync tables from MariaDB to Snowflake using splitting large files option"""
        assertions.assert_resync_tables_success(
            tap_mariadb_id, TARGET_ID, profiling=True
        )
        assertions.assert_row_counts_equal(
            self.run_query_tap_mysql,
            self.run_query_target_snowflake,
            schema_postfix=self.snowflake_schema_postfix,
        )
        assertions.assert_all_columns_exist(
            self.run_query_tap_mysql,
            self.run_query_target_snowflake,
            mysql_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.snowflake_schema_postfix,
        )
