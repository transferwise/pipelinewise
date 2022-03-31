from pipelinewise.fastsync import mysql_to_snowflake
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB

TAP_ID = 'mariadb_to_sf'
TARGET_ID = 'snowflake'


class TestResyncMariaDBToSF(TapMariaDB):
    """
    Resync tables from MariaDB to Snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def test_resync_mariadb_to_sf(self):
        """
        Resync tables from MariaDB to Snowflake
        """

        assertions.assert_resync_tables_success(
            self.tap_id,
            self.target_id,
            profiling=True,
        )
        assertions.assert_row_counts_equal(
            self.e2e_env.run_query_tap_mysql,
            self.e2e_env.run_query_target_snowflake,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )
        assertions.assert_all_columns_exist(
            self.e2e_env.run_query_tap_mysql,
            self.e2e_env.run_query_target_snowflake,
            mysql_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )
