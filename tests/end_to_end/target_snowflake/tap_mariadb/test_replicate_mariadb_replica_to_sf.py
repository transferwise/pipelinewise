from pipelinewise.fastsync import mysql_to_snowflake
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB

TAP_ID = 'mariadb_replica_to_sf'
TARGET_ID = 'snowflake'


class TestReplicateMariaDBReplicaToSF(TapMariaDB):
    """
    Replicate data from MariaDB to Snowflake
    """

    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)

    def tearDown(self):
        super().tearDown()

    def test_replicate_mariadb_replica_to_sf(self):
        # 1. Run tap first time - both fastsync and a singer should be triggered
        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ['fastsync', 'singer']
        )
        assertions.assert_row_counts_equal(
            self.e2e_env.run_query_tap_mysql_2,
            self.e2e_env.run_query_target_snowflake,
            self.e2e_env.sf_schema_postfix,
        )
        assertions.assert_all_columns_exist(
            self.e2e_env.run_query_tap_mysql_2,
            self.e2e_env.run_query_target_snowflake,
            mysql_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )
