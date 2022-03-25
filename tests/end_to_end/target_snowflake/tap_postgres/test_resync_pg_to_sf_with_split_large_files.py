from pipelinewise.fastsync import postgres_to_snowflake
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres


class TestResyncPGToSFWithSplitLargeFiles(TapPostgres):
    """
    Resync tables from Postgres to Snowflake using splitting large files option.
    """

    def setUp(self):
        self.TAP_ID = "postgres_to_sf_split_large_files"
        self.TARGET_ID = "snowflake"
        super().setUp()
        self.drop_schema_if_exists(f"{self.TAP_ID}{self.e2e_env.sf_schema_postfix}")

    def tearDown(self):
        self.drop_schema_if_exists(f"{self.TAP_ID}{self.e2e_env.sf_schema_postfix}")
        self.remove_dir_from_config_dir(f"{self.TARGET_ID}/{self.TAP_ID}")
        super().tearDown()

    def test_resync_pg_to_sf_with_split_large_files(self):
        assertions.assert_resync_tables_success(
            tap=self.TAP_ID,
            target=self.TARGET_ID,
        )

        assertions.assert_row_counts_equal(
            tap_query_runner_fn=self.e2e_env.run_query_tap_postgres,
            target_query_runner_fn=self.e2e_env.run_query_target_snowflake,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

        assertions.assert_all_columns_exist(
            tap_query_runner_fn=self.e2e_env.run_query_tap_postgres,
            target_query_runner_fn=self.e2e_env.run_query_target_snowflake,
            column_type_mapper_fn=postgres_to_snowflake.tap_type_to_target_type,
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )
