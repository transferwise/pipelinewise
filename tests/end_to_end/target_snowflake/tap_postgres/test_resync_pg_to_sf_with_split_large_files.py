from pipelinewise.fastsync import postgres_to_snowflake
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres

TAP_ID = "postgres_to_sf_split_large_files"
TARGET_ID = "snowflake"


class TestResyncPGToSFWithSplitLargeFiles(TapPostgres):
    """
    Resync tables from Postgres to Snowflake using splitting large files option.
    """

    def setUp(self):
        super().setUp()

    def tearDown(self):
        self.drop_schema_if_exists(f"{TAP_ID}{self.e2e_env.sf_schema_postfix}")
        self.remove_dir(f"{TARGET_ID}/{TAP_ID}")
        super().tearDown()

    def test_resync_pg_to_sf_with_split_large_files(self):
        assertions.assert_resync_tables_success(
            tap=TAP_ID,
            target=TARGET_ID,
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
