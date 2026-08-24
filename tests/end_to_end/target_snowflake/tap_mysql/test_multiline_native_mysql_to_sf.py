"""Native Snowflake multiline FastSync coverage for genuine MySQL."""

from tests.end_to_end.target_snowflake.multiline_values import (
    assert_native_multiline_table,
    exercise_multiline_fastsync,
    prepare_mysql_multiline_table,
)
from tests.end_to_end.target_snowflake.tap_mysql import TapMySQL


TAP_ID = 'mysql_to_sf_native'
TARGET_ID = 'snowflake'


class TestNativeMultilineMySQLToSnowflake(TapMySQL):
    """Exercise exact MySQL text through native FullSync and PartialSync."""

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.source_db = self.e2e_env.get_conn_env_var(
            'TAP_ORACLE_MYSQL', 'DB'
        )
        self.target_schema = (
            f'PPW_E2E_TAP_ORACLE_MYSQL_2{self.e2e_env.sf_schema_postfix}'
        ).upper()

    def prepare_source(self):
        """Create the multiline table before catalog discovery."""
        super().prepare_source()
        self.addCleanup(
            self.e2e_env.run_query_tap_oracle_mysql,
            'DROP TABLE IF EXISTS multiline_values',
        )
        prepare_mysql_multiline_table(
            self.e2e_env.run_query_tap_oracle_mysql
        )

    def test_native_full_and_bounded_partial_sync_preserve_multiline_bytes(self):
        """Native MySQL FastSync preserves exact source text bytes."""
        exercise_multiline_fastsync(
            self,
            self.e2e_env.run_query_tap_oracle_mysql,
            self.source_db,
            'oracle_mysql',
            self.target_schema,
            lambda: assert_native_multiline_table(self, self.target_schema),
        )
