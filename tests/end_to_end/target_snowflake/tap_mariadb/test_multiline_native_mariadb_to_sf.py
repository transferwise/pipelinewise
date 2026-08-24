"""Native Snowflake multiline FastSync coverage for MariaDB."""

from tests.end_to_end.target_snowflake.multiline_values import (
    assert_native_multiline_table,
    exercise_multiline_fastsync,
    prepare_mysql_multiline_table,
)
from tests.end_to_end.target_snowflake.tap_mariadb import TapMariaDB


TAP_ID = 'mariadb_to_sf_native_multiline'
TARGET_ID = 'snowflake'


class TestNativeMultilineMariaDBToSnowflake(TapMariaDB):
    """Exercise exact MariaDB text through native FullSync and PartialSync."""

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.source_db = self.e2e_env.get_conn_env_var('TAP_MYSQL', 'DB')
        self.target_schema = (
            f'PPW_E2E_TAP_MYSQL_2{self.e2e_env.sf_schema_postfix}'
        ).upper()

    def prepare_source(self):
        """Create the multiline table before catalog discovery."""
        super().prepare_source()
        self.addCleanup(
            self.e2e_env.run_query_tap_mysql,
            'DROP TABLE IF EXISTS multiline_values',
        )
        prepare_mysql_multiline_table(self.e2e_env.run_query_tap_mysql)

    def test_native_full_and_bounded_partial_sync_preserve_multiline_bytes(self):
        """Native MariaDB FastSync preserves exact source text bytes."""
        exercise_multiline_fastsync(
            self,
            self.e2e_env.run_query_tap_mysql,
            self.source_db,
            'mysql',
            self.target_schema,
            lambda: assert_native_multiline_table(self, self.target_schema),
        )
