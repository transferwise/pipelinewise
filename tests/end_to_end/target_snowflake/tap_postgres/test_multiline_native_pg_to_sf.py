"""Native Snowflake multiline FastSync coverage for PostgreSQL."""

from tests.end_to_end.target_snowflake.multiline_values import (
    assert_native_multiline_table,
    exercise_multiline_fastsync,
    prepare_postgres_multiline_table,
)
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres


TAP_ID = 'postgres_to_sf_native_multiline'
TARGET_ID = 'snowflake'


class TestNativeMultilinePostgresToSnowflake(TapPostgres):
    """Exercise exact PostgreSQL text through native FullSync and PartialSync."""

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.target_schema = (
            f'PPW_E2E_TAP_POSTGRES_2{self.e2e_env.sf_schema_postfix}'
        ).upper()

    def prepare_source(self):
        """Create the multiline table before catalog discovery."""
        super().prepare_source()
        self.addCleanup(
            self.e2e_env.run_query_tap_postgres,
            'DROP TABLE IF EXISTS public.multiline_values CASCADE',
        )
        prepare_postgres_multiline_table(
            self.e2e_env.run_query_tap_postgres
        )

    def test_native_full_and_bounded_partial_sync_preserve_multiline_bytes(self):
        """Native PostgreSQL FastSync preserves exact source text bytes."""
        exercise_multiline_fastsync(
            self,
            self.e2e_env.run_query_tap_postgres,
            'public',
            'postgres',
            self.target_schema,
            lambda: assert_native_multiline_table(self, self.target_schema),
        )
