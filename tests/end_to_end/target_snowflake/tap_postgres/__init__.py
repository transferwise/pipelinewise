from tests.end_to_end.target_snowflake import TargetSnowflake


POSTGRES_FASTSYNC_TABLES = {
    'public-city': True,
    'public-country': False,
    'public-no_pk_table': False,
    'public-edgydata': True,
    'public-order': True,
    'public-table_with_space and UPPERCase': True,
    'public-table_with_reserved_words': False,
    'public-customers': True,
    'public-empty_table': False,
    'public2-wearehere': False,
    'public2-public2_edgydata': True,
}


def postgres_initial_state_expectations():
    """Exact first-run FastSync streams and whether progress is required."""
    return {'fastsync': dict(POSTGRES_FASTSYNC_TABLES)}


def postgres_recurring_state_expectations():
    """Only FULL_TABLE streams return to FastSync after initialization."""
    return {
        'fastsync': {
            stream_id: False
            for stream_id, requires_progress in POSTGRES_FASTSYNC_TABLES.items()
            if not requires_progress
        }
    }


class TapPostgres(TargetSnowflake):
    """
    Base class for E2E tests for tap postgres -> target snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self, tap_id: str, target_id: str):
        super().setUp(tap_id=tap_id, target_id=target_id, tap_type='TAP_POSTGRES')

    def prepare_source(self):
        """Reset PostgreSQL before validate/import discovers its schema."""
        self.e2e_env.clean_up_temp_dir()
        self.e2e_env.setup_tap_postgres()
