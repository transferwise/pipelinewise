from tests.end_to_end.target_snowflake import TargetSnowflake


MYSQL_FASTSYNC_TABLES = {
    'iceberg_events': True,
    'iceberg_incremental': True,
    'iceberg_full_reload': False,
}


def mysql_initial_state_expectations(source_db='mysql8_source_db'):
    """Return exact first-run stream bookmarks for genuine MySQL."""
    return {
        'fastsync': {
            f'{source_db}-{table}': requires_progress
            for table, requires_progress in MYSQL_FASTSYNC_TABLES.items()
        }
    }


def mysql_recurring_state_expectations(source_db='mysql8_source_db'):
    """Return recurring FullSync streams for genuine MySQL."""
    return {
        'fastsync': {
            f'{source_db}-{table}': False
            for table, requires_progress in MYSQL_FASTSYNC_TABLES.items()
            if not requires_progress
        }
    }


class TapMySQL(TargetSnowflake):
    """Base class for genuine MySQL-to-Snowflake E2E tests."""

    # pylint: disable=arguments-differ
    def setUp(self, tap_id: str, target_id: str):
        super().setUp(
            tap_id=tap_id,
            target_id=target_id,
            tap_type='TAP_ORACLE_MYSQL',
        )

    def prepare_source(self):
        """Reset genuine MySQL before validate/import discovers its schema."""
        self.e2e_env.setup_tap_oracle_mysql()
