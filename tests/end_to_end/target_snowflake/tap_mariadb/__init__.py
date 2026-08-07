from tests.end_to_end.target_snowflake import TargetSnowflake


MARIADB_FASTSYNC_TABLES = {
    'weight_unit': True,
    'address': True,
    'order': False,
    'no_pk_table': False,
    'table_with_binary': True,
    'edgydata': True,
    'full': True,
    'table_with_space and UPPERCase': True,
    'all_datatypes': True,
    'customers': True,
}


def mariadb_initial_state_expectations(source_db='mysql_source_db'):
    """Exact first-run FastSync streams and whether progress is required."""
    return {
        'fastsync': {
            f'{source_db}-{table}': requires_progress
            for table, requires_progress in MARIADB_FASTSYNC_TABLES.items()
        }
    }


def mariadb_recurring_state_expectations(source_db='mysql_source_db'):
    """Only FULL_TABLE streams return to FastSync after initialization."""
    return {
        'fastsync': {
            f'{source_db}-{table}': False
            for table, requires_progress in MARIADB_FASTSYNC_TABLES.items()
            if not requires_progress
        }
    }


class TapMariaDB(TargetSnowflake):
    """
    Base class for E2E tests for tap mysql -> target snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self, tap_id: str, target_id: str):
        super().setUp(tap_id=tap_id, target_id=target_id, tap_type='TAP_MYSQL')

    def prepare_source(self):
        """Reset MariaDB before validate/import discovers its schema."""
        self.e2e_env.setup_tap_mysql()
