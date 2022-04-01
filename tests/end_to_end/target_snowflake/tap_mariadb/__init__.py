from tests.end_to_end.target_snowflake import TargetSnowflake


class TapMariaDB(TargetSnowflake):
    """
    Base class for E2E tests for tap mysql -> target snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self, tap_id: str, target_id: str):
        super().setUp(tap_id=tap_id, target_id=target_id, tap_type='TAP_MYSQL')
        self.e2e_env.setup_tap_mysql()
