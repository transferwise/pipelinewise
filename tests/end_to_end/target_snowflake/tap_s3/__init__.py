from tests.end_to_end.target_snowflake import TargetSnowflake


class TapS3(TargetSnowflake):
    """
    Base class for E2E tests for tap S3 -> target snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self, tap_id: str, target_id: str):
        super().setUp(tap_id=tap_id, target_id=target_id, tap_type='TAP_S3_CSV')

    def prepare_source(self):
        """Upload source fixtures before discovery and remove CI-owned keys later."""
        self.addCleanup(self.e2e_env.cleanup_tap_s3_csv)
        self.e2e_env.setup_tap_s3_csv()
