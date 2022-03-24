from tests.end_to_end.target_snowflake import TargetSnowflake


class TapS3(TargetSnowflake):
    def setUp(self):
        super().setUp()
        if self.e2e_env.env["TAP_S3_CSV"]["is_configured"] is False:
            self.skipTest("TAP S3 credentials are not configured")

    def tearDown(self):
        super().tearDown()
