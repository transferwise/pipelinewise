from tests.end_to_end.target_snowflake import TargetSnowflake


class TapPostgres(TargetSnowflake):
    def setUp(self):
        super().setUp()
        if self.e2e_env.env["TAP_POSTGRES"]["is_configured"] is False:
            self.skipTest("TAP POSTGRES credentials are not configured")

    def tearDown(self):
        super().tearDown()
