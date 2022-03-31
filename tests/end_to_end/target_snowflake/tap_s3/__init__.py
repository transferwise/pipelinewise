from tests.end_to_end.target_snowflake import TargetSnowflake


class TapS3(TargetSnowflake):
    def setUp(self, tap_id: str, target_id: str):
        super().setUp(tap_id=tap_id, target_id=target_id, tap_type='TAP_S3_CSV')
        self.e2e_env.setup_tap_s3_csv()

    def tearDown(self):
        super().tearDown()
