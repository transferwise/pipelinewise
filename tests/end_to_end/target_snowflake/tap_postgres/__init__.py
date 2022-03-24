from tests.end_to_end.target_snowflake import TargetSnowflake


class TapPostgres(TargetSnowflake):
    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()
