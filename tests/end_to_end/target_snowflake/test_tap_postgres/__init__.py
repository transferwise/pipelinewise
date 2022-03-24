from tests.end_to_end.target_snowflake import TargetSnowflake


class TapPostgres(TargetSnowflake):
    def setUp(self) -> None:
        return super().setUp()
