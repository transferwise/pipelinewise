import pytest

from tests.end_to_end.helpers.env import E2EEnv
from tests.end_to_end.target_snowflake import TargetSnowflake


@pytest.mark.skipif(not E2EEnv.env['TAP_MYSQL']['is_configured'], reason='MySql not configured.')
class TapMongoDB(TargetSnowflake):
    """
    Base class for E2E tests for tap mongodb -> target snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self, tap_id: str, target_id: str):
        super().setUp(tap_id=tap_id, target_id=target_id, tap_type='TAP_MONGODB')
        self.e2e_env.setup_tap_mongodb()
