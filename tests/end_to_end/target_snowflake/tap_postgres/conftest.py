import pytest
from tests.end_to_end.helpers.env import E2EEnv


@pytest.fixture
def pg_e2e_env(sf_e2e_env: E2EEnv) -> E2EEnv:
    yield sf_e2e_env
