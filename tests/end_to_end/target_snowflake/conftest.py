from pathlib import Path

import pytest
from tests.end_to_end.helpers import assertions, tasks
from tests.end_to_end.helpers.env import E2EEnv
from tests.end_to_end.target_snowflake import TEST_PROJECTS_DIR_PATH


def get_e2e_env(test_projects_dir_path: str = TEST_PROJECTS_DIR_PATH) -> E2EEnv:
    test_projects_dir = Path(test_projects_dir_path)
    assert test_projects_dir.exists() and test_projects_dir.is_dir()
    return E2EEnv(TEST_PROJECTS_DIR_PATH)


def check_snowflake_credentials_provided(e2e_env: E2EEnv):
    if e2e_env.env["TARGET_SNOWFLAKE"]["is_configured"] is False:
        pytest.skip("TARGET SNOWFLAKE credentials not configured")


def check_validate_taps():
    return_code, stdout, stderr = tasks.run_command(
        f"pipelinewise validate --dir {TEST_PROJECTS_DIR_PATH}"
    )
    assertions.assert_command_success(return_code, stdout, stderr)


def check_import_config():
    return_code, stdout, stderr = tasks.run_command(
        f"pipelinewise import_config --dir {TEST_PROJECTS_DIR_PATH}"
    )
    assertions.assert_command_success(return_code, stdout, stderr)


def check_target_snowflake_connection(e2e_env: E2EEnv):
    assert e2e_env.run_query_target_snowflake("SELECT 1")[0][0] == 1


@pytest.fixture
def sf_e2e_env() -> E2EEnv:
    e2e_env = get_e2e_env(TEST_PROJECTS_DIR_PATH)
    check_snowflake_credentials_provided(e2e_env)
    check_target_snowflake_connection(e2e_env)
    check_validate_taps()
    check_import_config()
    yield e2e_env
