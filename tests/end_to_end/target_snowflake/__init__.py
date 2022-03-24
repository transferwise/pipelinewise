import os
import shutil
import unittest
from pathlib import Path

from tests.end_to_end.helpers import assertions, tasks
from tests.end_to_end.helpers.env import E2EEnv

TEST_PROJECTS_DIR_PATH = "tests/end_to_end/test-project"
USER_HOME = os.path.expanduser("~")
CONFIG_DIR = os.path.join(USER_HOME, ".pipelinewise")


class TargetSnowflake(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.e2e_env = self.get_e2e_env()
        self.check_snowflake_credentials_provided()
        self.check_validate_taps()
        self.check_import_config()

    def tearDown(self):
        super().tearDown()

    def get_e2e_env(self) -> E2EEnv:
        test_projects_dir = Path(TEST_PROJECTS_DIR_PATH)
        if not (test_projects_dir.exists() and test_projects_dir.is_dir()):
            raise Exception(f"{TEST_PROJECTS_DIR_PATH} does not exist")
        return E2EEnv(TEST_PROJECTS_DIR_PATH)

    def check_snowflake_credentials_provided(self):
        if self.e2e_env.env["TARGET_SNOWFLAKE"]["is_configured"] is False:
            self.skipTest("TARGET SNOWFLAKE credentials are not configured")

    def check_validate_taps(self):
        return_code, stdout, stderr = tasks.run_command(
            f"pipelinewise validate --dir {TEST_PROJECTS_DIR_PATH}"
        )
        assertions.assert_command_success(return_code, stdout, stderr)

    def check_import_config(self):
        return_code, stdout, stderr = tasks.run_command(
            f"pipelinewise import_config --dir {TEST_PROJECTS_DIR_PATH}"
        )
        assertions.assert_command_success(return_code, stdout, stderr)

    def drop_schema_if_exists(self, schema: str):
        self.e2e_env.run_query_target_snowflake(
            f"DROP SCHEMA IF EXISTS {schema} CASCADE"
        )

    def remove_dir(self, dir_path: str):
        shutil.rmtree(os.path.join(CONFIG_DIR, dir_path), ignore_errors=True)
