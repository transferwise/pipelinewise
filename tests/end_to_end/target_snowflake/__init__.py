import os
import shutil
import unittest
from pathlib import Path

from tests.end_to_end.helpers import assertions, tasks
from tests.end_to_end.helpers.env import E2EEnv

TEST_PROJECTS_DIR_PATH = 'tests/end_to_end/test-project'
USER_HOME = os.path.expanduser('~')
CONFIG_DIR = os.path.join(USER_HOME, '.pipelinewise')


class TargetSnowflake(unittest.TestCase):
    """
    Base class for E2E tests for target snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self, tap_id: str, target_id: str, tap_type: str):
        super().setUp()

        self.tap_id = tap_id
        self.target_id = target_id
        self.e2e_env = self.get_e2e_env()

        if self.e2e_env.env[tap_type]['is_configured'] is False:
            self.skipTest(f'{tap_type} is not configured properly')

        self.remove_dir_from_config_dir(f'{self.target_id}/{self.tap_id}')
        self.drop_sf_schema_if_exists(f'{self.tap_id}{self.e2e_env.sf_schema_postfix}')

        self.check_snowflake_credentials_provided()
        self.check_validate_taps()
        self.check_import_config()

    def tearDown(self):
        self.remove_dir_from_config_dir(f'{self.target_id}/{self.tap_id}')
        self.drop_sf_schema_if_exists(f'{self.tap_id}{self.e2e_env.sf_schema_postfix}')
        super().tearDown()

    # pylint: disable=no-self-use
    def get_e2e_env(self) -> E2EEnv:
        """
        get validated end-to-end environment
        """
        test_projects_dir = Path(TEST_PROJECTS_DIR_PATH)
        if not (test_projects_dir.exists() and test_projects_dir.is_dir()):
            raise Exception(f'{TEST_PROJECTS_DIR_PATH} does not exist')
        return E2EEnv(TEST_PROJECTS_DIR_PATH)

    def check_snowflake_credentials_provided(self):
        """
        check if snowflake credentials are provided
        """
        if self.e2e_env.env['TARGET_SNOWFLAKE']['is_configured'] is False:
            self.skipTest('TARGET SNOWFLAKE credentials are not configured')

    # pylint: disable=no-self-use
    def check_validate_taps(self):
        """
        run `pipelinewise validate`
        """
        return_code, stdout, stderr = tasks.run_command(
            f'pipelinewise validate --dir {TEST_PROJECTS_DIR_PATH}'
        )
        assertions.assert_command_success(return_code, stdout, stderr)

    # pylint: disable=no-self-use
    def check_import_config(self):
        """
        run `pipelinewise import_config`
        """
        return_code, stdout, stderr = tasks.run_command(
            f'pipelinewise import_config --dir {TEST_PROJECTS_DIR_PATH}'
        )
        assertions.assert_command_success(return_code, stdout, stderr)

    def drop_sf_schema_if_exists(self, schema: str):
        """
        drop schema from snowflake if it exists
        """
        self.e2e_env.run_query_target_snowflake(
            f'DROP SCHEMA IF EXISTS {schema} CASCADE'
        )

    # pylint: disable=no-self-use
    def remove_dir_from_config_dir(self, dir_path: str):
        """
        remove directory from config directory
        """
        shutil.rmtree(os.path.join(CONFIG_DIR, dir_path), ignore_errors=True)
