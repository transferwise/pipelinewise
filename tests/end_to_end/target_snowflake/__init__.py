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

        # Recovery is target-scoped. Reset the whole generated target tree so
        # stale sibling files cannot make this tap depend on a previous test.
        self.remove_dir_from_config_dir(self.target_id)

        self.check_snowflake_credentials_provided()
        self.tap_type = tap_type
        self.addCleanup(
            self.remove_dir_from_config_dir,
            f'{self.target_id}/{self.tap_id}',
        )
        current_run_schemas = self._current_run_schemas()
        for schema in current_run_schemas:
            self.addCleanup(self.drop_sf_schema_if_exists, schema)
        if self.e2e_env.sf_schema_postfix_is_override:
            for schema in current_run_schemas:
                self.drop_sf_schema_if_exists(schema)

        self.prepare_source()
        self.check_validate_taps()
        self.check_import_config()

    def prepare_source(self):
        """Prepare a source fixture before validation and discovery."""

    def _current_run_schemas(self):
        """Return only the schemas rendered for this test environment."""
        schema_prefix = f'ppw_e2e_{self.tap_type}'
        schema_postfix = self.e2e_env.sf_schema_postfix
        return [
            f'{schema_prefix}{schema_postfix}'.upper(),
            f'{schema_prefix}_public2{schema_postfix}'.upper(),
            f'{schema_prefix}_2{schema_postfix}'.upper(),
        ]

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

    def check_validate_taps(self):
        """
        run `pipelinewise validate`
        """
        return_code, stdout, stderr = tasks.run_command(
            f'pipelinewise validate --dir {TEST_PROJECTS_DIR_PATH}'
        )
        assertions.assert_command_success(return_code, stdout, stderr)

    def check_import_config(self):
        """
        run `pipelinewise import_config`
        """
        return_code, stdout, stderr = tasks.run_command(
            f'pipelinewise import_config --dir {TEST_PROJECTS_DIR_PATH} '
            f'--taps {self.tap_id}'
        )
        assertions.assert_command_success(return_code, stdout, stderr)

    def iceberg_fastsync_s3_keys(self):
        """Return current route-owned FastSync staging keys."""
        bucket = self.e2e_env.get_conn_env_var(
            'TARGET_SNOWFLAKE', 'S3_BUCKET'
        )
        key_prefix = self.e2e_env.get_conn_env_var(
            'TARGET_SNOWFLAKE', 'S3_KEY_PREFIX'
        )
        route_prefix = f'{key_prefix}pipelinewise_{self.tap_id}_'
        response = self.e2e_env.get_aws_session().client('s3').list_objects_v2(
            Bucket=bucket,
            Prefix=route_prefix,
        )
        return sorted(item['Key'] for item in response.get('Contents', []))

    def assert_iceberg_fastsync_cleanup(
        self,
        target_schema,
        expected_s3_keys=(),
    ):
        """Assert a successful Iceberg route leaves no recoverable staging debt."""
        runtime_dir = Path(CONFIG_DIR) / self.target_id
        manifests = sorted(runtime_dir.glob('iceberg-recovery-*.json'))
        target_pointers = sorted(
            runtime_dir.glob('iceberg-fastsync-target-*.json')
        )
        self.assertEqual(manifests, [])
        self.assertEqual(target_pointers, [])

        table_rows = self.e2e_env.run_query_target_snowflake(
            f'SHOW TABLES IN SCHEMA "{target_schema}"'
        )
        staging_tables = [
            row[1] for row in table_rows if '_PW_ICEBERG_' in row[1]
        ]
        self.assertEqual(staging_tables, [])

        self.assertEqual(
            self.iceberg_fastsync_s3_keys(),
            sorted(expected_s3_keys),
        )

    def drop_sf_schema_if_exists(self, schema: str):
        """
        drop schema from snowflake if it exists
        """
        self.e2e_env.run_query_target_snowflake(
            f'DROP SCHEMA IF EXISTS {schema} CASCADE'
        )

    def remove_dir_from_config_dir(self, dir_path: str):
        """
        remove directory from config directory
        """
        try:
            shutil.rmtree(os.path.join(CONFIG_DIR, dir_path))
        except FileNotFoundError:
            pass
