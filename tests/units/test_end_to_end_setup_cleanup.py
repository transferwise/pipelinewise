"""Tests for failure cleanup in Snowflake E2E setup."""

from unittest import TestCase, mock

from tests.end_to_end.target_snowflake import TargetSnowflake


class EndToEndSetupCleanupTestCase(TestCase):
    """Validate exact cleanup after a Snowflake setup failure."""

    @mock.patch('tests.end_to_end.target_snowflake.tasks.run_command')
    def test_import_config_discovers_only_the_current_tap(self, run_command_mock):
        """Each E2E setup must not rediscover unrelated source connectors."""
        run_command_mock.return_value = (0, '', '')
        target = TargetSnowflake(methodName='runTest')
        target.tap_id = 'mariadb_to_sf_iceberg'

        target.check_import_config()

        run_command_mock.assert_called_once_with(
            'pipelinewise import_config --dir tests/end_to_end/test-project '
            '--taps mariadb_to_sf_iceberg'
        )

    def test_snowflake_setup_failure_runs_exact_registered_cleanup(self):
        """A later setup failure must not leak this run's schemas or config."""
        e2e_env = mock.MagicMock()
        target = TargetSnowflake(methodName='runTest')
        e2e_env.env = {
            'TAP_POSTGRES': {'is_configured': True},
            'TARGET_SNOWFLAKE': {'is_configured': True},
        }
        e2e_env.sf_schema_postfix = '_current'
        e2e_env.sf_schema_postfix_is_override = True
        target.get_e2e_env = mock.Mock(return_value=e2e_env)
        target.remove_dir_from_config_dir = mock.Mock()
        setattr(target, 'check_snowflake_credentials_provided', mock.Mock())
        target.check_validate_taps = mock.Mock(
            side_effect=RuntimeError('validation failed')
        )
        target.check_import_config = mock.Mock()
        target.drop_sf_schema_if_exists = mock.Mock()

        with self.assertRaisesRegex(RuntimeError, 'validation failed'):
            target.setUp('postgres_to_sf', 'snowflake', 'TAP_POSTGRES')

        target.doCleanups()

        self.assertEqual(
            target.drop_sf_schema_if_exists.call_args_list,
            [
                mock.call('PPW_E2E_TAP_POSTGRES_CURRENT'),
                mock.call('PPW_E2E_TAP_POSTGRES_PUBLIC2_CURRENT'),
                mock.call('PPW_E2E_TAP_POSTGRES_2_CURRENT'),
                mock.call('PPW_E2E_TAP_POSTGRES_2_CURRENT'),
                mock.call('PPW_E2E_TAP_POSTGRES_PUBLIC2_CURRENT'),
                mock.call('PPW_E2E_TAP_POSTGRES_CURRENT'),
            ],
        )
        self.assertEqual(
            target.remove_dir_from_config_dir.call_args_list,
            [
                mock.call('snowflake'),
                mock.call('snowflake/postgres_to_sf'),
            ],
        )
        target.check_import_config.assert_not_called()
