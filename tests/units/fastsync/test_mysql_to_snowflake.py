import unittest
from . import assertions

from pipelinewise.fastsync.mysql_to_snowflake import sync_table, main_impl

PACKAGE_IN_SCOPE = 'pipelinewise.fastsync.mysql_to_snowflake'
TAP = 'FastSyncTapMySql'
TARGET = 'FastSyncTargetSnowflake'


# pylint: disable=missing-function-docstring,invalid-name,no-self-use
class MysqlToSnowflake(unittest.TestCase):
    """
    Unit tests for fastsync mysql to snowflake
    """
    @staticmethod
    def test_sync_table_runs_successfully_returns_true():
        assertions.assert_sync_table_returns_true_on_success(
            sync_table, PACKAGE_IN_SCOPE, TAP, TARGET
        )

    @staticmethod
    def test_sync_table_exception_on_copy_table_returns_failed_table_name_and_exception():
        assertions.assert_sync_table_exception_on_failed_copy(
            sync_table, PACKAGE_IN_SCOPE, TAP, TARGET
        )

    @staticmethod
    def test_main_impl_with_all_tables_synced_successfully_should_exit_normally():
        assertions.assert_main_impl_exit_normally_on_success(
            main_impl, PACKAGE_IN_SCOPE, TAP, TARGET
        )

    @staticmethod
    def test_main_impl_with_one_table_fails_to_sync_should_exit_with_error():
        assertions.assert_main_impl_should_exit_with_error_on_failure(
            main_impl, PACKAGE_IN_SCOPE, TAP, TARGET
        )


if __name__ == '__main__':
    unittest.main()
