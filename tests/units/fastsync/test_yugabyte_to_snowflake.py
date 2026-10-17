import unittest
from unittest.mock import call, patch

from . import assertions

from pipelinewise.fastsync.yugabyte_to_snowflake import (
    tap_type_to_target_type,
    sync_table,
    main_impl,
)

PACKAGE_IN_SCOPE = 'pipelinewise.fastsync.yugabyte_to_snowflake'
TAP = 'FastSyncTapYugabyte'
TARGET = 'FastSyncTargetSnowflake'


class YugabyteToSnowflake(unittest.TestCase):
    """
    Unit tests for fastsync yugabyte to snowflake
    """

    def test_tap_type_to_target_type_with_defined_tap_type_returns_equivalent_target_type(
        self,
    ):
        type_mappings = {
            'char': 'VARCHAR(134217728)',
            'text': 'VARCHAR(134217728)',
            'bit': 'BOOLEAN',
            'serial': 'NUMBER',
            'numeric': 'FLOAT',
            'date': 'TIMESTAMP_NTZ',
            'time with time zone': 'TIME',
            'ARRAY': 'VARIANT',
            'jsonb': 'VARIANT',
        }

        for source_type, expected_type in type_mappings.items():
            with self.subTest(source_type=source_type):
                self.assertEqual(expected_type, tap_type_to_target_type(source_type))

    def test_tap_type_to_target_type_with_undefined_tap_type_returns_max_varchar(self):
        self.assertEqual('VARCHAR(134217728)', tap_type_to_target_type('random-type'))

    @staticmethod
    def test_sync_table_exception_on_copy_table_returns_failed_table_name_and_exception():
        with patch(
            f'{PACKAGE_IN_SCOPE}.iceberg_routes.require_native_target_format'
        ):
            assertions.assert_sync_table_exception_on_failed_copy(
                sync_table,
                PACKAGE_IN_SCOPE,
                TAP,
                TARGET,
                expected_cleanup=call.close_connection(silent=True),
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
