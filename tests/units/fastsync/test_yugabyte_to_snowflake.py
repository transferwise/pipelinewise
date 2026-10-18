import unittest
from unittest.mock import call, patch

from . import assertions

from pipelinewise.fastsync.yugabyte_to_snowflake import (
    tap_type_to_target_type,
    sync_table,
    main_impl,
    get_resync_size_errors,
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

    def test_resync_size_check_reports_every_oversized_or_unsized_table(self):
        with patch(f'{PACKAGE_IN_SCOPE}.{TAP}.fetch_table_sizes_mb', return_value={
            'public.small': 10.0, 'public.big': 10.5, 'public.coloc': None,
        }) as sizes:
            errors = get_resync_size_errors({'host': 'h'}, ['public.small', 'public.big', 'public.coloc'], 10)

        sizes.assert_called_once_with({'host': 'h'}, ['public.small', 'public.big', 'public.coloc'])
        self.assertEqual(2, len(errors))
        self.assertIn('size of table `public.big` is greater than `10`', errors[0])
        self.assertIn('reports no size for table `public.coloc`', errors[1])
        self.assertTrue(all('Use --force' in error for error in errors))

    def test_resync_size_check_skips_unlimited_or_partial_only_runs(self):
        with patch(f'{PACKAGE_IN_SCOPE}.{TAP}.fetch_table_sizes_mb') as sizes:
            self.assertEqual(get_resync_size_errors({}, ['public.full'], None), [])
            self.assertEqual(get_resync_size_errors({}, [], 10), [])
        sizes.assert_not_called()

    def test_main_impl_does_not_start_workers_when_size_check_fails(self):
        with patch(f'{PACKAGE_IN_SCOPE}.utils') as utils_mock, patch(
            f'{PACKAGE_IN_SCOPE}.iceberg_routes.validate_route_config',
        ), patch(
            f'{PACKAGE_IN_SCOPE}.get_resync_size_errors', return_value=['too big'],
        ), patch(f'{PACKAGE_IN_SCOPE}.multiprocessing') as multiprocessing_mock:
            utils_mock.parse_args.return_value.tables = ['public.full']
            utils_mock.parse_args.return_value.autoresync_size = 10
            utils_mock.get_pool_size.return_value = 1
            with self.assertRaises(SystemExit):
                main_impl()

        multiprocessing_mock.Pool.assert_not_called()

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
