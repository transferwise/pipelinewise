import unittest
from argparse import Namespace
from unittest.mock import Mock, patch

from . import assertions

from pipelinewise.fastsync.mongodb_to_snowflake import (
    tap_type_to_target_type,
    sync_table,
    main_impl,
)
from pipelinewise.fastsync.commons import snowflake_iceberg_routes
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    TABLE_FORMAT_MANAGED_ICEBERG_V3,
    TableCompatibilityError,
)

PACKAGE_IN_SCOPE = 'pipelinewise.fastsync.mongodb_to_snowflake'
TAP = 'FastSyncTapMongoDB'
TARGET = 'FastSyncTargetSnowflake'


# pylint: disable=missing-function-docstring,invalid-name
class MongoDBToSnowflake(unittest.TestCase):
    """
    Unit tests for fastsync MongoDB to postgres
    """

    def test_tap_type_to_target_type_with_defined_tap_type_returns_equivalent_target_type(
        self,
    ):
        self.assertEqual('VARIANT', tap_type_to_target_type('object'))

    def test_tap_type_to_target_type_with_undefined_tap_type_returns_CHARACTER_VARYING(
        self,
    ):
        self.assertEqual('VARCHAR', tap_type_to_target_type('random-type'))

    @staticmethod
    def test_sync_table_runs_successfully_returns_true():
        with patch(
            f'{PACKAGE_IN_SCOPE}.iceberg_routes.require_native_target_format'
        ):
            assertions.assert_sync_table_returns_true_on_success(
                sync_table, PACKAGE_IN_SCOPE, TAP, TARGET
            )

    @staticmethod
    def test_sync_table_exception_on_copy_table_returns_failed_table_name_and_exception():
        with patch(
            f'{PACKAGE_IN_SCOPE}.iceberg_routes.require_native_target_format'
        ):
            assertions.assert_sync_table_exception_on_failed_copy(
                sync_table, PACKAGE_IN_SCOPE, TAP, TARGET
            )

    def test_sync_table_rejects_iceberg_before_connector_construction(self):
        args = Namespace(target={'target_table_format': 'iceberg'})

        with patch(f'{PACKAGE_IN_SCOPE}.{TAP}') as tap, patch(
            f'{PACKAGE_IN_SCOPE}.{TARGET}'
        ) as target:
            with self.assertRaisesRegex(
                ValueError,
                'MongoDB-to-Snowflake FastSync does not support Iceberg targets',
            ):
                sync_table('table_1', args)

        tap.assert_not_called()
        target.assert_not_called()

    def test_native_contract_rejects_existing_iceberg_before_source_or_mutation(self):
        base_target = {
            'dbname': 'TARGET_DB',
            'default_target_schema': 'TARGET_SCHEMA',
            's3_bucket': 'staging-bucket',
            'tap_id': 'tap-id',
        }

        for format_settings in ({}, {'target_table_format': 'native'}):
            with self.subTest(format_settings=format_settings):
                args = Namespace(
                    tap={},
                    target={**base_target, **format_settings},
                    transform={},
                    properties={},
                    temp_dir='/tmp',
                    state='/tmp/state.json',
                )
                publisher = Mock()
                publisher.discover_table_format.return_value = (
                    TABLE_FORMAT_MANAGED_ICEBERG_V3
                )

                with patch(f'{PACKAGE_IN_SCOPE}.{TAP}') as source, patch(
                    f'{PACKAGE_IN_SCOPE}.{TARGET}'
                ) as target, patch.object(
                    snowflake_iceberg_routes,
                    'create_publisher',
                    return_value=publisher,
                ):
                    result = sync_table('source.table', args)

                self.assertIn('found managed_iceberg_v3', result)
                source.assert_not_called()
                self.assertEqual(target.return_value.method_calls, [])

    def test_native_format_race_stops_before_swap_or_state(self):
        args = assertions.FASTSYNC_NS
        format_error = TableCompatibilityError(
            'native target changed format after creation'
        )

        with patch(f'{PACKAGE_IN_SCOPE}.{TAP}') as source_class, patch(
            f'{PACKAGE_IN_SCOPE}.{TARGET}'
        ) as target_class, patch(
            f'{PACKAGE_IN_SCOPE}.utils'
        ) as utils, patch(
            f'{PACKAGE_IN_SCOPE}.os'
        ), patch(
            f'{PACKAGE_IN_SCOPE}.iceberg_routes.require_native_target_format',
            side_effect=[None, format_error],
        ) as native_format_guard:
            source = source_class.return_value
            target = target_class.return_value
            utils.get_target_schema.return_value = 'TARGET_SCHEMA'
            source.map_column_types_to_target.return_value = {
                'columns': ['"ID" NUMBER'],
                'primary_key': ['"ID"'],
            }
            target.upload_to_s3.return_value = 'staging/load.csv.gz'

            result = sync_table('source.table', args)

        self.assertEqual(
            result,
            'source.table: native target changed format after creation',
        )
        self.assertEqual(native_format_guard.call_args_list, [
            unittest.mock.call(
                target,
                args,
                'TARGET_SCHEMA',
                'source.table',
                allow_missing=True,
            ),
            unittest.mock.call(
                target,
                args,
                'TARGET_SCHEMA',
                'source.table',
                allow_missing=False,
            ),
        ])
        self.assertEqual(
            target.create_table.call_args_list[-1],
            unittest.mock.call(
                'TARGET_SCHEMA',
                'source.table',
                ['"ID" NUMBER'],
                ['"ID"'],
                allow_replace_table=False,
                normalize_primary_keys=False,
            ),
        )
        target.swap_tables.assert_not_called()
        utils.save_state_file.assert_not_called()
        utils.grant_privilege.assert_not_called()

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

    def test_main_impl_rejects_iceberg_before_pool_setup(self):
        args = Namespace(
            target={'target_table_format': 'iceberg'},
            tap={'fastsync_parallelism': 4},
        )

        with patch(
            f'{PACKAGE_IN_SCOPE}.utils.parse_args', return_value=args
        ), patch(f'{PACKAGE_IN_SCOPE}.utils.get_pool_size') as get_pool_size, patch(
            f'{PACKAGE_IN_SCOPE}.multiprocessing.Pool'
        ) as pool, patch(f'{PACKAGE_IN_SCOPE}.sync_table') as sync:
            with self.assertRaisesRegex(
                ValueError,
                'MongoDB-to-Snowflake FastSync does not support Iceberg targets',
            ):
                main_impl()

        get_pool_size.assert_not_called()
        pool.assert_not_called()
        sync.assert_not_called()


if __name__ == '__main__':
    unittest.main()
