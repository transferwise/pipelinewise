import unittest
from argparse import Namespace
from unittest.mock import call, Mock, patch

from . import assertions

from pipelinewise.fastsync.postgres_to_snowflake import (
    tap_type_to_target_type,
    sync_table,
    main_impl,
)
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    QueryHistoryLookupError,
    TABLE_FORMAT_MANAGED_ICEBERG_V3,
)
from pipelinewise.fastsync.commons import snowflake_iceberg_routes

PACKAGE_IN_SCOPE = 'pipelinewise.fastsync.postgres_to_snowflake'
TAP = 'FastSyncTapPostgres'
TARGET = 'FastSyncTargetSnowflake'


# pylint: disable=missing-function-docstring,invalid-name
class PostgresToSnowflake(unittest.TestCase):  # pylint: disable=too-many-public-methods
    """
    Unit tests for fastsync postgres to snowflake
    """

    def test_tap_type_to_target_type_with_defined_tap_type_returns_equivalent_target_type(
        self,
    ):
        type_mappings = {
            'char': 'VARCHAR(134217728)',
            'character': 'VARCHAR(134217728)',
            'varchar': 'VARCHAR(134217728)',
            'character varying': 'VARCHAR(134217728)',
            'text': 'VARCHAR(134217728)',
            'bit': 'BOOLEAN',
            'varbit': 'NUMBER',
            'bit varying': 'NUMBER',
            'smallint': 'NUMBER',
            'int': 'NUMBER',
            'integer': 'NUMBER',
            'bigint': 'NUMBER',
            'smallserial': 'NUMBER',
            'serial': 'NUMBER',
            'bigserial': 'NUMBER',
            'numeric': 'FLOAT',
            'double precision': 'FLOAT',
            'real': 'FLOAT',
            'bool': 'BOOLEAN',
            'boolean': 'BOOLEAN',
            'date': 'TIMESTAMP_NTZ',
            'timestamp': 'TIMESTAMP_NTZ',
            'timestamp without time zone': 'TIMESTAMP_NTZ',
            'timestamp with time zone': 'TIMESTAMP_NTZ',
            'time': 'TIME',
            'time without time zone': 'TIME',
            'time with time zone': 'TIME',
            'ARRAY': 'VARIANT',
            'hstore': 'VARCHAR(134217728)',
            'json': 'VARIANT',
            'jsonb': 'VARIANT',
        }

        for source_type, expected_type in type_mappings.items():
            with self.subTest(source_type=source_type):
                self.assertEqual(expected_type, tap_type_to_target_type(source_type))

    def test_tap_type_to_target_type_with_undefined_tap_type_returns_max_varchar(
        self,
    ):
        self.assertEqual(
            'VARCHAR(134217728)', tap_type_to_target_type('random-type')
        )

    def test_sync_table_rejects_removed_iceberg_create_before_connectors(self):
        args = Namespace(
            tap={},
            target={'iceberg_create': False},
            transform={},
        )

        with patch(f'{PACKAGE_IN_SCOPE}.{TAP}') as tap, patch(
            f'{PACKAGE_IN_SCOPE}.{TARGET}'
        ) as target:
            with self.assertRaisesRegex(ValueError, 'iceberg_create'):
                sync_table('source.table', args)

        tap.assert_not_called()
        target.assert_not_called()

    def test_main_impl_rejects_removed_iceberg_create_before_pool_or_connectors(self):
        args = Namespace(target={'iceberg_create': False})

        with patch(
            f'{PACKAGE_IN_SCOPE}.utils.parse_args', return_value=args
        ), patch(f'{PACKAGE_IN_SCOPE}.utils.get_pool_size') as get_pool_size, patch(
            f'{PACKAGE_IN_SCOPE}.{TAP}'
        ) as tap, patch(f'{PACKAGE_IN_SCOPE}.{TARGET}') as target, patch(
            f'{PACKAGE_IN_SCOPE}.multiprocessing.Pool'
        ) as pool:
            with self.assertRaisesRegex(ValueError, 'iceberg_create'):
                main_impl()

        get_pool_size.assert_not_called()
        tap.assert_not_called()
        target.assert_not_called()
        pool.assert_not_called()

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

    @staticmethod
    def test_sync_table_runs_successfully_returns_true():
        assertions.assert_snowflake_sync_table_native_workflow(
            sync_table, PACKAGE_IN_SCOPE, TAP, source_type='postgres',
            type_mapper=tap_type_to_target_type,
        )

    @staticmethod
    def test_sync_table_publish_failure_does_not_save_state():
        assertions.assert_snowflake_sync_table_native_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            publish_error=RuntimeError('publish failed'),
        )

    @staticmethod
    def test_sync_table_reports_state_failure():
        assertions.assert_snowflake_sync_table_native_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            state_error=RuntimeError('state failed'),
        )

    @staticmethod
    def test_sync_table_grant_failure_withholds_state():
        assertions.assert_snowflake_sync_table_native_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            grant_error=RuntimeError('grant failed'),
        )

    @staticmethod
    def test_sync_table_preserves_publication_and_finalization_failures():
        assertions.assert_snowflake_sync_table_native_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            publish_error=RuntimeError('publish failed'),
            grant_error=RuntimeError('grant failed'),
        )

    @staticmethod
    def test_sync_table_later_upload_failure_rolls_back_staging():
        assertions.assert_snowflake_sync_table_rolls_back_later_upload_failure(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
        )

    @staticmethod
    def test_sync_table_reports_and_retries_upload_cleanup_debt():
        assertions.assert_snowflake_sync_table_rolls_back_later_upload_failure(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            rollback_cleanup_error=RuntimeError('delete failed'),
        )

    @staticmethod
    def test_sync_table_publishes_iceberg_without_a_primary_key():
        assertions.assert_snowflake_sync_table_iceberg_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
        )

    @staticmethod
    def test_sync_table_iceberg_publish_failure_withholds_state():
        assertions.assert_snowflake_sync_table_iceberg_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            publish_error=RuntimeError('publish failed'),
            primary_key=['"ID"'],
        )

    @staticmethod
    def test_sync_table_finalized_iceberg_recovers_without_the_source():
        assertions.assert_snowflake_sync_table_iceberg_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            recovery_action='state_handoff',
            primary_key=['"ID"'],
            source_open_error=RuntimeError('source unavailable'),
        )

    @staticmethod
    def test_sync_table_published_iceberg_recovers_without_the_source():
        assertions.assert_snowflake_sync_table_iceberg_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            recovery_action='finalize',
            primary_key=['"ID"'],
            source_open_error=RuntimeError('source unavailable'),
        )

    @staticmethod
    def test_sync_table_query_history_lookup_failure_requires_unchanged_retry():
        error = QueryHistoryLookupError('attempt-1', 0.25, 1)
        assert 'retry the same FastSync command unchanged' in str(error)
        assertions.assert_snowflake_sync_table_iceberg_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            recovery_error=error,
            primary_key=['"ID"'],
        )

    @staticmethod
    def test_sync_table_recovery_publishes_the_persisted_contract():
        assertions.assert_snowflake_sync_table_iceberg_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            recovery_action='publish',
            primary_key=['"ID"'],
        )

    @staticmethod
    def test_sync_table_restarts_incomplete_iceberg_staging_with_saved_bookmark():
        assertions.assert_snowflake_sync_table_iceberg_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            recovery_action='restart_staging',
            primary_key=['"ID"'],
        )

    @staticmethod
    def test_sync_table_recovery_schema_mismatch_stops_before_publish():
        assertions.assert_snowflake_sync_table_iceberg_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            recovery_action='restart_staging',
            primary_key=['"ID"'],
            recovery_source_error=ValueError('persisted schema mismatch'),
        )

    @staticmethod
    def test_sync_table_persists_iceberg_upload_cleanup_debt():
        assertions.assert_snowflake_sync_table_iceberg_workflow(
            sync_table,
            PACKAGE_IN_SCOPE,
            TAP,
            source_type='postgres',
            type_mapper=tap_type_to_target_type,
            upload_cleanup_debt=True,
        )

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
