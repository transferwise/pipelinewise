import unittest
from unittest.mock import call

from . import assertions

from pipelinewise.fastsync.postgres_to_snowflake import (
    tap_type_to_target_type,
    sync_table,
    main_impl,
)

PACKAGE_IN_SCOPE = 'pipelinewise.fastsync.postgres_to_snowflake'
TAP = 'FastSyncTapPostgres'
TARGET = 'FastSyncTargetSnowflake'


# pylint: disable=missing-function-docstring,invalid-name
class PostgresToSnowflake(unittest.TestCase):
    """
    Unit tests for fastsync postgres to snowflake
    """

    def test_tap_type_to_target_type_with_defined_tap_type_returns_equivalent_target_type(
        self,
    ):
        type_mappings = {
            'char': 'VARCHAR',
            'character': 'VARCHAR',
            'varchar': 'VARCHAR',
            'character varying': 'VARCHAR',
            'text': 'VARCHAR',
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
            'json': 'VARIANT',
            'jsonb': 'VARIANT',
        }

        for source_type, expected_type in type_mappings.items():
            with self.subTest(source_type=source_type):
                self.assertEqual(expected_type, tap_type_to_target_type(source_type))

    def test_tap_type_to_target_type_with_undefined_tap_type_returns_CHARACTER_VARYING(
        self,
    ):
        self.assertEqual('VARCHAR', tap_type_to_target_type('random-type'))

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
    def test_sync_table_exception_on_copy_table_returns_failed_table_name_and_exception():
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
