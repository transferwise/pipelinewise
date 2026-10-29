import unittest
from argparse import Namespace
from unittest.mock import patch

from . import assertions

from pipelinewise.fastsync import yugabyte_to_postgres
from pipelinewise.fastsync.yugabyte_to_postgres import (
    tap_type_to_target_type,
    sync_table,
    main_impl,
)

PACKAGE_IN_SCOPE = 'pipelinewise.fastsync.yugabyte_to_postgres'
TAP = 'FastSyncTapYugabyte'
TARGET = 'FastSyncTargetPostgres'


class YugabyteToPostgres(unittest.TestCase):
    """
    Unit tests for fastsync yugabyte to postgres
    """

    def test_tap_type_to_target_type_with_defined_tap_type_returns_equivalent_target_type(
        self,
    ):
        self.assertEqual('DOUBLE PRECISION', tap_type_to_target_type('serial'))

    def test_tap_type_to_target_type_with_undefined_tap_type_returns_CHARACTER_VARYING(
        self,
    ):
        self.assertEqual('CHARACTER VARYING', tap_type_to_target_type('random-type'))

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

    def test_log_based_tables_keeps_only_selected_log_based_streams(self):
        def stream(table, method):
            return {'table_name': table, 'metadata': [
                {'breadcrumb': [], 'metadata': {'schema-name': 'public', 'replication-method': method}},
                {'breadcrumb': ['properties', 'id'], 'metadata': {'inclusion': 'automatic'}},
            ]}

        properties = {'streams': [
            stream('logical', 'LOG_BASED'), stream('full', 'FULL_TABLE'), stream('unselected', 'LOG_BASED'),
        ]}

        self.assertEqual(
            ['public.logical'],
            yugabyte_to_postgres._log_based_tables(properties, ['public.logical', 'public.full']),
        )

    def test_main_impl_checks_replica_identity_before_any_table_or_slot_work(self):
        """The slot fixes each table's identity when it is created, so the check precedes every worker."""
        args = Namespace(tap={'tap_id': 't'}, target={}, transform={}, tables=['public.a'], properties={})
        events = []
        with patch(f'{PACKAGE_IN_SCOPE}.utils.parse_args', return_value=args), \
                patch(f'{PACKAGE_IN_SCOPE}.utils.get_pool_size', return_value=1), \
                patch(f'{PACKAGE_IN_SCOPE}.check_replica_identity',
                      side_effect=lambda *a: events.append('check') or (_ for _ in ()).throw(RuntimeError('CHANGE'))), \
                patch(f'{PACKAGE_IN_SCOPE}.{TARGET}') as target, \
                patch(f'{PACKAGE_IN_SCOPE}.multiprocessing.Pool') as pool:
            with self.assertRaisesRegex(RuntimeError, 'CHANGE'):
                main_impl()

        self.assertEqual(['check'], events)
        target.return_value.create_schemas.assert_not_called()
        pool.assert_not_called()

    def test_check_replica_identity_passes_log_based_tables_to_the_source(self):
        properties = {'streams': [{'table_name': 'a', 'metadata': [
            {'breadcrumb': [], 'metadata': {'schema-name': 'public', 'replication-method': 'LOG_BASED'}}]}]}
        with patch(f'{PACKAGE_IN_SCOPE}.{TAP}.check_postgres_replica_identity') as check:
            yugabyte_to_postgres.check_replica_identity({'host': 'h'}, properties, ['public.a'])

        check.assert_called_once_with({'host': 'h'}, ['public.a'])


if __name__ == '__main__':
    unittest.main()
