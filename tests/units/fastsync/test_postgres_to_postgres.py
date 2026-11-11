import unittest
from argparse import Namespace
from unittest.mock import patch

from . import assertions

from pipelinewise.fastsync.postgres_to_postgres import (
    tap_type_to_target_type,
    sync_table,
    main_impl,
)

PACKAGE_IN_SCOPE = 'pipelinewise.fastsync.postgres_to_postgres'
TAP = 'FastSyncTapPostgres'
TARGET = 'FastSyncTargetPostgres'


class PostgresToPostgres(unittest.TestCase):
    """
    Unit tests for postgres postgres to postgres
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


if __name__ == '__main__':
    unittest.main()

    @staticmethod
    def test_sync_table_tells_the_source_its_state_and_tables():
        """A new slot is refused while LOG_BASED streams outside this sync hold positions."""
        args = Namespace(
            tap={'dbname': 'db'}, target={}, transform={}, properties={}, temp_dir='',
            state='state.json', tables=['public.one', 'public.two'],
        )
        with patch(f'{PACKAGE_IN_SCOPE}.{TAP}') as tap_mock, patch(f'{PACKAGE_IN_SCOPE}.{TARGET}'), patch(
            f'{PACKAGE_IN_SCOPE}.utils',
        ) as utils_mock:
            utils_mock.get_bookmark_for_table.side_effect = RuntimeError('stop after the bookmark')
            sync_table('public.one', args)

        assert tap_mock.return_value.state_path == 'state.json'
        assert tap_mock.return_value.synced_tables == ['public.one', 'public.two']
