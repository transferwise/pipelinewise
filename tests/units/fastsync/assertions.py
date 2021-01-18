import pytest
import collections
import multiprocessing

from unittest.mock import patch, Mock
from argparse import Namespace


FASTSYNC_NS = Namespace(
    **{
        'tap': {
            'bucket': 'testBucket'
        },
        'properties': {},
        'target': {},
        'transform': {},
        'temp_dir': '',
        'state': '',
    })


def _create_object_names_to_mock(package_nm: str,
                                 tap_class_nm: str,
                                 target_class_nm: str):
    """Function to generate dynamic object names"""
    ObjectNames = collections.namedtuple('ObjectNames', ['full_tap_class_nm',
                                                         'full_target_class_nm',
                                                         'sync_table_fn_nm',
                                                         'utils_module_nm',
                                                         'multiproc_module_nm',
                                                         'os_module_nm'])
    return ObjectNames(full_tap_class_nm=f'{package_nm}.{tap_class_nm}',
                       full_target_class_nm=f'{package_nm}.{target_class_nm}',
                       sync_table_fn_nm=f'{package_nm}.sync_table',
                       utils_module_nm=f'{package_nm}.utils',
                       multiproc_module_nm=f'{package_nm}.multiprocessing',
                       os_module_nm=f'{package_nm}.os')


# pylint: disable=missing-function-docstring,unused-variable
def assert_sync_table_returns_true_on_success(sync_table: callable,
                                              package_nm: str,
                                              tap_class_nm: str,
                                              target_class_nm: str) -> None:
    """Tests if fastsync sync table function returns true on success"""
    objects_to_mock = _create_object_names_to_mock(package_nm, tap_class_nm, target_class_nm)

    class LockMock:
        """
        Lock Mock
        """
        @staticmethod
        def acquire():
            print('Acquired lock')

        @staticmethod
        def release():
            print('Released lock')

    with patch(objects_to_mock.full_tap_class_nm) as tap_mock:
        with patch(objects_to_mock.full_target_class_nm) as target_mock:
            with patch(objects_to_mock.utils_module_nm) as utils_mock:
                with patch(objects_to_mock.multiproc_module_nm) as multiproc_mock:
                    with patch(objects_to_mock.os_module_nm) as os_mock:
                        utils_mock.get_target_schema.return_value = 'my-target-schema'
                        tap_mock.return_value.map_column_types_to_target.return_value = {
                            'columns': ['id INTEGER', 'is_test SMALLINT', 'age INTEGER', 'name VARCHAR'],
                            'primary_key': 'id,name'
                        }

                        target_mock.return_value.upload_to_s3.return_value = 's3_key'
                        utils_mock.return_value.get_bookmark_for_table.return_value = {
                            'modified_since': '2019-11-18'
                        }
                        utils_mock.return_value.get_grantees.return_value = ['role_1', 'role_2']
                        utils_mock.return_value.get_bookmark_for_table.return_value = None

                        multiproc_mock.lock.return_value = LockMock()

                        res = sync_table('table_1', FASTSYNC_NS)

                        assert isinstance(res, bool)
                        assert res


# pylint: disable=missing-function-docstring,unused-variable,invalid-name
def assert_sync_table_exception_on_failed_copy(sync_table: callable,
                                               package_nm: str,
                                               tap_class_nm: str,
                                               target_class_nm: str) -> None:
    objects_to_mock = _create_object_names_to_mock(package_nm, tap_class_nm, target_class_nm)

    with patch(objects_to_mock.full_tap_class_nm) as tap_mock:
        with patch(objects_to_mock.full_target_class_nm) as target_mock:
            with patch(objects_to_mock.utils_module_nm) as utils_mock:
                with patch(objects_to_mock.multiproc_module_nm) as multiproc_mock:
                    utils_mock.get_target_schema.return_value = 'my-target-schema'
                    tap_mock.return_value.copy_table.side_effect = Exception('Boooom')

                    assert sync_table('table_1', FASTSYNC_NS) == 'table_1: Boooom'

                    utils_mock.get_target_schema.assert_called_once()
                    tap_mock.return_value.copy_table.assert_called_once()


# pylint: disable=missing-function-docstring,unused-variable,invalid-name
def assert_main_impl_exit_normally_on_success(main_impl: callable,
                                              package_nm: str,
                                              tap_class_nm: str,
                                              target_class_nm: str) -> None:
    objects_to_mock = _create_object_names_to_mock(package_nm, tap_class_nm, target_class_nm)

    with patch(objects_to_mock.utils_module_nm) as utils_mock:
        with patch(objects_to_mock.full_target_class_nm) as target_mock:
            with patch(objects_to_mock.sync_table_fn_nm) as sync_table_mock:
                with patch(objects_to_mock.multiproc_module_nm) as multiproc_mock:
                    with patch(objects_to_mock.full_tap_class_nm) as tap_mock:
                        tap_mock.return_value.drop_slot.side_effect = None

                        ns = Namespace(**{
                            'tables': ['table_1', 'table_2', 'table_3', 'table_4'],
                            'target': 'sf',
                            'transform': None,
                            'drop_pg_slot': False
                        })

                        utils_mock.parse_args.return_value = ns
                        utils_mock.get_cpu_cores.return_value = 10

                        mock_enter = Mock()
                        mock_enter.return_value.map.return_value = [True, True, True, True]

                        pool_mock = Mock(spec_set=multiprocessing.Pool).return_value

                        # to mock variable p in with statement, we need __enter__ and __exist__
                        pool_mock.__enter__ = mock_enter
                        pool_mock.__exit__ = Mock()
                        multiproc_mock.Pool.return_value = pool_mock

                        # call function
                        main_impl()

                        # assertions
                        utils_mock.parse_args.assert_called_once()
                        utils_mock.get_cpu_cores.assert_called_once()
                        mock_enter.return_value.map.assert_called_once()
                        tap_mock.return_value.drop_slot.assert_not_called()


# pylint: disable=missing-function-docstring,unused-variable,invalid-name
def assert_main_impl_should_exit_with_error_on_failure(main_impl: callable,
                                                       package_nm: str,
                                                       tap_class_nm: str,
                                                       target_class_nm: str) -> None:
    objects_to_mock = _create_object_names_to_mock(package_nm, tap_class_nm, target_class_nm)

    with patch(objects_to_mock.utils_module_nm) as utils_mock:
        with patch(objects_to_mock.full_target_class_nm) as target_mock:
            with patch(objects_to_mock.sync_table_fn_nm) as sync_table_mock:
                with patch(objects_to_mock.multiproc_module_nm) as multiproc_mock:
                    with patch(objects_to_mock.full_tap_class_nm) as tap_mock:
                        tap_mock.return_value.drop_slot.side_effect = None

                        ns = Namespace(**{
                            'tables': ['table_1', 'table_2', 'table_3', 'table_4'],
                            'target': 'sf',
                            'transform': None,
                            'drop_pg_slot': True,
                            'tap': {},
                        })

                        utils_mock.parse_args.return_value = ns
                        utils_mock.get_cpu_cores.return_value = 10

                        mock_enter = Mock()
                        mock_enter.return_value.map.return_value = [True, True, 'Critical: random error', True]

                        pool_mock = Mock(spec_set=multiprocessing.Pool).return_value

                        # to mock variable p in with statement, we need __enter__ and __exist__
                        pool_mock.__enter__ = mock_enter
                        pool_mock.__exit__ = Mock()
                        multiproc_mock.Pool.return_value = pool_mock

                        with pytest.raises(SystemExit):
                            main_impl()

                            # assertions
                            utils_mock.parse_args.assert_called_once()
                            utils_mock.get_cpu_cores.assert_called_once()
                            mock_enter.return_value.map.assert_called_once()
                            tap_mock.return_value.drop_slot.assert_called_once()
