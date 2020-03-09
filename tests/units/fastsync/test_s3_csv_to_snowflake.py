import multiprocessing
import unittest

from unittest.mock import patch, Mock
from argparse import Namespace

from pipelinewise.fastsync.s3_csv_to_snowflake import tap_type_to_target_type, sync_table, main_impl


# pylint: disable=missing-function-docstring,invalid-name,no-self-use
class S3CsvToSnowflake(unittest.TestCase):
    """
    Unit tests for fastsync s3 csv to snowflake
    """

    #######################################
    ##      tap_type_to_target_type
    #######################################
    def test_tap_type_to_target_type_with_defined_tap_type_returns_equivalent_target_type(self):
        self.assertEqual('VARCHAR', tap_type_to_target_type('Bool'))

    def test_tap_type_to_target_type_with_undefined_tap_type_returns_VARCHAR(self):
        self.assertEqual('VARCHAR', tap_type_to_target_type('random-type'))

    #######################################
    ##      sync_table
    #######################################
    # pylint: disable=unused-variable
    def test_sync_table_runs_successfully_returns_true(self):
        ns = Namespace(
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

        class LockMock():
            """
            Lock Mock
            """

            def acquire(self):
                print('Acquired lock')

            def release(self):
                print('Released lock')

        with patch('pipelinewise.fastsync.s3_csv_to_snowflake.FastSyncTapS3Csv') as fastsync_s3_csv_mock:
            with patch('pipelinewise.fastsync.s3_csv_to_snowflake.FastSyncTargetSnowflake') as fastsync_target_sf_mock:
                with patch('pipelinewise.fastsync.s3_csv_to_snowflake.utils') as utils_mock:
                    with patch('pipelinewise.fastsync.s3_csv_to_snowflake.multiprocessing') as multiproc_mock:
                        with patch('pipelinewise.fastsync.s3_csv_to_snowflake.os') as os_mock:
                            utils_mock.get_target_schema.return_value = 'my-target-schema'
                            fastsync_s3_csv_mock.return_value.map_column_types_to_target.return_value = {
                                'columns': ['id INTEGER', 'is_test SMALLINT', 'age INTEGER', 'name VARCHAR'],
                                'primary_key': 'id,name'
                            }

                            fastsync_target_sf_mock.return_value.upload_to_s3.return_value = 's3_key'
                            utils_mock.return_value.get_bookmark_for_table.return_value = {
                                'modified_since': '2019-11-18'
                            }
                            utils_mock.return_value.get_grantees.return_value = ['role_1', 'role_2']
                            utils_mock.return_value.get_bookmark_for_table.return_value = None

                            multiproc_mock.lock.return_value = LockMock()

                            res = sync_table('table_1', ns)

                            self.assertIsInstance(res, bool)
                            self.assertTrue(res)

    # pylint: disable=unused-variable
    def test_sync_table_exception_on_copy_table_returns_failed_table_name_and_exception(self):
        ns = Namespace(**{
            'tap': {
                'bucket': 'testBucket'
            },
            'properties': {},
            'target': {},
            'transform': {},
            'temp_dir': '',
        })

        with patch('pipelinewise.fastsync.s3_csv_to_snowflake.FastSyncTapS3Csv') as fastsync_s3_csv_mock:
            with patch('pipelinewise.fastsync.s3_csv_to_snowflake.FastSyncTargetSnowflake') as fastsync_target_sf_mock:
                with patch('pipelinewise.fastsync.s3_csv_to_snowflake.utils') as utils_mock:
                    with patch('pipelinewise.fastsync.s3_csv_to_snowflake.multiprocessing') as multiproc_mock:
                        utils_mock.get_target_schema.return_value = 'my-target-schema'
                        fastsync_s3_csv_mock.return_value.copy_table.side_effect = Exception('Boooom')

                        self.assertEqual('table_1: Boooom', sync_table('table_1', ns))

                        utils_mock.get_target_schema.assert_called_once()
                        fastsync_s3_csv_mock.return_value.copy_table.assert_called_once()

    # pylint: disable=unused-variable
    def test_main_impl_with_all_tables_synced_successfully_should_exit_normally(self):
        # mocks prep
        with patch('pipelinewise.fastsync.s3_csv_to_snowflake.utils') as utils_mock:
            with patch('pipelinewise.fastsync.s3_csv_to_snowflake.FastSyncTargetSnowflake') as fastsync_target_sf_mock:
                with patch('pipelinewise.fastsync.s3_csv_to_snowflake.sync_table') as sync_table_mock:
                    with patch('pipelinewise.fastsync.s3_csv_to_snowflake.multiprocessing') as multiproc_mock:
                        ns = Namespace(**{
                            'tables': ['table_1', 'table_2', 'table_3', 'table_4'],
                            'target': 'sf',
                            'transform': None
                        })

                        utils_mock.parse_args.return_value = ns
                        utils_mock.get_cpu_cores.return_value = 10
                        fastsync_target_sf_mock.return_value.clear_information_schema_columns_cache.return_value = None

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
                        fastsync_target_sf_mock.return_value.clear_information_schema_columns_cache.assert_called_once()

    # pylint: disable=unused-variable
    def test_main_impl_with_one_table_fails_to_sync_should_exit_with_error(self):
        # mocks prep
        with patch('pipelinewise.fastsync.s3_csv_to_snowflake.utils') as utils_mock:
            with patch('pipelinewise.fastsync.s3_csv_to_snowflake.FastSyncTargetSnowflake') as fastsync_target_sf_mock:
                with patch('pipelinewise.fastsync.s3_csv_to_snowflake.sync_table') as sync_table_mock:
                    with patch('pipelinewise.fastsync.s3_csv_to_snowflake.multiprocessing') as multiproc_mock:
                        ns = Namespace(**{
                            'tables': ['table_1', 'table_2', 'table_3', 'table_4'],
                            'target': 'sf',
                            'transform': None
                        })

                        utils_mock.parse_args.return_value = ns
                        utils_mock.get_cpu_cores.return_value = 10
                        fastsync_target_sf_mock.return_value.clear_information_schema_columns_cache.return_value = None

                        mock_enter = Mock()
                        mock_enter.return_value.map.return_value = [True, True, 'Critical: random error', True]

                        pool_mock = Mock(spec_set=multiprocessing.Pool).return_value

                        # to mock variable p in with statement, we need __enter__ and __exist__
                        pool_mock.__enter__ = mock_enter
                        pool_mock.__exit__ = Mock()
                        multiproc_mock.Pool.return_value = pool_mock

                        with self.assertRaises(SystemExit):
                            # call function
                            main_impl()

                            # assertions
                            utils_mock.parse_args.assert_called_once()
                            utils_mock.get_cpu_cores.assert_called_once()
                            mock_enter.return_value.map.assert_called_once()
                            fastsync_target_sf_mock.return_value.clear_information_schema_columns_cache.assert_called_once()


if __name__ == '__main__':
    unittest.main()
