import unittest

from unittest.mock import patch

from pipelinewise.fastsync.s3_csv_to_snowflake import tap_type_to_target_type, sync_table


class S3CsvToSnowflake(unittest.TestCase):

    #######################################
    ##      tap_type_to_target_type
    #######################################
    def test_tap_type_to_target_type_with_defined_tap_type_returns_equivalent_target_type(self):
        self.assertEqual('SMALLINT', tap_type_to_target_type('Bool'))

    def test_tap_type_to_target_type_with_undefined_tap_type_returns_VARCHAR(self):
        self.assertEqual('VARCHAR', tap_type_to_target_type('random-type'))

    #######################################
    ##      sync_table
    #######################################
    def test_sync_table_runs_successfully_returns_true(self):
        from argparse import Namespace
        ns = Namespace(**{
            'tap': {
                'bucket': 'testBucket'
            },
            'properties': {},
            'target': {},
            'transform': {},
            'export_dir': '',
            'state': '',

        })

        class LockMock():
            def acquire(self):
                print('Acquired lock')

            def release(self):
                print('Released lock')

        with patch('pipelinewise.fastsync.s3_csv_to_snowflake.FastSyncTapS3Csv') as fastsync_s3_csv_mock:
            with patch('pipelinewise.fastsync.s3_csv_to_snowflake.FastSyncTargetSnowflake') as fastsync_target_sf_mock:
                with patch('pipelinewise.fastsync.s3_csv_to_snowflake.utils') as utils_mock:
                    with patch('pipelinewise.fastsync.s3_csv_to_snowflake.multiprocessing') as multiproc_mock:
                        with patch('pipelinewise.fastsync.s3_csv_to_snowflake.os') as os_mock:
                            utils_mock.return_value.get_target_schema.return_value = 'my-target-schema'
                            fastsync_s3_csv_mock.return_value.map_column_types_to_target.return_value = {
                                'columns': ['id INTEGER','is_test SMALLINT', 'age INTEGER', 'name VARCHAR'],
                                'primary_key': 'id,name'
                            }

                            fastsync_target_sf_mock.return_value.upload_to_s3.return_value = 's3_key'
                            utils_mock.return_value.get_bookmark_for_table.return_value = {
                                'modified_since': '2019-11-18'
                            }
                            utils_mock.return_value.get_grantees.return_value = ['role_1', 'role_2']
                            utils_mock.return_value.get_bookmark_for_table.return_value = None

                            multiproc_mock.return_value.lock.return_value = LockMock()

                            res = sync_table('table_1', ns)

                            self.assertIsInstance(res, bool)
                            self.assertTrue(res)

    def test_sync_table_exception_on_copy_table_returns_failed_table_name_and_exception(self):
        from argparse import Namespace
        ns = Namespace(**{
            'tap': {
                'bucket': 'testBucket'
            },
            'properties': {},
            'target': {},
            'transform': {},
            'export_dir': '',
        })

        with patch('pipelinewise.fastsync.s3_csv_to_snowflake.FastSyncTapS3Csv') as fastsync_s3_csv_mock:
            with patch('pipelinewise.fastsync.s3_csv_to_snowflake.FastSyncTargetSnowflake') as fastsync_target_sf_mock:
                with patch('pipelinewise.fastsync.s3_csv_to_snowflake.utils') as utils_mock:
                    with patch('pipelinewise.fastsync.s3_csv_to_snowflake.multiprocessing') as multiproc_mock:
                        utils_mock.return_value.get_target_schema.return_value = 'my-target-schema'
                        fastsync_s3_csv_mock.return_value.copy_table.side_effect = Exception('Boooom')

                        self.assertEqual('table_1: Boooom', sync_table('table_1', ns))

if __name__ == '__main__':
    unittest.main()
