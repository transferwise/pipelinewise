import datetime
from unittest import TestCase
from unittest.mock import patch, Mock

from pipelinewise.fastsync.commons.tap_s3_csv import FastSyncTapS3Csv


class TestFastSyncTapS3Csv(TestCase):

    def setUp(self) -> None:
        con_config = {
            'bucket': 'testBucket',
            'aws_endpoint_url': 'https://aws.com/random-url',
            'start_date': '2000-01-01',
            'tables': [
                {
                    "table_name": 'table 1'
                },
                {
                    "table_name": 'table 2'
                },
                {
                    "table_name": 'table 3'
                },
                {
                    "table_name": 'table 4'
                }
            ]
        }

        def tap_type_to_target_type(tap_type):
            return {
                'bool': 'smallint',
                'int': 'number',
                'string': 'varchar'
            }.get(tap_type, 'varchar')

        with patch('pipelinewise.fastsync.commons.tap_s3_csv.S3Helper') as s3_helper_mock:
            s3_helper_mock.return_value.list_files_in_bucket.return_value = []
            self.fs_tap_s3_csv = FastSyncTapS3Csv(con_config, tap_type_to_target_type)

    def test_copy_table_given_an_invalid_file_path_throws_exception(self):
        with self.assertRaises(Exception):
            self.fs_tap_s3_csv.copy_table('table_1', 'invalid_file_path.csv')

    def test_copy_table_given_a_valid_file_path(self):
        with patch('pipelinewise.fastsync.commons.tap_s3_csv.S3Helper') as s3_helper_mock:
            s3_helper_mock.get_input_files_for_table.return_value = [
                {
                    'key': 'file_1.csv',
                    'last_modified': datetime.datetime.strptime('2001-07-13', '%Y-%m-%d')
                },
                {
                    'key': 'file_2.csv',
                    'last_modified': datetime.datetime.strptime('2001-10-05', '%Y-%m-%d')
                }
            ]

            with patch.object(self.fs_tap_s3_csv, '_get_file_records') as get_file_rec_mock:
                get_file_rec_mock.return_value = 'test'

                with patch('pipelinewise.fastsync.commons.tap_s3_csv.gzip') as gzip_mock:

                    mock_enter = Mock()
                    mock_enter.return_value.open.return_value = ''

                    gzip_mock.return_value.__enter__ = mock_enter
                    gzip_mock.return_value.__exit__ = Mock()

                    self.fs_tap_s3_csv.copy_table('table 2', 'file_path.csv.gz')

                    self.assertEqual(2, get_file_rec_mock.call_count)
                    self.assertIn('table 2', self.fs_tap_s3_csv.tables_last_modified)
                    self.assertEqual('2001-10-05',
                                     self.fs_tap_s3_csv.tables_last_modified['table 2'].strftime('%Y-%m-%d'))