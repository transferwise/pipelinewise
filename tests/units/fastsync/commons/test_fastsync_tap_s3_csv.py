import datetime
import os
from unittest import TestCase
from unittest.mock import patch, Mock

from pipelinewise.fastsync.commons.tap_s3_csv import FastSyncTapS3Csv, S3Helper


# pylint: disable=missing-function-docstring,protected-access,invalid-name
class TestFastSyncTapS3Csv(TestCase):
    """
    Unit tests for fastsync common functions for tap s3 csv
    """

    def setUp(self) -> None:
        self.maxDiff = None
        con_config = {
            'bucket': 'testBucket',
            'aws_endpoint_url': 'https://aws.com/random-url',
            'start_date': '2000-01-01',
            'tables': [
                {
                    'table_name': 'table 1',
                    'key_properties': None
                }, {
                    'table_name': 'table 2',
                    'key_properties': []
                }, {
                    'table_name': 'table 3',
                    'key_properties': ['key_1']
                }, {
                    'table_name': 'table 4',
                    'key_properties': ['key_2', 'key_3']
                }
            ]
        }

        def tap_type_to_target_type(tap_type):
            return {
                'boolean': 'boolean',
                'integer': 'number',
                'number': 'number',
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
                }, {
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

    def test_get_file_records(self):
        with patch.object(S3Helper, 'get_file_handle') as get_file_handle_mock:
            handle = Mock().return_value
            handle.configure_mock(**{
                '_raw_stream.return_value': 'file handle'
            })

            get_file_handle_mock.return_value = handle.return_value

            with patch('pipelinewise.fastsync.commons.tap_s3_csv.singer_encodings_csv') as singer_encodings_csv_mock:
                singer_encodings_csv_mock.get_row_iterator.return_value = [
                    {
                        'id': 1,
                        'group': 'A',
                    }, {
                        'id': 2,
                        'group': 'A',
                        'test': True
                    }, {
                        'id': 3,
                        'group': 'B',
                    }
                ]

                with patch('pipelinewise.fastsync.commons.tap_s3_csv.datetime') as datetime_mock:
                    datetime_mock.utcnow.return_value.strftime.return_value = '2019-11-21'

                    records = []
                    headers = set()

                    self.fs_tap_s3_csv._get_file_records('s3 path 1',
                                                         {},
                                                         records,
                                                         headers)

                    self.assertListEqual([
                        {
                            S3Helper.SDC_SOURCE_BUCKET_COLUMN: 'testBucket',
                            S3Helper.SDC_SOURCE_FILE_COLUMN: 's3 path 1',
                            S3Helper.SDC_SOURCE_LINENO_COLUMN: 1,
                            '_SDC_EXTRACTED_AT': '2019-11-21',
                            '_SDC_BATCHED_AT': '2019-11-21',
                            '_SDC_DELETED_AT': None,
                            '"ID"': 1,
                            '"GROUP"': 'A',
                        }, {
                            S3Helper.SDC_SOURCE_BUCKET_COLUMN: 'testBucket',
                            S3Helper.SDC_SOURCE_FILE_COLUMN: 's3 path 1',
                            S3Helper.SDC_SOURCE_LINENO_COLUMN: 2,
                            '_SDC_EXTRACTED_AT': '2019-11-21',
                            '_SDC_BATCHED_AT': '2019-11-21',
                            '_SDC_DELETED_AT': None,
                            '"ID"': 2,
                            '"GROUP"': 'A',
                            '"TEST"': True,
                        }, {
                            S3Helper.SDC_SOURCE_BUCKET_COLUMN: 'testBucket',
                            S3Helper.SDC_SOURCE_FILE_COLUMN: 's3 path 1',
                            S3Helper.SDC_SOURCE_LINENO_COLUMN: 3,
                            '_SDC_EXTRACTED_AT': '2019-11-21',
                            '_SDC_BATCHED_AT': '2019-11-21',
                            '_SDC_DELETED_AT': None,
                            '"ID"': 3,
                            '"GROUP"': 'B',
                        }
                    ], records)

                    self.assertSetEqual(
                        {
                            '"ID"', '"GROUP"', '"TEST"',
                            S3Helper.SDC_SOURCE_LINENO_COLUMN, S3Helper.SDC_SOURCE_FILE_COLUMN,
                            S3Helper.SDC_SOURCE_BUCKET_COLUMN, '_SDC_EXTRACTED_AT',
                            '_SDC_BATCHED_AT', '_SDC_DELETED_AT'
                        },
                        headers)

    def test_fetch_current_incremental_key_pos_with_no_tables_in_dictionary_returns_empty_dict(self):
        self.assertFalse(self.fs_tap_s3_csv.fetch_current_incremental_key_pos('table-x'))

    def test_fetch_current_incremental_key_pos_with_tables_in_dictionary_returns_empty_dict(self):
        dt = datetime.datetime.strptime('2019-11-21', '%Y-%m-%d')
        self.fs_tap_s3_csv.tables_last_modified['table-x'] = dt
        self.assertEqual({'modified_since': dt.isoformat()},
                         self.fs_tap_s3_csv.fetch_current_incremental_key_pos('table-x', 'key'))

    def test_get_primary_keys_with_table_that_has_no_keys_returns_none(self):
        self.assertIsNone(self.fs_tap_s3_csv._get_primary_keys({}))

    def test_get_primary_keys_with_table_that_has_empty_keys_list_returns_none(self):
        self.assertIsNone(self.fs_tap_s3_csv._get_primary_keys({'key_properties': []}))

    def test_get_primary_keys_with_table_that_has_1_key_returns_one_safe_key(self):
        self.assertEqual(['"KEY_1"'], self.fs_tap_s3_csv._get_primary_keys({'key_properties': ['key_1']}))

    def test_get_primary_keys_with_table_that_has_2_keys_returns_concatenated_keys(self):
        self.assertIn(self.fs_tap_s3_csv._get_primary_keys({'key_properties': ['key_2', 'key_3']}),
                      [['"KEY_2"', '"KEY_3"'], ['"KEY_3"', '"KEY_2"']])

    def test_get_table_columns(self):
        output = list(
            self.fs_tap_s3_csv._get_table_columns(f'{os.path.dirname(__file__)}/resources/dummy_data.csv.gz'))

        self.assertListEqual([
            ('Region', 'string'),
            ('Country', 'string'),
            ('Item Type', 'string'),
            ('Sales Channel', 'string'),
            ('Order Priority', 'string'),
            ('Order Date', 'string'),
            ('Order ID', 'integer'),
            ('Ship Date', 'string'),
            ('Units Sold', 'integer'),
            ('Unit Price', 'number'),
            ('Unit Cost', 'number'),
            ('Total Revenue', 'number'),
            ('Total Cost', 'number'),
            ('Total Profit', 'number'),
        ], output)

    def test_map_column_types_to_target(self):
        output = self.fs_tap_s3_csv.map_column_types_to_target(
            f'{os.path.dirname(__file__)}/resources/dummy_data.csv.gz', 'table 1')

        self.assertDictEqual({
            'columns': [
                'Region varchar',
                'Country varchar',
                'Item Type varchar',
                'Sales Channel varchar',
                'Order Priority varchar',
                'Order Date varchar',
                'Order ID number',
                'Ship Date varchar',
                'Units Sold number',
                'Unit Price number',
                'Unit Cost number',
                'Total Revenue number',
                'Total Cost number',
                'Total Profit number',
            ],
            'primary_key': None
        }, output)
