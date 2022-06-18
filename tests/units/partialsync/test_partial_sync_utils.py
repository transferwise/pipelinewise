from unittest import TestCase, mock
from tempfile import TemporaryDirectory

from pipelinewise.fastsync.partialsync.utils import load_into_snowflake, upload_to_s3, update_state_file
from tests.units.partialsync.utils import PartialSync2SFArgs


class PartialSyncUtilsTestCase(TestCase):
    """Test case for partial sync utils"""

    def test_upload_to_s3(self):
        """Test _upload_to_s3 method"""
        with TemporaryDirectory() as temp_test_dir:
            test_file_part = f'{temp_test_dir}/foo.gz1'
            test_s3_key = 'foo_s3_key'
            with open(test_file_part, 'w', encoding='utf8') as file_to_test:
                file_to_test.write('bar')

            class MockedSnowflake(PartialSyncUtilsTestCase):
                """mock for snowflake"""
                def upload_to_s3(self, file_part, tmp_dir):
                    """mock for upload_to_s3 method"""
                    self.assertEqual(tmp_dir, temp_test_dir)
                    self.assertEqual(file_part, test_file_part)
                    return test_s3_key

            # pylint: disable=protected-access
            actual_return = upload_to_s3(MockedSnowflake(), [test_file_part], temp_test_dir)
            self.assertTupleEqual(([test_s3_key], test_s3_key), actual_return)


    # pylint: disable=protected-access
    def test_load_into_snowflake(self):
        """Test load_into_snowflake method"""
        test_table = 'weight_unit'

        with TemporaryDirectory() as temp_test_dir:
            test_end_value_cases = (None, '30')

            for test_end_value in test_end_value_cases:
                args = PartialSync2SFArgs(
                    temp_test_dir=temp_test_dir, table=test_table, start_value='20', end_value=test_end_value
                )
                test_target_schema = args.target['schema_mapping'][args.tap['dbname']]['target_schema']
                test_s3_key_pattern = ['s3_key_pattern_foo']
                test_size_byte = 4000
                test_s3_keys = ['s3_key_foo']
                test_tap_id = args.target['tap_id']
                test_bucket = args.target['s3_bucket']

                class MockedS3(PartialSyncUtilsTestCase):
                    """mock for S3"""

                    # pylint: disable=invalid-name, cell-var-from-loop
                    def delete_object(self, Bucket, Key):
                        """mock for delete_object method"""
                        self.assertEqual(Bucket, test_bucket)
                        self.assertEqual(Key, test_s3_keys[0])

                class MockedSnowflake(PartialSyncUtilsTestCase):
                    """mock for snowflake"""

                    # pylint: disable=cell-var-from-loop, invalid-name
                    def __init__(self):
                        super().__init__()
                        self.s3 = MockedS3()

                    # pylint: disable=no-self-use, cell-var-from-loop
                    def query(self, query_str):
                        """mocked query method"""
                        where_clause_for_end = f' AND {args.column} <= {args.end_value}' if args.end_value else ''
                        assert query_str == f'DELETE FROM {test_target_schema}."{test_table.upper()}"' \
                                            f' WHERE {args.column} >= {args.start_value}{where_clause_for_end}'

                    def copy_to_table(self, s3_key_pattern, target_schema, table, size_bytes, is_temporary=False):
                        """mocked copy_to_table method"""
                        self.assertEqual(s3_key_pattern, test_s3_key_pattern)
                        self.assertEqual(target_schema, test_target_schema)
                        self.assertEqual(table, args.table)
                        self.assertEqual(size_bytes, test_size_byte)
                        self.assertFalse(is_temporary)

                    def copy_to_archive(self, s3_key, tap_id, table):
                        """mocked copy_to_archive method"""
                        self.assertEqual(s3_key, test_s3_keys[0])
                        self.assertEqual(tap_id, test_tap_id)
                        self.assertEqual(table, args.table)

                load_into_snowflake(
                    MockedSnowflake(),
                    args,
                    test_s3_keys,
                    test_s3_key_pattern,
                    test_size_byte)


    # pylint: disable=no-self-use
    def test_update_state_file(self):
        """Test state file updating with and without end value"""
        bookmark = {'foo': 2}
        test_end_values = (None, 'bar')

        for end_value in test_end_values:
            with self.subTest(endvalue=end_value):
                with mock.patch('pipelinewise.fastsync.commons.utils.save_state_file') as mocked_save_state_file:
                    args = PartialSync2SFArgs(
                        temp_test_dir='foo_temp', table='FOO', start_value='20', end_value=end_value, state='foo_state'
                    )
                    update_state_file(args, bookmark)
                if end_value:
                    mocked_save_state_file.assert_not_called()
                else:
                    mocked_save_state_file.assert_called_with(args.state, args.table, bookmark)
