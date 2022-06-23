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

            mocked_snowflake = mock.MagicMock()
            mocked_upload_to_s3 = mocked_snowflake.upload_to_s3
            mocked_upload_to_s3.return_value = test_s3_key

            # pylint: disable=protected-access
            actual_return = upload_to_s3(mocked_snowflake, [test_file_part], temp_test_dir)
            self.assertTupleEqual(([test_s3_key], test_s3_key), actual_return)
            mocked_upload_to_s3.assert_called_with(test_file_part, tmp_dir=temp_test_dir)

    # pylint: disable=protected-access
    def test_load_into_snowflake(self):
        """Test load_into_snowflake method"""
        test_table = 'weight_unit'

        with TemporaryDirectory() as temp_test_dir:
            test_end_value_cases = (None, '30')

            for test_end_value in test_end_value_cases:
                with self.subTest(endvalue= test_end_value):
                    args = PartialSync2SFArgs(
                        temp_test_dir=temp_test_dir, table=test_table, start_value='20', end_value=test_end_value
                    )
                    test_target_schema = args.target['schema_mapping'][args.tap['dbname']]['target_schema']
                    test_s3_key_pattern = ['s3_key_pattern_foo']
                    test_size_byte = 4000
                    test_s3_keys = ['s3_key_foo']
                    test_tap_id = args.target['tap_id']
                    test_bucket = args.target['s3_bucket']
                    where_clause_for_end = f' AND {args.column} <= {args.end_value}' if args.end_value else ''

                    mocked_snowflake = mock.MagicMock()

                    load_into_snowflake(mocked_snowflake, args, test_s3_keys, test_s3_key_pattern, test_size_byte)

                    mocked_snowflake.query.assert_called_with(
                        f'DELETE FROM {test_target_schema}."{test_table.upper()}"'
                        f' WHERE {args.column} >= {args.start_value}{where_clause_for_end}')

                    mocked_snowflake.copy_to_table.assert_called_with(
                        test_s3_key_pattern, test_target_schema, args.table, test_size_byte, is_temporary=False
                    )

                    mocked_snowflake.copy_to_archive.assert_called_with(test_s3_keys[0], test_tap_id, args.table)
                    mocked_snowflake.s3.delete_object.assert_called_with(Bucket=test_bucket, Key=test_s3_keys[0])

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
