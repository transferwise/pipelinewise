import os

from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from pipelinewise.fastsync.partialsync import mysql_to_snowflake
from tests.units.partialsync.utils import MySQL2SFArgs, run_mysql_to_snowflake


class PartialSyncTestCase(TestCase):
    """Partial Sync test cases"""
    def setUp(self) -> None:
        resources_dir = f'{os.path.dirname(__file__)}/resources'
        self.config_dir = f'{resources_dir}/test_partial_sync'
        self.maxDiff = None  # pylint: disable=invalid-name

    def test_mysql_to_snowflake_partial_sync_table_if_exception_happens(self):
        """Test partial sync if an exception raises"""
        # TODO: an exception in database connection!

        args = MySQL2SFArgs(temp_test_dir='FOO_DIR')
        exception_message = 'FOO Exception!'
        with mock.patch(
                'pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTapMySql.open_connections'
        ) as mocked_mysql_connection:
            mocked_mysql_connection.side_effect = Exception(exception_message)
            actual_return = mysql_to_snowflake.partial_sync_table(args)

        self.assertEqual(f'{args.table}: {exception_message}', actual_return)

    def test_export_source_table_data(self):
        """Test _export_source_table_data method"""
        expected_file_parts = []

        with TemporaryDirectory() as temp_test_dir:

            args = MySQL2SFArgs(temp_test_dir=temp_test_dir)

            # pylint: disable=unused-argument
            def mocked_copy_table(table, filepath, **kwargs):
                for part_number in range(3):
                    with open(f'{filepath}{part_number}', 'w', encoding='utf8') as data_file:
                        expected_file_parts.insert(0, f'{filepath}{part_number}')
                        data_file.write('foo')

            tap_id = 'tap_id_foo'
            with mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTapMySql') as mocked_mysql:
                mysql_instance = mocked_mysql.return_value
                mysql_instance.copy_table.side_effect = mocked_copy_table

                # pylint: disable=protected-access
                actual_file_parts = mysql_to_snowflake._export_source_table_data(args, tap_id)

                call_args = mysql_instance.copy_table.call_args.args
                call_kwargs = mysql_instance.copy_table.call_args.kwargs

                expected_call_kwargs = {
                    'split_large_files': False,
                    'split_file_chunk_size_mb': args.target['split_file_chunk_size_mb'],
                    'split_file_max_chunks': args.target['split_file_max_chunks'],
                    'where_clause_setting': {'column': 'FOO_COLUMN', 'start_value': 'FOO_START', 'end_value': 'FOO_END'}
                }

        self.assertEqual(2, len(call_args))
        self.assertEqual(args.table, call_args[0])
        self.assertRegex(call_args[1],
                        f'^{args.temp_dir}/pipelinewise_{tap_id}_{args.table}_[0-9]{{8}}-[0-9]{{6}}-[0-9]{{6}}'
                        f'_partialsync_[0-9A-Z]{{8}}.csv.gz')

        self.assertDictEqual(expected_call_kwargs, call_kwargs)
        self.assertEqual(len(actual_file_parts), len(expected_file_parts))
        for file_part in expected_file_parts:
            self.assertIn(file_part, actual_file_parts)

    def test_upload_to_s3(self):
        """Test _upload_to_s3 method"""
        with TemporaryDirectory() as temp_test_dir:
            test_file_part = f'{temp_test_dir}/foo.gz1'
            test_s3_key = 'foo_s3_key'
            with open(test_file_part, 'w', encoding='utf8') as file_to_test:
                file_to_test.write('bar')

            class MockedSnowflake(PartialSyncTestCase):
                """mock for snowflake"""
                def upload_to_s3(self, file_part, tmp_dir):
                    """mock for upload_to_s3 method"""
                    self.assertEqual(tmp_dir, temp_test_dir)
                    self.assertEqual(file_part, test_file_part)
                    return test_s3_key

            # pylint: disable=protected-access
            actual_return = mysql_to_snowflake._upload_to_s3(MockedSnowflake(), [test_file_part], temp_test_dir)
            self.assertTupleEqual(([test_s3_key], test_s3_key), actual_return)

    # pylint: disable=protected-access
    def test_load_into_snowflake(self):
        """Test _load_into_snowflake method"""
        test_table = 'weight_unit'

        with TemporaryDirectory() as temp_test_dir:
            test_end_value_cases = (None, '30')

            for test_end_value in test_end_value_cases:
                args = MySQL2SFArgs(
                    temp_test_dir=temp_test_dir, table=test_table, start_value='20', end_value=test_end_value
                )
                test_target_schema = args.target['schema_mapping'][args.tap['dbname']]['target_schema']
                test_s3_key_pattern = ['s3_key_pattern_foo']
                test_size_byte = 4000
                test_s3_keys = ['s3_key_foo']
                test_tap_id = args.target['tap_id']
                test_bucket = args.target['s3_bucket']

                class MockedS3(PartialSyncTestCase):
                    """mock for S3"""

                    # pylint: disable=invalid-name, cell-var-from-loop
                    def delete_object(self, Bucket, Key):
                        """mock for delete_object method"""
                        self.assertEqual(Bucket, test_bucket)
                        self.assertEqual(Key, test_s3_keys[0])

                class MockedSnowflake(PartialSyncTestCase):
                    """mock for snowflake"""

                    # pylint: disable=cell-var-from-loop, invalid-name
                    def __init__(self):
                        super().__init__()
                        self.s3 = MockedS3()

                    # pylint: disable=no-self-use, cell-var-from-loop
                    def query(self, query_str):
                        """mocked query method"""
                        where_clause_for_end = f' AND {args.column} <= {args.end_value}' if args.end_value else ''
                        assert query_str == f'DELETE FROM {test_target_schema}.{test_table}' \
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

                mysql_to_snowflake._load_into_snowflake(
                    MockedSnowflake(),
                    args,
                    test_s3_keys,
                    test_s3_key_pattern,
                    test_size_byte)

    # pylint: disable=too-many-locals, too-many-arguments
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.utils.save_state_file')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake._load_into_snowflake')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake._upload_to_s3')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake._export_source_table_data')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.utils.get_bookmark_for_table')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTapMySql')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTargetSnowflake')
    def test_running_partial_sync_mysql_to_snowflake(self,
                                                     mocked_fastsync_sf,
                                                     mocked_fastsyncmysql,
                                                     mocked_bookmark,
                                                     mocked_export_data,
                                                     mocked_upload_to_s3,
                                                     mocked_load_into_sf,
                                                     mocked_save_state):
        """Test the whole partial_sync_mysql_to_snowflake module works as expected"""
        with TemporaryDirectory() as temp_directory:
            file_size = 5
            file_parts = [f'{temp_directory}/t1',]
            s3_keys = ['FOO_S3_KEYS',]
            s3_key_pattern = 'BAR_S3_KEY_PATTERN'
            bookmark = 'foo_bookmark'

            def export_data_to_file(*args, **kwargs):  # pylint: disable=unused-argument
                with open(f'{temp_directory}/t1', 'w', encoding='utf8') as exported_file:
                    exported_file.write('F'*file_size)

                return file_parts

            mocked_fastsyncmysql.return_va = mock.MagicMock()
            mocked_upload_to_s3.return_value = (s3_keys, s3_key_pattern)
            mocked_bookmark.return_value = bookmark
            mocked_export_data.side_effect = export_data_to_file

            table_name = 'foo_table'
            column = 'foo_column'
            start_value = '1'
            test_end_values = ('10', None)
            for end_value in test_end_values:
                arguments = {
                    'tap': f'{self.config_dir}/target_snowflake/tap_mysql/config.json',
                    'target': f'{self.config_dir}/tmp/target_config_tmp.json',
                    'properties': 'foo_properties',
                    'state': 'foo_state',
                    'temp_dir': temp_directory,
                    'transform': 'foo_transform',
                    'table': table_name,
                    'column': column,
                    'start_value': start_value,
                    'end_value': end_value
                }

                with self.assertLogs('pipelinewise') as actual_logs:
                    args_namespace = run_mysql_to_snowflake(arguments)

                expected_log_messages = [
                    [
                        'STARTING PARTIAL SYNC',
                        f'Table selected to sync         : {table_name}',
                        f'Column                         : {column}',
                        f'Start value                    : {start_value}',
                        f'End value                      : {end_value}',
                    ],
                    [
                        'PARTIAL SYNC FINISHED - SUMMARY',
                        f'Table selected to sync         : {table_name}',
                        f'Column                         : {column}',
                        f'Start value                    : {start_value}',
                        f'End value                      : {end_value}',
                        f'Exceptions during table sync   : {[]}',
                    ]
                ]
                for log_index, log_messages in enumerate(expected_log_messages):
                    for message in log_messages:
                        self.assertIn(message, actual_logs.output[log_index])

                mocked_export_data.assert_called_with(args_namespace, args_namespace.target.get('tap_id'))
                mocked_upload_to_s3.assert_called_with(mocked_fastsync_sf(), file_parts, arguments['temp_dir'])
                mocked_load_into_sf.assert_called_with(
                    mocked_fastsync_sf(), args_namespace, s3_keys, s3_key_pattern, file_size
                )
                if end_value:
                    mocked_save_state.assert_not_called()
                else:
                    mocked_save_state.assert_called_with(arguments['state'], table_name, bookmark)
