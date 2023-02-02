import os

from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from pipelinewise.fastsync.partialsync import mysql_to_snowflake
from pipelinewise.fastsync.partialsync.utils import parse_args_for_partial_sync
from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql
from tests.units.partialsync.utils import PartialSync2SFArgs, get_argv_list


class PartialSyncTestCase(TestCase):
    """Partial Sync test cases"""
    def setUp(self) -> None:
        resources_dir = f'{os.path.dirname(__file__)}/resources'
        self.config_dir = f'{resources_dir}/test_partial_sync'
        self.maxDiff = None  # pylint: disable=invalid-name

    def test_mysql_to_snowflake_partial_sync_table_if_exception_happens(self):
        """Test partial sync if an exception raises"""

        args = PartialSync2SFArgs(temp_test_dir='FOO_DIR')
        exception_message = 'FOO Exception!'
        test_table = ('foo', {
            'column': 'foo_column',
            'start_value': '1',
            'end_value': '2',
            'drop_target_table': False,
        })
        with mock.patch(
                'pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTapMySql.open_connections'
        ) as mocked_mysql_connection:
            mocked_mysql_connection.side_effect = Exception(exception_message)
            actual_return = mysql_to_snowflake.partial_sync_table(test_table, args)

        self.assertEqual(f'{args.table}: {exception_message}', actual_return)

    def test_export_source_table_data(self):
        """Test export_source_table_data method"""
        expected_file_parts = []

        with TemporaryDirectory() as temp_test_dir:
            args = PartialSync2SFArgs(temp_test_dir=temp_test_dir)

            # pylint: disable=unused-argument
            def mocked_copy_table_method(table, filepath, **kwargs):
                for part_number in range(3):
                    with open(f'{filepath}{part_number}', 'w', encoding='utf8') as data_file:
                        expected_file_parts.insert(0, f'{filepath}{part_number}')
                        data_file.write('foo')

            tap_id = 'tap_id_foo'
            with mock.patch('pipelinewise.fastsync.commons.tap_mysql.FastSyncTapMySql.copy_table') as mocked_copy_table:
                mocked_copy_table.side_effect = mocked_copy_table_method

                test_fast_sync = FastSyncTapMySql({}, {})

                where_clause = 'FOO WHERE'
                actual_file_parts = test_fast_sync.export_source_table_data(
                    args, tap_id, where_clause)

                call_args = mocked_copy_table.call_args[0]
                call_kwargs = mocked_copy_table.call_args[1]

                expected_call_kwargs = {
                    'split_large_files': False,
                    'split_file_chunk_size_mb': args.target['split_file_chunk_size_mb'],
                    'split_file_max_chunks': args.target['split_file_max_chunks'],
                    'where_clause_sql': where_clause
                }

        self.assertEqual(2, len(call_args))
        self.assertEqual(args.table, call_args[0])
        self.assertRegex(
            call_args[1],
            f'^{args.temp_dir}/pipelinewise_{tap_id}_{args.table}_[0-9]{{8}}-[0-9]{{6}}-[0-9]{{6}}'
            f'_partialsync_[0-9A-Z]{{8}}.csv.gz'
        )

        self.assertDictEqual(expected_call_kwargs, call_kwargs)
        self.assertEqual(len(actual_file_parts), len(expected_file_parts))
        for file_part in expected_file_parts:
            self.assertIn(file_part, actual_file_parts)

    # pylint: disable=too-many-locals, too-many-arguments

    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.multiprocessing.Pool')
    def test_running_partial_sync_mysql_to_snowflake(self, mocked_pool):
        """Test the whole partial_sync_mysql_to_snowflake module works as expected"""
        test_table = {}
        expected_args = None

        # pylint: disable=too-few-public-methods
        class MockedMultiprocessor:
            """"Mocked multiprocessing class"""
            @staticmethod
            def map(partial_func, itter_param):
                """Mocked map method which is used for assertion"""
                # Asserting if multiprocess calling is as expected
                actual_args = partial_func.keywords['args']

                assert itter_param == test_table.items()
                assert actual_args == expected_args
                return [True]

        class PoolClass:
            """Mocked pool class"""
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return MockedMultiprocessor()

            def __exit__(self, *args, **kwargs):
                pass

        mocked_pool.side_effect = PoolClass

        with TemporaryDirectory() as temp_directory:

            table_name = 'foo_table'
            column = 'foo_column'
            start_value = '1'
            test_end_values = ('10', None)
            for end_value in test_end_values:
                with self.subTest(endvalue=end_value):
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
                        test_table = {
                            table_name: {
                                'column': column,
                                'start_value': start_value,
                                'end_value': end_value,
                                'drop_target_table': False
                            }
                        }
                        argv_list = get_argv_list(arguments)
                        with mock.patch('sys.argv', argv_list):
                            expected_args = parse_args_for_partial_sync(mysql_to_snowflake.REQUIRED_CONFIG_KEYS)
                            mysql_to_snowflake.main()

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
                            'Exceptions during table sync   : []',
                        ]
                    ]
                    for log_index, log_messages in enumerate(expected_log_messages):
                        for message in log_messages:
                            self.assertIn(message, actual_logs.output[log_index])

    @mock.patch('pipelinewise.fastsync.partialsync.utils.load_into_snowflake')
    @mock.patch('pipelinewise.fastsync.partialsync.utils.upload_to_s3')
    @mock.patch('pipelinewise.fastsync.commons.utils.save_state_file')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTapMySql')
    @mock.patch('pipelinewise.fastsync.commons.utils.get_bookmark_for_table')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTargetSnowflake')
    def test_mysql_to_snowflake_partial_sync_table(self,
                                                   mocked_fastsync_sf, mocked_bookmark, mocked_fastsyncmysql,
                                                   mocked_save_state, mocked_upload_to_s3, mocked_load_into_sf):
        """Test mysql to sf partial sync table"""
        table_name = 'foo'
        test_end_values = (None, )

        for end_value in test_end_values:
            args = PartialSync2SFArgs(temp_test_dir='FOO_DIR', end_value=end_value)
            test_table = (table_name, {
                'column': 'foo_column',
                'start_value': '1',
                'end_value': end_value,
                'drop_target_table': False,
            })

            with TemporaryDirectory() as temp_directory:
                file_size = 5
                file_parts = [f'{temp_directory}/t1', ]
                s3_keys = ['FOO_S3_KEYS', ]
                s3_key_pattern = 'BAR_S3_KEY_PATTERN'
                bookmark = 'foo_bookmark'
                maped_column_types_to_target = {
                    'columns': ['foo type1', 'bar type2'],
                    'primary_key': 'foo_primary'
                }

                # pylint: disable=cell-var-from-loop
                def export_data_to_file(*args, **kwargs):  # pylint: disable=unused-argument
                    with open(f'{temp_directory}/t1', 'w', encoding='utf8') as exported_file:
                        exported_file.write('F' * file_size)

                    return file_parts

                mocked_upload_to_s3.return_value = (s3_keys, s3_key_pattern)
                mocked_bookmark.return_value = bookmark
                mocked_export_data = mocked_fastsyncmysql.return_value.export_source_table_data
                mocked_fastsyncmysql.return_value.map_column_types_to_target.return_value = maped_column_types_to_target
                mocked_export_data.side_effect = export_data_to_file

                actual_return = mysql_to_snowflake.partial_sync_table(test_table, args)
                self.assertTrue(actual_return)

                target = {
                    'schema': 'foo_schema',
                    'sf_object': mocked_fastsync_sf(),
                    'table': table_name,
                    'temp': 'foo_temp'
                }
                columns_diff = {
                    'added_columns': {'bar': 'type2', 'foo': 'type1'},
                    'removed_columns': {},
                    'source_columns': {'bar': 'type2', 'foo': 'type1'},
                    'target_columns': []
                }

                mocked_fastsync_sf.return_value.query.assert_called_with(
                    'UPDATE foo_schema."FOO" SET _SDC_DELETEd_AT = CURRENT_TIMESTAMP()'
                    ' WHERE foo_column >= \'1\' AND _SDC_DELETED_AT IS NULL'
                )
                mocked_fastsync_sf.return_value.create_schema.assert_called_with('foo_schema')
                mocked_fastsync_sf.return_value.create_table.assert_called_with(
                    'foo_schema', 'foo', ['foo type1', 'bar type2'], 'foo_primary', is_temporary=True)
                mocked_load_into_sf.assert_called_with(
                    target, args, columns_diff, maped_column_types_to_target['primary_key'],
                    s3_key_pattern, file_size, f" WHERE {test_table[1]['column']} >= '{test_table[1]['start_value']}'")

                if end_value:
                    mocked_save_state.assert_not_called()
                else:
                    mocked_save_state.assert_called_with('state.json', table_name, bookmark)
