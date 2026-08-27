import os

from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from pipelinewise.fastsync.partialsync import mysql_to_snowflake
from pipelinewise.fastsync.partialsync.utils import parse_args_for_partial_sync
from pipelinewise.fastsync.commons.tap_mysql import FastSyncTapMySql
from pipelinewise.fastsync.commons.partial_sync_boundary import (
    PartialSyncBoundary,
)
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    QueryHistoryLookupError,
    TABLE_FORMAT_MANAGED_ICEBERG_V3,
)
from pipelinewise.fastsync.commons import snowflake_iceberg_routes
from tests.units.partialsync.utils import (
    PartialSync2SFArgs,
    assert_iceberg_partial_sync_workflow,
    get_argv_list,
)


class PartialSyncTestCase(TestCase):
    """Partial Sync test cases"""
    def setUp(self) -> None:
        resources_dir = f'{os.path.dirname(__file__)}/resources'
        self.config_dir = f'{resources_dir}/test_partial_sync'
        self.maxDiff = None  # pylint: disable=invalid-name

    def test_mysql_partial_sync_rejects_removed_iceberg_create_before_connectors(self):
        """Reject legacy routing before creating source or target connectors."""
        args = PartialSync2SFArgs(temp_test_dir='FOO_DIR')
        args.target = {'iceberg_create': False}
        table = ('foo', {'column': 'id'})

        with mock.patch.object(
            mysql_to_snowflake, 'FastSyncTapMySql'
        ) as source, mock.patch.object(
            mysql_to_snowflake, 'FastSyncTargetSnowflake'
        ) as target:
            with self.assertRaisesRegex(ValueError, 'iceberg_create'):
                mysql_to_snowflake.partial_sync_table(table, args)

        source.assert_not_called()
        target.assert_not_called()

    def test_mysql_main_rejects_removed_iceberg_create_before_pool_or_connectors(self):
        """Reject legacy routing before starting workers or connectors."""
        args = PartialSync2SFArgs(temp_test_dir='FOO_DIR')
        args.target = {'iceberg_create': False}

        with mock.patch.object(
            mysql_to_snowflake.utils,
            'parse_args_for_partial_sync',
            return_value=args,
        ), mock.patch.object(
            mysql_to_snowflake.common_utils, 'get_pool_size'
        ) as get_pool_size, mock.patch.object(
            mysql_to_snowflake, 'FastSyncTapMySql'
        ) as source, mock.patch.object(
            mysql_to_snowflake, 'FastSyncTargetSnowflake'
        ) as target, mock.patch.object(
            mysql_to_snowflake.multiprocessing, 'Pool'
        ) as pool:
            with self.assertRaisesRegex(ValueError, 'iceberg_create'):
                mysql_to_snowflake.main_impl()

        get_pool_size.assert_not_called()
        source.assert_not_called()
        target.assert_not_called()
        pool.assert_not_called()

    def test_native_contract_rejects_existing_iceberg_before_source_or_mutation(self):
        """Reject an existing Iceberg target before static native source work."""
        table = ('foo', {
            'column': 'id',
            'start_value': '<S>1',
            'end_value': '<S>2',
            'drop_target_table': False,
        })

        for table_format in (None, 'native'):
            with self.subTest(table_format=table_format):
                args = PartialSync2SFArgs(
                    temp_test_dir='FOO_DIR',
                    target_table_format=table_format,
                )
                publisher = mock.Mock()
                publisher.discover_table_format.return_value = (
                    TABLE_FORMAT_MANAGED_ICEBERG_V3
                )

                with mock.patch.object(
                    mysql_to_snowflake, 'FastSyncTapMySql'
                ) as source, mock.patch.object(
                    mysql_to_snowflake, 'FastSyncTargetSnowflake'
                ) as target, mock.patch.object(
                    snowflake_iceberg_routes,
                    'create_publisher',
                    return_value=publisher,
                ):
                    result = mysql_to_snowflake.partial_sync_table(table, args)

                self.assertIn('found managed_iceberg_v3', result)
                source.assert_not_called()
                self.assertEqual(target.return_value.method_calls, [])

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
            'pipelinewise.fastsync.partialsync.mysql_to_snowflake.'
            'FastSyncTapMySql.open_connections'
        ) as mocked_mysql_connection, mock.patch.object(
            mysql_to_snowflake.iceberg_routes,
            'require_native_target_format',
        ):
            mocked_mysql_connection.side_effect = Exception(exception_message)
            actual_return = mysql_to_snowflake.partial_sync_table(test_table, args)

        self.assertEqual(f'{test_table[0]}: {exception_message}', actual_return)
        self.assertEqual(args.table, 'mysql_source_db.email')

    @mock.patch('pipelinewise.fastsync.commons.utils.save_state_file')
    @mock.patch('pipelinewise.fastsync.commons.utils.get_bookmark_for_table', return_value='bookmark')
    @mock.patch(
        'pipelinewise.fastsync.partialsync.utils.upload_to_s3',
        return_value=(['s3-key'], 's3-pattern'),
    )
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTapMySql')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTargetSnowflake')
    @mock.patch.object(
        mysql_to_snowflake.iceberg_routes,
        'require_native_target_format',
    )
    def test_mysql_partial_sync_failure_state_semantics(
        self,
        _mocked_native_format,
        mocked_fastsync_sf,
        mocked_fastsyncmysql,
        _mocked_upload,
        _mocked_bookmark,
        mocked_save_state,
    ):
        """Publication and staging-cleanup failures all withhold state."""
        for failure_method, error_message in (
            ('copy_to_table', 'copy failed'),
            ('publish_partial_sync', 'transaction failed'),
            ('drop_table', 'cleanup failed'),
        ):
            with self.subTest(failure_method=failure_method), TemporaryDirectory() as temp_directory:
                args = PartialSync2SFArgs(temp_test_dir=temp_directory, end_value=None)
                test_table = ('foo', {
                    'column': 'foo_column',
                    'start_value': '<S>1',
                    'end_value': None,
                    'drop_target_table': False,
                })
                file_part = f'{temp_directory}/part.csv.gz'
                with open(file_part, 'w', encoding='utf8') as exported_file:
                    exported_file.write('data')

                source = mock.MagicMock()
                source.export_source_table_data.return_value = [file_part]
                source.map_column_types_to_target.return_value = {
                    'columns': ['"ID" NUMBER'],
                    'primary_key': ['"ID"'],
                    'source_column_names': ['foo_column'],
                }
                mocked_fastsyncmysql.return_value = source

                snowflake = mock.MagicMock()
                snowflake.query.return_value = []
                getattr(snowflake, failure_method).side_effect = RuntimeError(error_message)
                mocked_fastsync_sf.return_value = snowflake
                mocked_save_state.reset_mock()

                result = mysql_to_snowflake.partial_sync_table(test_table, args)

                source.close_connections.assert_called_once_with(silent=True)
                if failure_method == 'drop_table':
                    self.assertIn('foo: cleanup failed; staging cleanup failed:', result)
                else:
                    self.assertEqual(f'foo: {error_message}', result)
                mocked_save_state.assert_not_called()
                snowflake.s3.delete_object.assert_called_once_with(
                    Bucket=args.target['s3_bucket'], Key='s3-key'
                )

                if failure_method == 'copy_to_table':
                    snowflake.obfuscate_columns.assert_not_called()
                    snowflake.publish_partial_sync.assert_not_called()
                    self.assertEqual(snowflake.create_table.call_count, 1)
                elif failure_method == 'publish_partial_sync':
                    snowflake.copy_to_table.assert_called_once_with(
                        's3-pattern', 'foo_schema', 'foo', 4, is_temporary=True
                    )
                    snowflake.publish_partial_sync.assert_called_once()
                expected_drop_call = mock.call(
                    'foo_schema',
                    'foo',
                    is_temporary=True,
                    max_attempts=3,
                )
                self.assertEqual(
                    snowflake.drop_table.call_args_list,
                    [expected_drop_call]
                    * (2 if failure_method == 'drop_table' else 1),
                )

    @mock.patch('pipelinewise.fastsync.commons.utils.save_state_file')
    @mock.patch(
        'pipelinewise.fastsync.commons.utils.get_bookmark_for_table',
        return_value='bookmark',
    )
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTapMySql')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTargetSnowflake')
    @mock.patch.object(
        mysql_to_snowflake.iceberg_routes,
        'require_native_target_format',
    )
    def test_mysql_empty_unbounded_sync_publishes_and_advances_state(
        self,
        _mocked_native_format,
        mocked_fastsync_sf,
        mocked_fastsyncmysql,
        _mocked_bookmark,
        mocked_save_state,
    ):
        """An empty MariaDB/MySQL export still publishes its range and bookmark."""
        args = PartialSync2SFArgs(temp_test_dir='FOO_DIR', end_value=None)
        test_table = ('foo', {
            'column': 'foo_column',
            'start_value': '<S>1',
            'end_value': None,
            'drop_target_table': False,
        })
        source = mocked_fastsyncmysql.return_value
        source.export_source_table_data.return_value = []
        source.map_column_types_to_target.return_value = {
            'columns': ['"ID" NUMBER'],
            'primary_key': ['"ID"'],
            'source_column_names': ['foo_column'],
        }
        snowflake = mocked_fastsync_sf.return_value
        snowflake.query.return_value = []

        result = mysql_to_snowflake.partial_sync_table(test_table, args)

        self.assertIs(result, True)
        snowflake.copy_to_table.assert_called_once_with(
            'NO_FILES_TO_LOAD', 'foo_schema', 'foo', 0, is_temporary=True
        )
        snowflake.publish_partial_sync.assert_called_once()
        snowflake.s3.delete_object.assert_not_called()
        mocked_save_state.assert_called_once_with(args.state, 'foo', 'bookmark')

    def test_mysql_iceberg_partial_sync_supports_replacement(self):
        """Iceberg PartialSync can replace the target when explicitly requested."""
        assert_iceberg_partial_sync_workflow(
            mysql_to_snowflake,
            'FastSyncTapMySql',
            drop_target=True,
        )

    def test_mysql_iceberg_partial_sync_failure_withholds_state(self):
        """A failed Iceberg publication must not advance source state."""
        assert_iceberg_partial_sync_workflow(
            mysql_to_snowflake,
            'FastSyncTapMySql',
            publish_error=RuntimeError('publish failed'),
        )

    def test_mysql_finalized_iceberg_partial_sync_recovers_without_the_source(self):
        """A finalized attempt hands off state without opening the source."""
        assert_iceberg_partial_sync_workflow(
            mysql_to_snowflake,
            'FastSyncTapMySql',
            recovery_action='state_handoff',
            source_open_error=RuntimeError('source unavailable'),
        )

    def test_mysql_published_iceberg_partial_sync_recovers_without_the_source(self):
        """A published attempt finalizes without opening the source."""
        assert_iceberg_partial_sync_workflow(
            mysql_to_snowflake,
            'FastSyncTapMySql',
            recovery_action='finalize',
            source_open_error=RuntimeError('source unavailable'),
        )

    def test_mysql_query_history_lookup_failure_requires_unchanged_retry(self):
        """Ambiguous recovery stops before opening the source or publishing."""
        error = QueryHistoryLookupError('attempt-1', 0.25, 1)
        self.assertIn('retry the same FastSync command unchanged', str(error))
        assert_iceberg_partial_sync_workflow(
            mysql_to_snowflake,
            'FastSyncTapMySql',
            recovery_error=error,
        )

    def test_mysql_iceberg_partial_sync_publishes_the_persisted_contract(self):
        """A staged recovery publishes its persisted schema contract."""
        assert_iceberg_partial_sync_workflow(
            mysql_to_snowflake,
            'FastSyncTapMySql',
            recovery_action='publish',
        )

    def test_mysql_iceberg_partial_sync_restarts_the_saved_range(self):
        """A staging restart reuses its persisted source range."""
        assert_iceberg_partial_sync_workflow(
            mysql_to_snowflake,
            'FastSyncTapMySql',
            recovery_action='restart_staging',
        )

    def test_mysql_iceberg_partial_sync_schema_mismatch_stops_before_publish(self):
        """An incompatible re-export cannot reach publication."""
        assert_iceberg_partial_sync_workflow(
            mysql_to_snowflake,
            'FastSyncTapMySql',
            recovery_action='restart_staging',
            recovery_source_error=ValueError('persisted schema mismatch'),
        )

    def test_mysql_iceberg_partial_sync_persists_upload_cleanup_debt(self):
        """A failed upload cleanup remains represented by the attempt."""
        assert_iceberg_partial_sync_workflow(
            mysql_to_snowflake,
            'FastSyncTapMySql',
            upload_cleanup_debt=True,
        )

    def test_mysql_iceberg_partial_sync_requires_primary_key_before_export(self):
        """A new PartialSync attempt requires a key before source export."""
        assert_iceberg_partial_sync_workflow(
            mysql_to_snowflake,
            'FastSyncTapMySql',
            missing_primary_key=True,
        )

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

                boundary = PartialSyncBoundary('foo_column', '1', '2')
                actual_file_parts = test_fast_sync.export_source_table_data(
                    args, tap_id, boundary)

                call_args = mocked_copy_table.call_args[0]
                call_kwargs = mocked_copy_table.call_args[1]

                expected_call_kwargs = {
                    'split_large_files': False,
                    'split_file_chunk_size_mb': args.target['split_file_chunk_size_mb'],
                    'split_file_max_chunks': args.target['split_file_max_chunks'],
                    'boundary': boundary,
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

    # pylint: disable=too-many-positional-arguments
    @mock.patch('pipelinewise.fastsync.partialsync.utils.load_into_snowflake')
    @mock.patch('pipelinewise.fastsync.partialsync.utils.upload_to_s3')
    @mock.patch('pipelinewise.fastsync.commons.utils.save_state_file')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTapMySql')
    @mock.patch('pipelinewise.fastsync.commons.utils.get_bookmark_for_table')
    @mock.patch('pipelinewise.fastsync.partialsync.mysql_to_snowflake.FastSyncTargetSnowflake')
    @mock.patch.object(
        mysql_to_snowflake.iceberg_routes,
        'require_native_target_format',
    )
    def test_mysql_to_snowflake_partial_sync_table(self,
                                                   _mocked_native_format,
                                                   mocked_fastsync_sf, mocked_bookmark, mocked_fastsyncmysql,
                                                   mocked_save_state, mocked_upload_to_s3, mocked_load_into_sf):
        """Test mysql to sf partial sync table"""
        table_name = 'foo'
        test_end_values = (None, )

        for end_value in test_end_values:
            args = PartialSync2SFArgs(temp_test_dir='FOO_DIR', end_value=end_value)
            test_table = (table_name, {
                'column': 'foo_column',
                'start_value': '<S>1',
                'end_value': end_value,
                'drop_target_table': False,
            })

            with TemporaryDirectory() as temp_directory:
                file_size = 5
                file_parts = [f'{temp_directory}/t1', ]
                s3_key_pattern = 'BAR_S3_KEY_PATTERN'
                bookmark = 'foo_bookmark'
                maped_column_types_to_target = {
                    'columns': ['foo type1', 'bar type2'],
                    'primary_key': 'foo_primary',
                    'source_column_names': ['foo_column'],
                }

                # pylint: disable=cell-var-from-loop
                def export_data_to_file(*args, **kwargs):  # pylint: disable=unused-argument
                    with open(f'{temp_directory}/t1', 'w', encoding='utf8') as exported_file:
                        exported_file.write('F' * file_size)

                    return file_parts

                mocked_upload_to_s3.return_value = (['FOO_S3_KEYS'], s3_key_pattern)
                mocked_bookmark.return_value = bookmark
                mocked_export_data = mocked_fastsyncmysql.return_value.export_source_table_data
                mocked_fastsyncmysql.return_value.map_column_types_to_target.return_value = maped_column_types_to_target
                mocked_export_data.side_effect = export_data_to_file

                actual_return = mysql_to_snowflake.partial_sync_table(test_table, args)
                self.assertIs(actual_return, True)

                mocked_fastsyncmysql.assert_called_once_with(
                    args.tap, mysql_to_snowflake.tap_type_to_target_type
                )

                target = {
                    'schema': 'foo_schema',
                    'sf_object': mocked_fastsync_sf(),
                    'table': table_name,
                    'temp': 'foo_temp',
                    'publication_status': {'attempted': True},
                }
                mocked_fastsync_sf.return_value.create_schema.assert_called_with('foo_schema')
                mocked_fastsync_sf.return_value.create_table.assert_called_once_with(
                    'foo_schema', 'foo', ['foo type1', 'bar type2'], 'foo_primary', is_temporary=True)
                mocked_fastsync_sf.return_value.query.assert_not_called()
                mocked_fastsyncmysql.return_value.close_connections.assert_called_once_with(silent=True)
                runtime_args = mocked_load_into_sf.call_args.args[1]
                self.assertIsNot(runtime_args, args)
                self.assertEqual(args.table, 'mysql_source_db.email')
                self.assertEqual(runtime_args.table, table_name)
                mocked_load_into_sf.assert_called_with(
                    target, runtime_args, maped_column_types_to_target['columns'],
                    maped_column_types_to_target['primary_key'],
                    s3_key_pattern, file_size,
                    ' WHERE "FOO_COLUMN" >= \'1\'',
                )
                mocked_fastsync_sf.return_value.s3.delete_object.assert_called_once_with(
                    Bucket=args.target['s3_bucket'], Key='FOO_S3_KEYS'
                )

                if end_value:
                    mocked_save_state.assert_not_called()
                else:
                    mocked_save_state.assert_called_with('state.json', table_name, bookmark)
