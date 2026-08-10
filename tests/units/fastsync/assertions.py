import pytest
import collections
import multiprocessing

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import call, patch, Mock
from argparse import Namespace

from pipelinewise.fastsync.commons import utils as fastsync_utils


FASTSYNC_NS = Namespace(
    **{
        'tap': {'bucket': 'testBucket'},
        'properties': {},
        'target': {},
        'transform': {},
        'temp_dir': '',
        'state': '',
    }
)


SNOWFLAKE_FASTSYNC_NS = Namespace(
    tap={'dbname': 'source_db'},
    properties={'schema': 'source properties'},
    target={
        'archive_load_files': False,
        's3_bucket': 'staging-bucket',
        'split_file_chunk_size_mb': 64,
        'split_file_max_chunks': 4,
        'split_large_files': True,
        'tap_id': 'tap-id',
    },
    transform={'transformations': []},
    temp_dir='/tmp',
    state='/tmp/state.json',
)


def _create_object_names_to_mock(
    package_nm: str, tap_class_nm: str, target_class_nm: str
):
    """Function to generate dynamic object names"""
    ObjectNames = collections.namedtuple(
        'ObjectNames',
        [
            'full_tap_class_nm',
            'full_target_class_nm',
            'sync_table_fn_nm',
            'utils_module_nm',
            'multiproc_module_nm',
            'os_module_nm',
        ],
    )
    return ObjectNames(
        full_tap_class_nm=f'{package_nm}.{tap_class_nm}',
        full_target_class_nm=f'{package_nm}.{target_class_nm}',
        sync_table_fn_nm=f'{package_nm}.sync_table',
        utils_module_nm=f'{package_nm}.utils',
        multiproc_module_nm=f'{package_nm}.multiprocessing',
        os_module_nm=f'{package_nm}.os',
    )


# pylint: disable=missing-function-docstring,unused-variable
def assert_sync_table_returns_true_on_success(
    sync_table: callable, package_nm: str, tap_class_nm: str, target_class_nm: str
) -> None:
    """Tests if fastsync sync table function returns true on success"""
    objects_to_mock = _create_object_names_to_mock(
        package_nm, tap_class_nm, target_class_nm
    )

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
                    with patch(objects_to_mock.os_module_nm):
                        utils_mock.get_target_schema.return_value = 'my-target-schema'
                        tap_mock.return_value.map_column_types_to_target.return_value = {
                            'columns': [
                                'id INTEGER',
                                'is_test SMALLINT',
                                'age INTEGER',
                                'name VARCHAR',
                            ],
                            'primary_key': 'id,name',
                        }

                        target_mock.return_value.upload_to_s3.return_value = 's3_key'
                        utils_mock.return_value.get_bookmark_for_table.return_value = {
                            'modified_since': '2019-11-18'
                        }
                        utils_mock.return_value.get_grantees.return_value = [
                            'role_1',
                            'role_2',
                        ]
                        utils_mock.return_value.get_bookmark_for_table.return_value = (
                            None
                        )

                        multiproc_mock.lock.return_value = LockMock()

                        res = sync_table('table_1', FASTSYNC_NS)

                        assert isinstance(res, bool)
                        assert res


# pylint: disable=missing-function-docstring,unused-variable,invalid-name,no-member
def assert_sync_table_exception_on_failed_copy(
    sync_table: callable,
    package_nm: str,
    tap_class_nm: str,
    target_class_nm: str,
    expected_cleanup=None,
) -> None:
    objects_to_mock = _create_object_names_to_mock(
        package_nm, tap_class_nm, target_class_nm
    )

    with patch(objects_to_mock.full_tap_class_nm) as tap_mock:
        with patch(objects_to_mock.full_target_class_nm) as target_mock:
            with patch(objects_to_mock.utils_module_nm) as utils_mock:
                utils_mock.get_target_schema.return_value = 'my-target-schema'
                utils_mock.gen_export_filename.return_value = 'my-export-file'
                utils_mock.staging_failure_result.return_value = (
                    'table_1: Boooom'
                )
                tap_mock.return_value.copy_table.side_effect = Exception('Boooom')

                assert sync_table('table_1', FASTSYNC_NS) == 'table_1: Boooom'

                utils_mock.get_target_schema.assert_called_once_with(FASTSYNC_NS.target, 'table_1')
                tap_mock.return_value.copy_table.assert_called_once()
                utils_mock.save_state_file.assert_not_called()
                assert target_mock.return_value.method_calls == []
                if expected_cleanup is not None:
                    assert tap_mock.return_value.method_calls[-1] == expected_cleanup


def assert_snowflake_sync_table_native_workflow(
    sync_table: callable,
    package_nm: str,
    tap_class_nm: str,
    source_type: str,
    type_mapper: callable,
    publish_error: Exception = None,
    state_error: Exception = None,
    grant_error: Exception = None,
) -> None:
    """Assert the native Snowflake staging, publication, and state workflow."""
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    # pylint: disable=too-many-locals,too-many-statements,no-member
    objects_to_mock = _create_object_names_to_mock(
        package_nm, tap_class_nm, 'FastSyncTargetSnowflake'
    )
    args = SNOWFLAKE_FASTSYNC_NS
    table = 'source.table'
    export_path = '/tmp/export.csv.gz'
    file_parts = [f'{export_path}.part0', f'{export_path}.part1']
    s3_keys = ['loads/export.csv.gz.part0', 'loads/export.csv.gz.part1']
    columns = ['"ID" NUMBER', '"VALUE" VARCHAR']
    primary_key = ['"ID"']
    bookmark = {'version': 1, 'position': 42}
    timeline = []

    if publish_error and state_error:
        raise AssertionError('State persistence cannot be reached after publication fails')

    finalization_error = grant_error
    if publish_error and grant_error:
        finalization_error = RuntimeError(
            f'{publish_error}; post-publication finalization failed: {grant_error}'
        )

    def record(name, result=None, error=None):
        def side_effect(*_args, **_kwargs):
            timeline.append(name)
            if error:
                raise error
            return result

        return side_effect

    file_sizes = iter([10, 20])

    def getsize(*_args):
        timeline.append('getsize')
        return next(file_sizes)

    with patch(objects_to_mock.full_tap_class_nm) as tap_class_mock, \
            patch(objects_to_mock.full_target_class_nm) as target_class_mock, \
            patch(objects_to_mock.utils_module_nm) as utils_mock, \
            patch(f'{package_nm}.glob.glob', return_value=file_parts) as glob_mock, \
            patch(f'{package_nm}.os.path.exists', return_value=True) as exists_mock, \
            patch(f'{package_nm}.os.path.getsize', side_effect=getsize) as getsize_mock:
        tap = tap_class_mock.return_value
        target = target_class_mock.return_value
        utils_mock.gen_export_filename.return_value = 'export.csv.gz'
        utils_mock.get_target_schema.return_value = 'TARGET_SCHEMA'
        utils_mock.get_bookmark_for_table.side_effect = record('bookmark', bookmark)
        utils_mock.upload_files_to_s3.side_effect = record(
            'upload_all', (s3_keys, 'loads/export.csv.gz')
        )
        utils_mock.apply_snowflake_table_grants.side_effect = record('pregrant')
        utils_mock.finalize_snowflake_fullsync.side_effect = record(
            'finalize', error=finalization_error
        )
        tap.map_column_types_to_target.side_effect = record(
            'source.map',
            {'columns': columns, 'primary_key': primary_key},
        )
        source_open_method = tap.open_connections if source_type == 'mysql' else tap.open_connection
        source_close_method = tap.close_connections if source_type == 'mysql' else tap.close_connection
        source_open_method.side_effect = record('source.open')
        tap.copy_table.side_effect = record('source.copy')
        source_close_method.side_effect = record('source.close')

        target.copy_to_table.side_effect = record('target.copy')
        target.obfuscate_columns.side_effect = record('obfuscate')

        def publish(*_args, **_kwargs):
            timeline.append('publish')
            if publish_error:
                raise publish_error

        target.swap_tables.side_effect = publish
        utils_mock.save_state_file.side_effect = record('state', error=state_error)

        def staging_failure(
            _target,
            _s3_keys,
            _bucket,
            _target_schema,
            failed_table,
            _temp_created,
            operation_error,
        ):
            timeline.append('rollback')
            return f'{failed_table}: {operation_error}'

        utils_mock.staging_failure_result.side_effect = staging_failure

        result = sync_table(table, args)

    expected_error = finalization_error or publish_error or state_error
    expected_result = f'{table}: {expected_error}' if expected_error else True
    assert result == expected_result
    tap_class_mock.assert_called_once_with(args.tap, type_mapper)
    target_class_mock.assert_called_once_with(args.target, args.transform)
    exists_mock.assert_called_once_with(export_path)
    glob_mock.assert_called_once_with(f'{export_path}*')
    assert getsize_mock.call_args_list == [call(file_parts[0]), call(file_parts[1])]

    expected_copy_call = call.copy_table(
        table,
        export_path,
        split_large_files=True,
        split_file_chunk_size_mb=64,
        split_file_max_chunks=4,
    )
    if source_type == 'mysql':
        assert tap.method_calls == [
            call.open_connections(),
            expected_copy_call,
            call.map_column_types_to_target(table),
            call.close_connections(),
            call.close_connections(silent=True),
        ]
        source_timeline = [
            'source.open', 'bookmark', 'source.copy', 'source.map', 'source.close',
            'getsize', 'getsize',
        ]
        expected_bookmark_call = call.get_bookmark_for_table(table, args.properties, tap)
    elif source_type == 'postgres':
        assert tap.method_calls == [
            call.open_connection(),
            expected_copy_call,
            call.map_column_types_to_target(table),
            call.close_connection(),
            call.close_connection(silent=True),
        ]
        source_timeline = [
            'source.open', 'bookmark', 'source.copy', 'getsize', 'getsize',
            'source.map', 'source.close',
        ]
        expected_bookmark_call = call.get_bookmark_for_table(
            table, args.properties, tap, dbname='source_db'
        )
    else:
        raise AssertionError(f'Unsupported source type: {source_type}')

    expected_target_calls = [
        call.create_schema('TARGET_SCHEMA'),
        call.create_table('TARGET_SCHEMA', table, columns, primary_key, is_temporary=True),
        call.copy_to_table('loads/export.csv.gz', 'TARGET_SCHEMA', table, 30, is_temporary=True),
        call.obfuscate_columns('TARGET_SCHEMA', table),
        call.create_table(
            'TARGET_SCHEMA',
            table,
            columns,
            primary_key,
            allow_replace_table=False,
            normalize_primary_keys=False,
        ),
        call.swap_tables('TARGET_SCHEMA', table, cleanup_old_table=False),
    ]
    assert target.method_calls == expected_target_calls

    expected_utils_calls = [
        call.gen_export_filename(tap_id='tap-id', table=table),
        call.get_target_schema(args.target, table),
        expected_bookmark_call,
        call.upload_files_to_s3(target, file_parts, args.temp_dir, 'staging-bucket'),
        call.apply_snowflake_table_grants(
            target,
            args.target,
            'TARGET_SCHEMA',
            table,
            is_temporary=True,
        ),
    ]
    expected_utils_calls.append(call.finalize_snowflake_fullsync(
        target,
        s3_keys,
        'staging-bucket',
        args.target,
        'TARGET_SCHEMA',
        table,
        publication_error=publish_error,
    ))
    expected_timeline = source_timeline + [
        'upload_all', 'target.copy', 'obfuscate', 'pregrant', 'publish', 'finalize',
    ]

    if publish_error or grant_error:
        expected_utils_calls.append(call.staging_failure_result(
            target,
            s3_keys if grant_error else [],
            'staging-bucket',
            'TARGET_SCHEMA',
            table,
            bool(grant_error),
            finalization_error or publish_error,
        ))
        expected_timeline.extend(['rollback', 'source.close'])
    else:
        expected_utils_calls.append(
            call.save_state_file(args.state, table, bookmark)
        )
        expected_timeline.append('state')
        if state_error:
            expected_utils_calls.append(call.staging_failure_result(
                target,
                [],
                'staging-bucket',
                'TARGET_SCHEMA',
                table,
                False,
                state_error,
            ))
            expected_timeline.append('rollback')
        expected_timeline.append('source.close')

    assert timeline == expected_timeline
    assert utils_mock.method_calls == expected_utils_calls


def assert_snowflake_sync_table_rolls_back_later_upload_failure(
    sync_table: callable,
    package_nm: str,
    tap_class_nm: str,
    source_type: str,
    rollback_cleanup_error: Exception = None,
) -> None:
    """A failed later part upload must retain local files and remove earlier S3 parts."""
    # pylint: disable=too-many-locals
    objects_to_mock = _create_object_names_to_mock(
        package_nm, tap_class_nm, 'FastSyncTargetSnowflake'
    )
    table = 'source.table'

    with TemporaryDirectory() as temp_directory:
        args = Namespace(**{
            **vars(SNOWFLAKE_FASTSYNC_NS),
            'temp_dir': temp_directory,
        })
        export_path = Path(temp_directory, 'export.csv.gz')
        file_parts = [f'{export_path}.part0', f'{export_path}.part1']
        for file_part in file_parts:
            Path(file_part).write_text('data', encoding='utf8')

        with patch(objects_to_mock.full_tap_class_nm) as tap_class_mock, \
                patch(objects_to_mock.full_target_class_nm) as target_class_mock, \
                patch.object(fastsync_utils, 'gen_export_filename', return_value=export_path.name), \
                patch.object(fastsync_utils, 'get_target_schema', return_value='TARGET_SCHEMA'), \
                patch.object(fastsync_utils, 'get_bookmark_for_table', return_value={'position': 42}), \
                patch.object(fastsync_utils, 'save_state_file') as save_state_file_mock, \
                patch.object(fastsync_utils, 'get_grantees') as get_grantees_mock:
            tap = tap_class_mock.return_value
            tap.map_column_types_to_target.return_value = {
                'columns': ['"ID" NUMBER'],
                'primary_key': ['"ID"'],
            }
            target = target_class_mock.return_value
            target.upload_to_s3.side_effect = [
                'loads/export.csv.gz.part0',
                RuntimeError('second upload failed'),
            ]
            target.s3.delete_object.side_effect = rollback_cleanup_error

            result = sync_table(table, args)

        if rollback_cleanup_error:
            assert result.startswith(f'{table}: second upload failed')
            assert 'staging upload rollback failed' in result
            assert 'staging cleanup failed' in result
        else:
            assert result == f'{table}: second upload failed'
        upload_calls = target.upload_to_s3.call_args_list
        assert len(upload_calls) == 2
        assert {upload_call.args[0] for upload_call in upload_calls} == set(file_parts)
        assert all(
            upload_call.kwargs == {'tmp_dir': temp_directory}
            for upload_call in upload_calls
        )
        assert target.s3.delete_object.call_count == (
            6 if rollback_cleanup_error else 1
        )
        assert all(
            delete_call == call(
                Bucket='staging-bucket', Key='loads/export.csv.gz.part0'
            )
            for delete_call in target.s3.delete_object.call_args_list
        )
        target.create_schema.assert_not_called()
        target.copy_to_table.assert_not_called()
        target.swap_tables.assert_not_called()
        assert all(Path(file_part).exists() for file_part in file_parts)
        save_state_file_mock.assert_not_called()
        get_grantees_mock.assert_not_called()

        if source_type == 'mysql':
            assert tap.method_calls[-2:] == [
                call.close_connections(),
                call.close_connections(silent=True),
            ]
        elif source_type == 'postgres':
            assert tap.method_calls[-2:] == [
                call.close_connection(),
                call.close_connection(silent=True),
            ]
        else:
            raise AssertionError(f'Unsupported source type: {source_type}')


# pylint: disable=missing-function-docstring,unused-variable,invalid-name
def assert_main_impl_exit_normally_on_success(
    main_impl: callable, package_nm: str, tap_class_nm: str, target_class_nm: str
) -> None:
    objects_to_mock = _create_object_names_to_mock(
        package_nm, tap_class_nm, target_class_nm
    )

    with patch(objects_to_mock.utils_module_nm) as utils_mock:
        with patch(objects_to_mock.full_target_class_nm):
            with patch(objects_to_mock.sync_table_fn_nm):
                with patch(objects_to_mock.multiproc_module_nm) as multiproc_mock:
                    with patch(objects_to_mock.full_tap_class_nm) as tap_mock:
                        tap_mock.return_value.drop_slot.side_effect = None

                        ns = Namespace(
                            **{
                                'tables': ['table_1', 'table_2', 'table_3', 'table_4'],
                                'target': 'sf',
                                'transform': None,
                                'drop_pg_slot': False,
                                'tap': {},
                                'autoresync_size': None
                            }
                        )

                        utils_mock.parse_args.return_value = ns
                        utils_mock.get_pool_size.return_value = 10

                        mock_enter = Mock()
                        mock_enter.return_value.map.return_value = [
                            True,
                            True,
                            True,
                            True,
                        ]

                        pool_mock = Mock(spec_set=multiprocessing.Pool).return_value

                        # to mock variable p in with statement, we need __enter__ and __exist__
                        pool_mock.__enter__ = mock_enter
                        pool_mock.__exit__ = Mock()
                        multiproc_mock.Pool.return_value = pool_mock

                        # call function
                        main_impl()

                        # assertions
                        utils_mock.get_pool_size.assert_called_once_with({})
                        multiproc_mock.Pool.assert_called_once_with(10)
                        assert utils_mock.parse_args.call_count == 1
                        assert mock_enter.return_value.map.call_count == 1
                        assert tap_mock.return_value.drop_slot.call_count == 0


# pylint: disable=missing-function-docstring,unused-variable,invalid-name
def assert_main_impl_should_exit_with_error_on_failure(
    main_impl: callable, package_nm: str, tap_class_nm: str, target_class_nm: str
) -> None:
    objects_to_mock = _create_object_names_to_mock(
        package_nm, tap_class_nm, target_class_nm
    )

    with patch(objects_to_mock.utils_module_nm) as utils_mock:
        with patch(objects_to_mock.full_target_class_nm):
            with patch(objects_to_mock.sync_table_fn_nm):
                with patch(objects_to_mock.multiproc_module_nm) as multiproc_mock:
                    with patch(objects_to_mock.full_tap_class_nm) as tap_mock:
                        tap_mock.return_value.drop_slot.side_effect = None

                        ns = Namespace(
                            **{
                                'tables': ['table_1', 'table_2', 'table_3', 'table_4'],
                                'target': 'sf',
                                'transform': None,
                                'drop_pg_slot': True,
                                'tap': {
                                    'fastsync_parallelism': 4,
                                },
                                'autoresync_size': None
                            }
                        )

                        utils_mock.parse_args.return_value = ns
                        utils_mock.get_pool_size.return_value = 10

                        mock_enter = Mock()
                        mock_enter.return_value.map.return_value = [
                            True,
                            True,
                            'Critical: random error',
                            True,
                        ]

                        pool_mock = Mock(spec_set=multiprocessing.Pool).return_value

                        # to mock variable p in with statement, we need __enter__ and __exist__
                        pool_mock.__enter__ = mock_enter
                        pool_mock.__exit__ = Mock()
                        multiproc_mock.Pool.return_value = pool_mock

                        with pytest.raises(SystemExit):
                            main_impl()

                            # assertions
                            assert utils_mock.parse_args.call_count == 1
                            assert mock_enter.return_value.map.call_count == 1
                            assert tap_mock.return_value.drop_slot.call_count == 1
                            utils_mock.get_pool_size.assert_called_once_with(
                                {
                                    'fastsync_parallelism': 4,
                                }
                            )
                            multiproc_mock.Pool.assert_called_once_with(10)
