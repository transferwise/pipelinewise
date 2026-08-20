import pytest
import collections
import multiprocessing

from types import SimpleNamespace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import call, patch, Mock
from argparse import Namespace

from pipelinewise.fastsync.commons import utils as fastsync_utils
from pipelinewise.fastsync.commons import snowflake_iceberg_routes as iceberg_routes


FULLSYNC_DRIVER = 'pipelinewise.fastsync.commons.rdbms_to_snowflake'


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

    with patch(objects_to_mock.full_tap_class_nm) as tap_mock, \
            patch(objects_to_mock.full_target_class_nm) as target_mock, \
            patch(objects_to_mock.utils_module_nm) as utils_mock, \
            patch(objects_to_mock.multiproc_module_nm) as multiproc_mock, \
            patch(objects_to_mock.os_module_nm):
        utils_mock.get_target_schema.return_value = 'my-target-schema'
        tap_mock.return_value.map_column_types_to_target.return_value = {
            'columns': ['id INTEGER', 'is_test SMALLINT', 'age INTEGER', 'name VARCHAR'],
            'primary_key': 'id,name',
        }
        target_mock.return_value.upload_to_s3.return_value = 's3_key'
        utils_mock.return_value.get_bookmark_for_table.return_value = None
        utils_mock.return_value.get_grantees.return_value = ['role_1', 'role_2']
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

    with patch(objects_to_mock.full_tap_class_nm) as tap_mock, \
            patch(objects_to_mock.full_target_class_nm) as target_mock, \
            patch(objects_to_mock.utils_module_nm) as utils_mock:
        utils_mock.get_target_schema.return_value = 'my-target-schema'
        utils_mock.gen_export_filename.return_value = 'my-export-file'
        utils_mock.staging_failure_result.return_value = 'table_1: Boooom'
        tap_mock.return_value.copy_table.side_effect = Exception('Boooom')

        assert sync_table('table_1', FASTSYNC_NS) == 'table_1: Boooom'

        utils_mock.get_target_schema.assert_called_once_with(
            FASTSYNC_NS.target, 'table_1'
        )
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
            patch(
                f'{package_nm}.iceberg_routes.require_native_target_format',
                side_effect=record('format.guard'),
            ) as native_format_guard, \
            patch(f'{FULLSYNC_DRIVER}.glob.glob', return_value=file_parts) as glob_mock, \
            patch(f'{FULLSYNC_DRIVER}.os.path.exists', return_value=True) as exists_mock, \
            patch(f'{FULLSYNC_DRIVER}.os.path.getsize', side_effect=getsize) as getsize_mock:
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
    assert native_format_guard.call_args_list == [
        call(target, args, 'TARGET_SCHEMA', table, allow_missing=True),
        call(target, args, 'TARGET_SCHEMA', table, allow_missing=False),
    ]
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
    expected_timeline = ['format.guard'] + source_timeline + [
        'upload_all', 'target.copy', 'obfuscate', 'format.guard',
        'pregrant', 'publish', 'finalize',
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


def assert_snowflake_sync_table_iceberg_workflow(
    sync_table: callable,
    package_nm: str,
    tap_class_nm: str,
    source_type: str,
    type_mapper: callable,
    publish_error: Exception = None,
    recovery_action: str = None,
    primary_key=None,
    upload_cleanup_debt=False,
    recovery_source_error=None,
    source_open_error=None,
    recovery_error=None,
) -> None:
    """Assert an Iceberg route publishes or recovers before state advances."""
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    # pylint: disable=too-many-locals,too-many-statements,too-many-branches
    objects_to_mock = _create_object_names_to_mock(
        package_nm, tap_class_nm, 'FastSyncTargetSnowflake'
    )
    args = Namespace(
        **{
            **vars(SNOWFLAKE_FASTSYNC_NS),
            'target': {
                **SNOWFLAKE_FASTSYNC_NS.target,
                'dbname': 'TARGET_DB',
                'default_target_schema': 'TARGET_SCHEMA',
                'hard_delete': True,
                'data_flattening_max_level': 0,
                'target_table_format': 'iceberg',
                'iceberg_version': 3,
            },
        }
    )
    table = 'source.table'
    export_path = '/tmp/export.csv.gz'
    file_parts = [f'{export_path}.part0']
    s3_keys = ['loads/export.csv.gz.part0']
    upload_error = (
        fastsync_utils.StagingUploadError(
            RuntimeError('upload failed'),
            RuntimeError('cleanup failed'),
            s3_keys,
        )
        if upload_cleanup_debt
        else None
    )
    columns = ['"ID" NUMBER', '"VALUE" VARIANT']
    bookmark = {'version': 1, 'position': 42}
    timeline = []
    current_spec = Mock(name='current-iceberg-spec')
    persisted_spec = Mock(name='persisted-iceberg-spec')
    if recovery_action is not None and recovery_error is not None:
        raise AssertionError('A recovery cannot both return an action and fail')
    recovering = recovery_action is not None or recovery_error is not None
    publication_spec = persisted_spec if recovering else current_spec
    restarting = recovery_action == iceberg_routes.RECOVERY_RESTART_STAGING
    recovery_target = Mock(name='recovery-target')
    staging_config = iceberg_routes.staging_config_identity(args.target)
    recovery_identity = iceberg_routes.fastsync_recovery_identity(
        args,
        table,
        source_route=f'{source_type}_to_snowflake',
        source_engine=source_type,
        staging_config=staging_config,
        iceberg_version=3,
    )
    attempt = SimpleNamespace(
        staging_table='PW_STAGE_123',
        source_bookmark=bookmark,
        s3_keys=s3_keys,
        context={},
        table_spec=persisted_spec,
    )

    def record(name, result=None, error=None):
        def side_effect(*_args, **_kwargs):
            timeline.append(name)
            if error:
                raise error
            return result

        return side_effect

    def complete_state_handoff(publisher, publication_attempt, state_writer):
        timeline.append('state_handoff')
        state_writer(publication_attempt.source_bookmark)
        publisher.complete_state_handoff(publication_attempt)

    class TrackingLock:
        """Record the lifetime of the per-table publication lock."""

        def __enter__(self):
            timeline.append('lock.enter')

        def __exit__(self, *_args):
            timeline.append('lock.exit')

    with patch(objects_to_mock.full_tap_class_nm) as tap_class_mock, \
            patch(objects_to_mock.full_target_class_nm) as target_class_mock, \
            patch(objects_to_mock.utils_module_nm) as utils_mock, \
            patch(f'{FULLSYNC_DRIVER}.glob.glob', return_value=file_parts) as glob_mock, \
            patch(f'{FULLSYNC_DRIVER}.os.path.exists', return_value=True) as exists_mock, \
            patch(f'{FULLSYNC_DRIVER}.os.path.getsize', return_value=30) as getsize_mock, \
            patch.object(iceberg_routes, 'create_publisher') as create_publisher_mock, \
            patch.object(
                iceberg_routes, 'target_name', return_value=recovery_target
            ) as target_name_mock, \
            patch.object(
                iceberg_routes, 'create_spec', return_value=current_spec
            ) as create_spec_mock, \
            patch.object(
                iceberg_routes,
                'validate_recovery_source_spec',
                side_effect=recovery_source_error,
            ) as validate_recovery_source_spec_mock, \
            patch.object(
                iceberg_routes,
                'plan_staging_uploads',
                side_effect=record('plan_uploads', s3_keys),
            ) as plan_staging_uploads_mock, \
            patch.object(
                iceberg_routes, 'restart_staging', side_effect=record('restart')
            ) as restart_mock, \
            patch.object(
                iceberg_routes,
                'finalize_attempt',
                autospec=True,
                side_effect=record('finalize'),
            ) \
            as finalize_mock, \
            patch.object(
                iceberg_routes,
                'complete_state_handoff',
                side_effect=complete_state_handoff,
            ) as handoff_mock, \
            patch.object(
                iceberg_routes,
                'validate_route_config',
                side_effect=record('validate', 3),
            ) as validate_mock:
        tap = tap_class_mock.return_value
        target = target_class_mock.return_value
        publisher = create_publisher_mock.return_value

        utils_mock.gen_export_filename.return_value = 'export.csv.gz'
        utils_mock.get_target_schema.return_value = 'TARGET_SCHEMA'
        utils_mock.get_bookmark_for_table.side_effect = record('bookmark', bookmark)
        utils_mock.upload_files_to_s3.side_effect = record(
            'upload_all',
            (s3_keys, 'loads/export.csv.gz'),
            error=upload_error,
        )
        utils_mock.save_state_file.side_effect = record('state')
        tap.map_column_types_to_target.side_effect = record(
            'source.map',
            {'columns': columns, 'primary_key': primary_key},
        )
        source_open_method = (
            tap.open_connections if source_type == 'mysql' else tap.open_connection
        )
        source_close_method = (
            tap.close_connections if source_type == 'mysql' else tap.close_connection
        )
        source_open_method.side_effect = record(
            'source.open', error=source_open_error
        )
        tap.copy_table.side_effect = record('source.copy')
        source_close_method.side_effect = record('source.close')
        target.create_schema.side_effect = record('target.create_schema')
        target.copy_to_table.side_effect = record('target.copy', 1)
        target.obfuscate_columns.side_effect = record('obfuscate')
        publisher.prepare_full_sync.side_effect = record('prepare', attempt)
        publisher.plan_full_sync.side_effect = record('plan')
        publisher.record_uploaded.side_effect = record('record_uploaded')
        publisher.record_staging_created.side_effect = record(
            'record_staging_created'
        )
        publisher.staging_evidence.side_effect = record(
            'staging_evidence', (1, 'staged-fingerprint')
        )
        publisher.record_staged.side_effect = record('record_staged')
        publisher.publish_full_sync.side_effect = record(
            'publish', error=publish_error
        )
        publisher.table_lock.return_value = TrackingLock()

        if recovering:
            publisher.load_attempt.side_effect = record('load_attempt', attempt)
            publisher.reconcile.side_effect = record(
                'reconcile',
                SimpleNamespace(action=recovery_action),
                error=recovery_error,
            )
        else:
            publisher.load_attempt.side_effect = record('load_attempt')

        result = sync_table(table, args)

    expected_error = (
        recovery_error
        or recovery_source_error
        or upload_error
        or publish_error
    )
    assert result == (f'{table}: {expected_error}' if expected_error else True)
    tap_class_mock.assert_called_once_with(args.tap, type_mapper)
    target_class_mock.assert_called_once_with(args.target, args.transform)
    target_name_mock.assert_called_once_with(args, 'TARGET_SCHEMA', table)
    if recovering and not restarting:
        create_spec_mock.assert_not_called()
    else:
        assert create_spec_mock.call_args_list[0] == call(
            args, 'TARGET_SCHEMA', table, columns, primary_key
        )
    create_publisher_mock.assert_called_once_with(target, args)
    validate_mock.assert_called_once_with(args.target)
    if source_type == 'mysql':
        tap.set_mariadb_json_aliases_enabled.assert_called_once_with(True)
    publisher.table_lock.assert_called_once_with(recovery_target, recovery_identity)
    publisher.load_attempt.assert_called_once_with(
        recovery_target,
        expected_kind='full',
        recovery_identity=recovery_identity,
        staging_config=staging_config,
    )
    assert timeline.index('lock.enter') < timeline.index('load_attempt')
    assert timeline.index('load_attempt') < timeline.index('target.create_schema')
    if recovering and not restarting:
        source_open_method.assert_not_called()
    else:
        assert timeline.index('validate') < timeline.index('source.open')
        assert timeline.index('load_attempt') < timeline.index('source.open')
    target.create_schema.assert_called_once_with('TARGET_SCHEMA')
    assert timeline[-1] == 'lock.exit'

    if recovering and not restarting:
        validate_recovery_source_spec_mock.assert_not_called()
    elif restarting:
        expected_calls = [call(persisted_spec, current_spec)]
        if not recovery_source_error:
            expected_calls.append(call(persisted_spec, current_spec))
        assert validate_recovery_source_spec_mock.call_args_list == expected_calls
    else:
        validate_recovery_source_spec_mock.assert_called_once_with(
            current_spec, current_spec
        )

    if upload_cleanup_debt:
        plan_staging_uploads_mock.assert_called_once_with(
            publisher, attempt, target, file_parts
        )
        utils_mock.upload_files_to_s3.assert_called_once_with(
            target,
            file_parts,
            args.temp_dir,
            'staging-bucket',
            planned_s3_keys=s3_keys,
        )
        assert timeline.index('plan_uploads') < timeline.index('upload_all')
        publisher.record_uploaded.assert_called_once_with(attempt, s3_keys)
        target.create_table.assert_not_called()
        target.copy_to_table.assert_not_called()
        publisher.record_staging_created.assert_not_called()
        publisher.record_staged.assert_not_called()
        publisher.publish_full_sync.assert_not_called()
        finalize_mock.assert_not_called()
        handoff_mock.assert_not_called()
        utils_mock.save_state_file.assert_not_called()
        return

    if recovering and not restarting:
        plan_staging_uploads_mock.assert_not_called()
        utils_mock.get_bookmark_for_table.assert_not_called()
        tap.copy_table.assert_not_called()
        target.create_table.assert_not_called()
        target.copy_to_table.assert_not_called()
        publisher.prepare_full_sync.assert_not_called()
        publisher.plan_full_sync.assert_not_called()
        publisher.reconcile.assert_called_once_with(attempt, persisted_spec)
        if recovery_action == iceberg_routes.RECOVERY_PUBLISH:
            publisher.publish_full_sync.assert_called_once_with(
                attempt, persisted_spec
            )
        else:
            publisher.publish_full_sync.assert_not_called()
        source_open_method.assert_not_called()
        tap.map_column_types_to_target.assert_not_called()
        assert timeline.index('load_attempt') < timeline.index('reconcile')
    else:
        if restarting:
            publisher.prepare_full_sync.assert_not_called()
            restart_mock.assert_called_once()
            if recovery_source_error:
                tap.copy_table.assert_not_called()
                target.create_table.assert_not_called()
                target.copy_to_table.assert_not_called()
                publisher.plan_full_sync.assert_not_called()
                publisher.publish_full_sync.assert_not_called()
                finalize_mock.assert_not_called()
                handoff_mock.assert_not_called()
                utils_mock.save_state_file.assert_not_called()
                return
        else:
            publisher.prepare_full_sync.assert_called_once_with(
                current_spec,
                bookmark,
                recovery_identity=recovery_identity,
                staging_config=staging_config,
            )
        exists_mock.assert_called_once_with(export_path)
        glob_mock.assert_called_once_with(f'{export_path}*')
        getsize_mock.assert_called_once_with(file_parts[0])
        publisher.plan_full_sync.assert_called_once_with(
            attempt, publication_spec
        )
        assert create_spec_mock.call_args_list == [
            call(args, 'TARGET_SCHEMA', table, columns, primary_key),
            call(args, 'TARGET_SCHEMA', table, columns, primary_key),
        ]
        target.create_table.assert_called_once_with(
            'TARGET_SCHEMA',
            table,
            columns,
            primary_key,
            is_temporary=True,
            staging_table_name='PW_STAGE_123',
        )
        target.copy_to_table.assert_called_once_with(
            'loads/export.csv.gz',
            'TARGET_SCHEMA',
            table,
            30,
            is_temporary=True,
            staging_table_name='PW_STAGE_123',
        )
        target.obfuscate_columns.assert_called_once_with(
            'TARGET_SCHEMA', table, staging_table_name='PW_STAGE_123'
        )
        publisher.record_uploaded.assert_called_once_with(attempt, s3_keys)
        plan_staging_uploads_mock.assert_called_once_with(
            publisher, attempt, target, file_parts
        )
        utils_mock.upload_files_to_s3.assert_called_once_with(
            target,
            file_parts,
            args.temp_dir,
            'staging-bucket',
            planned_s3_keys=s3_keys,
        )
        publisher.record_staging_created.assert_called_once_with(attempt)
        publisher.staging_evidence.assert_called_once_with(
            attempt, publication_spec, 1
        )
        publisher.record_staged.assert_called_once_with(
            attempt,
            row_count=1,
            row_fingerprint='staged-fingerprint',
        )
        publisher.publish_full_sync.assert_called_once_with(
            attempt, publication_spec
        )
        if restarting:
            utils_mock.get_bookmark_for_table.assert_not_called()
            assert timeline.index('restart') < timeline.index('source.copy')
            assert timeline.index('restart') < timeline.index('source.open')
        else:
            assert timeline.index('load_attempt') < timeline.index('source.open')
            assert timeline.index('source.map') < timeline.index('bookmark')
            assert timeline.index('plan') < timeline.index('source.copy')
        assert timeline.index('record_uploaded') < timeline.index(
            'record_staging_created'
        )
        assert timeline.index('plan_uploads') < timeline.index('upload_all')
        assert timeline.index('obfuscate') < timeline.index('staging_evidence')
        assert timeline.index('record_staged') < timeline.index('publish')

    target.swap_tables.assert_not_called()
    if publish_error or recovery_error:
        finalize_mock.assert_not_called()
        handoff_mock.assert_not_called()
        utils_mock.save_state_file.assert_not_called()
        publisher.complete_state_handoff.assert_not_called()
    else:
        assert finalize_mock.call_count == int(
            recovery_action != iceberg_routes.RECOVERY_STATE_HANDOFF
        )
        if recovery_action != iceberg_routes.RECOVERY_STATE_HANDOFF:
            finalize_mock.assert_called_once_with(
                publisher,
                target,
                args.target,
                'TARGET_SCHEMA',
                table,
                attempt,
                'Successful Iceberg FullSync staging cleanup',
            )
        handoff_mock.assert_called_once()
        utils_mock.save_state_file.assert_called_once_with(
            args.state, table, bookmark
        )
        publisher.complete_state_handoff.assert_called_once_with(attempt)
        if recovery_action != iceberg_routes.RECOVERY_STATE_HANDOFF:
            assert timeline.index('finalize') < timeline.index('state')


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
                patch(
                    f'{package_nm}.iceberg_routes.require_native_target_format'
                ) as native_format_guard, \
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

        native_format_guard.assert_called_once_with(
            target, args, 'TARGET_SCHEMA', table, allow_missing=True
        )

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
                                'target': {},
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
                                'target': {},
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
