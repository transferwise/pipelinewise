import json
import os

from types import SimpleNamespace
from unittest import mock

from pipelinewise.fastsync.commons import snowflake_iceberg_routes as iceberg_routes
from pipelinewise.fastsync.commons import utils as common_utils
from pipelinewise.fastsync.commons.snowflake_iceberg import PartialSyncBoundary


# pylint: disable=too-many-instance-attributes, too-few-public-methods
class PartialSync2SFArgs:
    """Arguments for using in mysql to snowflake tests"""
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-positional-arguments
    # pylint: disable=too-many-locals
    def __init__(self, temp_test_dir, table='email',
                 start_value='FOO_START', end_value='FOO_END', state='state.json',
                 hard_delete=None, drop_target_table=False,
                 target_table_format=None, iceberg_version=None,
                 data_flattening_max_level=0):
        resources_dir = f'{os.path.dirname(__file__)}/resources'
        config_dir = f'{resources_dir}/test_partial_sync'
        tap_config = self._load_json_config(f'{config_dir}/target_snowflake/tap_mysql/config.json')
        target_config = self._load_json_config(f'{config_dir}/tmp/target_config_tmp.json')
        transform_config = self._load_json_config(f'{config_dir}/target_snowflake/tap_mysql/transformation.json')
        properties_config = self._load_json_config(f'{config_dir}/target_snowflake/tap_mysql/properties.json')
        if hard_delete is not None:
            target_config['hard_delete'] = hard_delete
        if target_table_format is not None:
            target_config['target_table_format'] = target_table_format
        if iceberg_version is not None:
            target_config['iceberg_version'] = iceberg_version
        if target_table_format is not None:
            target_config['data_flattening_max_level'] = data_flattening_max_level

        self.table = f'{tap_config["dbname"]}.{table}'
        self.column = 'FOO_COLUMN'
        self.start_value = start_value
        self.end_value = end_value
        self.tap = tap_config
        self.target = target_config
        self.transform = transform_config
        self.temp_dir = temp_test_dir
        self.properties = properties_config
        self.state = state
        self.drop_target_table = drop_target_table

    @staticmethod
    def _load_json_config(file_name):
        with open(file_name, 'r', encoding='utf8') as config_file:
            return json.load(config_file)


def get_argv_list(arguments_dict):
    """Get list of argv"""
    argv_list = ['main']
    if arguments_dict.get('tap'):
        argv_list.extend(['--tap', arguments_dict['tap']])
    if arguments_dict.get('target'):
        argv_list.extend(['--target', arguments_dict['target']])
    if arguments_dict.get('table'):
        argv_list.extend(['--table', arguments_dict['table']])
    if arguments_dict.get('column'):
        argv_list.extend(['--column', arguments_dict['column']])
    if arguments_dict.get('start_value'):
        argv_list.extend(['--start_value', arguments_dict['start_value']])
    if arguments_dict.get('end_value'):
        argv_list.extend(['--end_value', arguments_dict['end_value']])
    if arguments_dict.get('temp_dir'):
        argv_list.extend(['--temp_dir', arguments_dict['temp_dir']])
    if arguments_dict.get('state'):
        argv_list.extend(['--state', arguments_dict['state']])

    return argv_list


def assert_iceberg_partial_sync_workflow(
    route_module,
    source_class_name,
    *,
    publish_error=None,
    recovery_action=None,
    empty_export=False,
    drop_target=False,
    upload_cleanup_debt=False,
    missing_primary_key=False,
    recovery_source_error=None,
    source_open_error=None,
    recovery_error=None,
):
    """Assert a source route uses durable Iceberg PartialSync publication."""
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals,too-many-statements
    # pylint: disable=too-many-branches
    package_name = route_module.__name__
    args = PartialSync2SFArgs(
        temp_test_dir='/tmp',
        end_value=None,
        drop_target_table=drop_target,
        target_table_format='iceberg',
        iceberg_version=3,
    )
    original_table = args.table
    table = ('foo', {
        'column': 'foo_column',
        'start_value': '<S>1',
        'end_value': None,
        'drop_target_table': drop_target,
    })
    bookmark = {'position': 42}
    columns = ['"ID" NUMBER', '"VALUE" VARIANT']
    primary_keys = None if missing_primary_key else ['"ID"']
    s3_keys = [] if empty_export else ['loads/part.csv.gz']
    s3_pattern = 'NO_FILES_TO_LOAD' if empty_export else 'loads/part.csv.gz'
    file_parts = [] if empty_export else ['/tmp/part.csv.gz']
    upload_error = (
        common_utils.StagingUploadError(
            RuntimeError('upload failed'),
            RuntimeError('cleanup failed'),
            s3_keys,
        )
        if upload_cleanup_debt
        else None
    )
    attempt = SimpleNamespace(
        staging_table='PW_STAGE_123',
        source_bookmark=bookmark,
        s3_keys=s3_keys,
        manifest_payload=SimpleNamespace(
            column_name='foo_column',
            start_value='1',
            end_value=None,
            end_is_unbounded=True,
            drop_target=drop_target,
        ),
        table_spec=None,
    )
    current_spec = mock.Mock(name='current-iceberg-spec')
    persisted_spec = mock.Mock(name='persisted-iceberg-spec')
    if recovery_action is not None and recovery_error is not None:
        raise AssertionError('A recovery cannot both return an action and fail')
    recovering = recovery_action is not None or recovery_error is not None
    publication_spec = persisted_spec if recovering else current_spec
    restarting = recovery_action == iceberg_routes.RECOVERY_RESTART_STAGING
    recovery_target = mock.Mock(name='recovery-target')
    staging_config = iceberg_routes.staging_config_identity(args.target)
    source_engine = (
        args.tap.get('engine', 'mysql')
        if source_class_name == 'FastSyncTapMySql'
        else 'postgres'
    )
    recovery_identity = iceberg_routes.fastsync_recovery_identity(
        args,
        table[0],
        source_route=(
            'mysql_to_snowflake'
            if source_class_name == 'FastSyncTapMySql'
            else 'postgres_to_snowflake'
        ),
        source_engine=source_engine,
        staging_config=staging_config,
        iceberg_version=3,
        partial_boundary={
            'column_name': table[1]['column'],
            'start_value': table[1]['start_value'],
            'end_value': table[1]['end_value'],
            'drop_target': table[1]['drop_target_table'],
        },
    )
    attempt.table_spec = persisted_spec
    timeline = []

    def record(name, result=None, error=None):
        def side_effect(*_args, **_kwargs):
            timeline.append(name)
            if error:
                raise error
            return result

        return side_effect

    def complete_state_handoff(
        publisher, publication_attempt, state_path, state_table
    ):
        timeline.append('state_handoff')
        if publication_attempt.manifest_payload.end_is_unbounded:
            common_utils.save_state_file(
                state_path, state_table, publication_attempt.source_bookmark
            )
        publisher.complete_state_handoff(publication_attempt)

    class TrackingLock:
        """Record the lifetime of the per-table publication lock."""

        def __enter__(self):
            timeline.append('lock.enter')

        def __exit__(self, *_args):
            timeline.append('lock.exit')

    with mock.patch(f'{package_name}.{source_class_name}') as source_class_mock, \
            mock.patch(f'{package_name}.FastSyncTargetSnowflake') as target_class_mock, \
            mock.patch.object(common_utils, 'get_bookmark_for_table') as bookmark_mock, \
            mock.patch.object(common_utils, 'save_state_file') as save_state_mock, \
            mock.patch.object(
                route_module.utils,
                'upload_to_s3',
                return_value=(s3_keys, s3_pattern),
                side_effect=upload_error,
            ) as upload_mock, \
            mock.patch.object(
                route_module.utils, 'load_into_snowflake'
            ) as native_load_mock, \
            mock.patch.object(
                iceberg_routes, 'create_publisher'
            ) as create_publisher_mock, \
            mock.patch.object(
                iceberg_routes, 'target_name', return_value=recovery_target
            ) as target_name_mock, \
            mock.patch.object(
                iceberg_routes, 'create_spec', return_value=current_spec
            ) as create_spec_mock, \
            mock.patch.object(
                iceberg_routes,
                'validate_recovery_source_spec',
                side_effect=recovery_source_error,
            ) as recovery_spec_mock, \
            mock.patch.object(
                iceberg_routes,
                'plan_staging_uploads',
                side_effect=record('plan_uploads', s3_keys),
            ) as plan_staging_uploads_mock, \
            mock.patch.object(
                iceberg_routes, 'restart_staging', side_effect=record('restart')
            ) as restart_mock, \
            mock.patch.object(
                iceberg_routes,
                'finalize_attempt',
                autospec=True,
                side_effect=record('finalize'),
            ) as finalize_mock, \
            mock.patch.object(
                iceberg_routes,
                'complete_partial_state_handoff',
                side_effect=complete_state_handoff,
            ) as handoff_mock, \
            mock.patch.object(
                iceberg_routes,
                'validate_route_config',
                side_effect=record('validate', 3),
            ) as validate_mock, \
            mock.patch(
                'pipelinewise.fastsync.partialsync.rdbms_to_snowflake.os.path.getsize',
                return_value=4,
            ):
        source = source_class_mock.return_value
        target = target_class_mock.return_value
        publisher = create_publisher_mock.return_value
        source.map_column_types_to_target.side_effect = record(
            'source.map',
            {
                'columns': columns,
                'primary_key': primary_keys,
                'source_column_names': ['foo_column'],
            },
        )
        source.export_source_table_data.side_effect = record(
            'source.export', file_parts
        )
        source_open_method = (
            source.open_connections
            if source_class_name == 'FastSyncTapMySql'
            else source.open_connection
        )
        source_open_method.side_effect = record(
            'source.open', error=source_open_error
        )
        bookmark_mock.side_effect = record('bookmark', bookmark)
        target.create_schema.side_effect = record('target.create_schema')
        target.copy_to_table.side_effect = record(
            'target.copy', 0 if empty_export else 1
        )
        target.obfuscate_columns.side_effect = record('obfuscate')
        publisher.prepare_partial_sync.side_effect = record('prepare', attempt)
        publisher.plan_partial_sync.side_effect = record('plan')
        publisher.record_uploaded.side_effect = record('record_uploaded')
        publisher.record_staging_created.side_effect = record(
            'record_staging_created'
        )
        staged_row_count = 0 if empty_export else 1
        publisher.staging_evidence.side_effect = record(
            'staging_evidence', (staged_row_count, 'staged-fingerprint')
        )
        publisher.record_staged.side_effect = record('record_staged')
        publisher.publish_partial_sync.side_effect = record(
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

        result = route_module.partial_sync_table(table, args)

    primary_key_error = (
        ValueError('Iceberg PartialSync requires a primary key for foo')
        if missing_primary_key
        else None
    )
    expected_error = (
        recovery_error
        or primary_key_error
        or recovery_source_error
        or upload_error
        or publish_error
    )
    assert result == (f'foo: {expected_error}' if expected_error else True)
    validate_mock.assert_called_once_with(args.target)
    if source_class_name == 'FastSyncTapMySql' and not (
        recovering and not restarting
    ):
        source.set_mariadb_json_aliases_enabled.assert_called_once_with(True)
    runtime_args = target_name_mock.call_args.args[0]
    assert runtime_args is not args
    assert args.table == original_table
    assert runtime_args.table == 'foo'
    target_name_mock.assert_called_once_with(runtime_args, 'foo_schema', 'foo')
    if recovering and not restarting:
        source_class_mock.assert_not_called()
        create_spec_mock.assert_not_called()
    else:
        source_class_mock.assert_called_once()
        assert create_spec_mock.call_args_list[0] == mock.call(
            runtime_args, 'foo_schema', 'foo', columns, primary_keys
        )
    create_publisher_mock.assert_called_once_with(target, runtime_args)
    publisher.table_lock.assert_called_once_with(recovery_target, recovery_identity)
    publisher.load_attempt.assert_called_once_with(
        recovery_target,
        expected_kind='partial',
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
    target.create_schema.assert_called_once_with('foo_schema')
    assert timeline[-1] == 'lock.exit'
    native_load_mock.assert_not_called()

    if missing_primary_key:
        plan_staging_uploads_mock.assert_not_called()
        bookmark_mock.assert_not_called()
        source.export_source_table_data.assert_not_called()
        upload_mock.assert_not_called()
        target.create_table.assert_not_called()
        publisher.prepare_partial_sync.assert_not_called()
        publisher.plan_partial_sync.assert_not_called()
        publisher.publish_partial_sync.assert_not_called()
        recovery_spec_mock.assert_not_called()
        return

    if recovering and not restarting:
        recovery_spec_mock.assert_not_called()
    elif restarting:
        expected_calls = [mock.call(persisted_spec, current_spec)]
        if not recovery_source_error:
            expected_calls.append(mock.call(persisted_spec, current_spec))
        assert recovery_spec_mock.call_args_list == expected_calls
    else:
        recovery_spec_mock.assert_called_once_with(current_spec, current_spec)

    if upload_cleanup_debt:
        plan_staging_uploads_mock.assert_called_once_with(
            publisher, attempt, target, file_parts
        )
        upload_mock.assert_called_once_with(
            target,
            file_parts,
            args.temp_dir,
            planned_s3_keys=s3_keys,
        )
        assert timeline.index('plan_uploads') < timeline.index('record_uploaded')
        publisher.record_uploaded.assert_called_once_with(attempt, s3_keys)
        target.create_table.assert_not_called()
        target.copy_to_table.assert_not_called()
        publisher.record_staging_created.assert_not_called()
        publisher.record_staged.assert_not_called()
        publisher.publish_partial_sync.assert_not_called()
        finalize_mock.assert_not_called()
        handoff_mock.assert_not_called()
        save_state_mock.assert_not_called()
        return

    if recovering and not restarting:
        plan_staging_uploads_mock.assert_not_called()
        bookmark_mock.assert_not_called()
        source.export_source_table_data.assert_not_called()
        upload_mock.assert_not_called()
        target.create_table.assert_not_called()
        publisher.prepare_partial_sync.assert_not_called()
        publisher.plan_partial_sync.assert_not_called()
        publisher.reconcile.assert_called_once_with(attempt, persisted_spec)
        if recovery_action == iceberg_routes.RECOVERY_PUBLISH:
            publisher.publish_partial_sync.assert_called_once_with(
                attempt, persisted_spec
            )
        else:
            publisher.publish_partial_sync.assert_not_called()
        source.map_column_types_to_target.assert_not_called()
        assert timeline.index('load_attempt') < timeline.index('reconcile')
    else:
        if restarting:
            bookmark_mock.assert_not_called()
            publisher.prepare_partial_sync.assert_not_called()
            restart_mock.assert_called_once()
            if recovery_source_error:
                source.export_source_table_data.assert_not_called()
                upload_mock.assert_not_called()
                target.create_table.assert_not_called()
                target.copy_to_table.assert_not_called()
                publisher.plan_partial_sync.assert_not_called()
                publisher.publish_partial_sync.assert_not_called()
                finalize_mock.assert_not_called()
                handoff_mock.assert_not_called()
                save_state_mock.assert_not_called()
                return
        else:
            publisher.prepare_partial_sync.assert_called_once_with(
                current_spec,
                bookmark,
                PartialSyncBoundary(
                    'foo_column',
                    '1',
                    drop_target=drop_target,
                ),
                recovery_identity=recovery_identity,
                staging_config=staging_config,
            )
        publisher.plan_partial_sync.assert_called_once_with(
            attempt, publication_spec
        )
        assert create_spec_mock.call_args_list == [
            mock.call(runtime_args, 'foo_schema', 'foo', columns, primary_keys),
            mock.call(runtime_args, 'foo_schema', 'foo', columns, primary_keys),
        ]
        target.create_table.assert_called_once_with(
            'foo_schema',
            'foo',
            columns,
            primary_keys,
            is_temporary=True,
            staging_table_name='PW_STAGE_123',
        )
        target.copy_to_table.assert_called_once_with(
            s3_pattern,
            'foo_schema',
            'foo',
            0 if empty_export else 4,
            is_temporary=True,
            staging_table_name='PW_STAGE_123',
        )
        target.obfuscate_columns.assert_called_once_with(
            'foo_schema', 'foo', staging_table_name='PW_STAGE_123'
        )
        publisher.record_uploaded.assert_called_once_with(attempt, s3_keys)
        plan_staging_uploads_mock.assert_called_once_with(
            publisher, attempt, target, file_parts
        )
        upload_mock.assert_called_once_with(
            target,
            file_parts,
            args.temp_dir,
            planned_s3_keys=s3_keys,
        )
        publisher.record_staging_created.assert_called_once_with(attempt)
        publisher.staging_evidence.assert_called_once_with(
            attempt, publication_spec, staged_row_count
        )
        publisher.record_staged.assert_called_once_with(
            attempt,
            row_count=staged_row_count,
            row_fingerprint='staged-fingerprint',
        )
        publisher.publish_partial_sync.assert_called_once_with(
            attempt, publication_spec
        )
        if restarting:
            assert timeline.index('restart') < timeline.index('source.export')
            assert timeline.index('restart') < timeline.index('source.open')
        else:
            assert timeline.index('source.map') < timeline.index('bookmark')
            assert timeline.index('plan') < timeline.index('source.export')
        assert timeline.index('record_uploaded') < timeline.index(
            'record_staging_created'
        )
        assert timeline.index('plan_uploads') < timeline.index('record_uploaded')
        assert timeline.index('obfuscate') < timeline.index('staging_evidence')
        assert timeline.index('record_staged') < timeline.index('publish')

    target.swap_tables.assert_not_called()
    if publish_error or recovery_error:
        finalize_mock.assert_not_called()
        handoff_mock.assert_not_called()
        save_state_mock.assert_not_called()
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
                'foo_schema',
                'foo',
                attempt,
                'Successful Iceberg PartialSync staging cleanup',
            )
        handoff_mock.assert_called_once()
        save_state_mock.assert_called_once_with(args.state, 'foo', bookmark)
        publisher.complete_state_handoff.assert_called_once_with(attempt)
        if recovery_action != iceberg_routes.RECOVERY_STATE_HANDOFF:
            assert timeline.index('finalize') < timeline.index('state_handoff')
