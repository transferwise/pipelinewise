from argparse import Namespace
from types import SimpleNamespace
from unittest import mock

import pytest

from pipelinewise.fastsync.commons import snowflake_iceberg_routes as routes
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    IcebergColumn,
    IcebergTableSpec,
    SnowflakeObjectName,
    TABLE_FORMAT_MANAGED_ICEBERG_V3,
    TABLE_FORMAT_MISSING,
    TABLE_FORMAT_NATIVE,
    TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG,
    TableCompatibilityError,
    TableFormatDiscoveryError,
)

# pylint: disable=missing-function-docstring,invalid-name


@pytest.mark.parametrize(
    ('target_config', 'expected'),
    [
        ({}, None),
        ({'target_table_format': 'native'}, None),
        ({
            'target_table_format': 'iceberg',
            'iceberg_version': 3,
            'hard_delete': True,
            'data_flattening_max_level': 0,
        }, 3),
    ],
)
def test_validate_route_config(target_config, expected):
    assert routes.validate_route_config(target_config) == expected


@pytest.mark.parametrize(
    ('override', 'message'),
    [
        ({'iceberg_version': True}, 'iceberg_version'),
        ({'iceberg_version': 3.0}, 'iceberg_version'),
        ({'iceberg_version': '3'}, 'iceberg_version'),
        ({'iceberg_version': 2}, 'iceberg_version'),
        ({'iceberg_version': 4}, 'iceberg_version'),
        ({'hard_delete': False}, 'hard_delete'),
        ({'data_flattening_max_level': False}, 'data_flattening_max_level'),
        ({'data_flattening_max_level': 1}, 'data_flattening_max_level'),
        ({'iceberg_query_history_poll_timeout_seconds': False}, 'query_history'),
        ({'iceberg_query_history_poll_timeout_seconds': 0}, 'query_history'),
        ({'iceberg_query_history_poll_timeout_seconds': -1}, 'query_history'),
        ({'iceberg_query_history_poll_timeout_seconds': 900.0}, 'query_history'),
        ({'iceberg_query_history_poll_timeout_seconds': '900'}, 'query_history'),
    ],
)
def test_validate_route_config_rejects_unsupported_iceberg_settings(
    override, message
):
    target_config = {
        'target_table_format': 'iceberg',
        'iceberg_version': 3,
        'hard_delete': True,
        'data_flattening_max_level': 0,
        **override,
    }

    with pytest.raises(ValueError, match=message):
        routes.validate_route_config(target_config)


@pytest.mark.parametrize(
    ('target_config', 'message'),
    [
        ({'iceberg_create': True}, 'iceberg_create'),
        ({'iceberg_create': False}, 'iceberg_create'),
        ({'iceberg_create': 'true'}, 'iceberg_create'),
        ({'iceberg_version': 3}, 'target_table_format'),
        ({'target_table_format': 'Iceberg', 'iceberg_version': 3}, 'target_table_format'),
        ({'target_table_format': None}, 'target_table_format'),
        ({'target_table_format': 'native', 'iceberg_version': 3}, 'iceberg_version'),
    ],
)
def test_validate_route_config_rejects_stray_direct_format_settings(
    target_config, message
):
    with pytest.raises(ValueError, match=message):
        routes.validate_route_config(target_config)


def test_staging_config_identity_contains_no_credentials():
    config = {
        's3_bucket': 'bucket',
        's3_key_prefix': 'prefix',
        'stage': 'stage',
        'file_format': 'format',
        'aws_secret_access_key': 'secret',
    }

    assert routes.staging_config_identity(config) == {
        's3_bucket': 'bucket',
        's3_key_prefix': 'prefix',
        'stage': 'stage',
        'file_format': 'format',
    }


@pytest.mark.parametrize(
    'table_format',
    [TABLE_FORMAT_MISSING, TABLE_FORMAT_NATIVE],
)
def test_require_native_target_format_accepts_missing_or_native(table_format):
    args = Namespace(
        target={'dbname': 'target_db'},
        state='/tmp/state.json',
        temp_dir='/tmp',
    )
    publisher = mock.Mock()
    publisher.discover_table_format.return_value = table_format

    with mock.patch.object(routes, 'create_publisher', return_value=publisher):
        result = routes.require_native_target_format(
            mock.Mock(),
            args,
            'target_schema',
            'source.target_table',
            allow_missing=True,
        )

    assert result == table_format
    publisher.discover_table_format.assert_called_once_with(
        'TARGET_SCHEMA', 'TARGET_TABLE'
    )


@pytest.mark.parametrize(
    'table_format',
    [
        TABLE_FORMAT_MANAGED_ICEBERG_V3,
        TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG,
    ],
)
def test_require_native_target_format_rejects_iceberg(table_format):
    args = Namespace(
        target={'dbname': 'target_db'},
        state='/tmp/state.json',
        temp_dir='/tmp',
    )
    publisher = mock.Mock()
    publisher.discover_table_format.return_value = table_format

    with mock.patch.object(routes, 'create_publisher', return_value=publisher), \
            pytest.raises(TableCompatibilityError, match=f'found {table_format}'):
        routes.require_native_target_format(
            mock.Mock(),
            args,
            'target_schema',
            'source.target_table',
            allow_missing=True,
        )


@pytest.mark.parametrize(
    'table_format',
    [
        TABLE_FORMAT_MISSING,
        TABLE_FORMAT_MANAGED_ICEBERG_V3,
        TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG,
    ],
)
def test_require_native_target_format_requires_native_after_creation(table_format):
    args = Namespace(
        target={'dbname': 'target_db'},
        state='/tmp/state.json',
        temp_dir='/tmp',
    )
    publisher = mock.Mock()
    publisher.discover_table_format.return_value = table_format

    with mock.patch.object(routes, 'create_publisher', return_value=publisher), \
            pytest.raises(TableCompatibilityError, match=f'found {table_format}'):
        routes.require_native_target_format(
            mock.Mock(),
            args,
            'target_schema',
            'source.target_table',
            allow_missing=False,
        )


def test_require_native_target_format_accepts_native_after_creation():
    args = Namespace(
        target={'dbname': 'target_db'},
        state='/tmp/state.json',
        temp_dir='/tmp',
    )
    publisher = mock.Mock()
    publisher.discover_table_format.return_value = TABLE_FORMAT_NATIVE

    with mock.patch.object(routes, 'create_publisher', return_value=publisher):
        result = routes.require_native_target_format(
            mock.Mock(),
            args,
            'target_schema',
            'source.target_table',
            allow_missing=False,
        )

    assert result == TABLE_FORMAT_NATIVE


def test_require_native_target_format_rejects_unsupported_managed_version():
    args = Namespace(
        target={'dbname': 'target_db'},
        state='/tmp/state.json',
        temp_dir='/tmp',
    )
    publisher = mock.Mock()
    publisher.discover_table_format.side_effect = TableFormatDiscoveryError(
        'Snowflake returned unsupported ICEBERG_VERSION 2'
    )

    with mock.patch.object(routes, 'create_publisher', return_value=publisher), \
            pytest.raises(TableFormatDiscoveryError, match='ICEBERG_VERSION 2'):
        routes.require_native_target_format(
            mock.Mock(),
            args,
            'target_schema',
            'source.target_table',
            allow_missing=True,
        )


def _recovery_args(tap_override=None, target_override=None, transform=None):
    tap = {
        'host': 'source.example',
        'port': 3306,
        'dbname': 'source_db',
        'user': 'source_user',
        'engine': 'mariadb',
        **(tap_override or {}),
    }
    target = {
        'tap_id': 'tap_test',
        'account': 'target_account',
        'dbname': 'target_db',
        'user': 'target_user',
        'role': 'target_role',
        'default_target_schema': 'target_schema',
        'target_table_format': 'iceberg',
        'iceberg_version': 3,
        **(target_override or {}),
    }
    return Namespace(
        tap=tap,
        target=target,
        transform=transform if transform is not None else {},
    )


def _recovery_identity(args, table='source.table'):
    return routes.fastsync_recovery_identity(
        args,
        table,
        source_route='mysql_to_snowflake',
        source_engine=args.tap['engine'],
        staging_config={'s3_bucket': 'staging'},
        iceberg_version=args.target['iceberg_version'],
    )


@pytest.mark.parametrize(
    'changed_args',
    [
        _recovery_args(tap_override={'host': 'other-source.example'}),
        _recovery_args(tap_override={'dbname': 'other_source'}),
        _recovery_args(tap_override={'user': 'other_user'}),
        _recovery_args(tap_override={'charset': 'utf8mb4'}),
        _recovery_args(tap_override={'session_sqls': ['SET time_zone="+01:00"']}),
        _recovery_args(tap_override={'use_gtid': True}),
        _recovery_args(target_override={'account': 'other_account'}),
        _recovery_args(target_override={'role': 'other_role'}),
        _recovery_args(target_override={'default_target_schema': 'other_schema'}),
        _recovery_args(transform={'transformations': [{'field': 'secret'}]}),
    ],
)
def test_fastsync_recovery_identity_detects_execution_contract_drift(changed_args):
    original = _recovery_identity(_recovery_args())
    changed = _recovery_identity(changed_args)

    assert changed['stream_fingerprint'] == original['stream_fingerprint']
    assert changed['fingerprint'] != original['fingerprint']


def test_fastsync_recovery_identity_uses_separate_opaque_stream_keys():
    first = _recovery_identity(_recovery_args(), 'source.first')
    second = _recovery_identity(_recovery_args(), 'source.second')

    assert first['stream_fingerprint'] != second['stream_fingerprint']
    assert set(first) == {
        'identity_version',
        'scope',
        'stream_fingerprint',
        'target_table_format',
        'iceberg_version',
        'transformation_semantics_version',
        'transformation_fingerprint',
        'fingerprint',
    }
    assert 'source.example' not in str(first)


def test_fastsync_recovery_identity_excludes_query_history_budget():
    original = _recovery_identity(_recovery_args())
    changed = _recovery_identity(_recovery_args(target_override={
        'iceberg_query_history_poll_timeout_seconds': 3600,
    }))

    assert changed == original


def test_create_publisher_uses_target_directory(tmp_path):
    target_dir = tmp_path / 'target'
    state_path = target_dir / 'tap' / 'state.json'
    args = Namespace(state=str(state_path), temp_dir='/other')
    snowflake = mock.Mock()

    with mock.patch.object(routes, 'SnowflakeIcebergPublisher') as publisher_class:
        publisher = routes.create_publisher(snowflake, args)

    assert publisher is publisher_class.return_value
    publisher_class.assert_called_once_with(
        snowflake,
        str(target_dir),
        history_poll_interval_seconds=5.0,
        history_poll_timeout_seconds=900,
    )


def test_create_publisher_uses_configured_query_history_budget(tmp_path):
    target_dir = tmp_path / 'target'
    args = Namespace(
        state=str(target_dir / 'tap' / 'state.json'),
        temp_dir='/other',
        target={'iceberg_query_history_poll_timeout_seconds': 3600},
    )

    with mock.patch.object(routes, 'SnowflakeIcebergPublisher') as publisher_class:
        routes.create_publisher(mock.Mock(), args)

    publisher_class.assert_called_once_with(
        mock.ANY,
        str(target_dir),
        history_poll_interval_seconds=5.0,
        history_poll_timeout_seconds=3600,
    )


def test_create_publishers_for_different_taps_share_target_directory(tmp_path):
    target_dir = tmp_path / 'target'
    snowflake = mock.Mock()

    first = routes.create_publisher(
        snowflake,
        Namespace(state=str(target_dir / 'tap-one' / 'state.json'), temp_dir='/other'),
    )
    second = routes.create_publisher(
        snowflake,
        Namespace(state=str(target_dir / 'tap-two' / 'state.json'), temp_dir='/other'),
    )

    assert first.runtime_dir == second.runtime_dir == str(target_dir)


def test_create_publisher_requires_state_for_iceberg(tmp_path):
    args = Namespace(
        state=None,
        temp_dir=str(tmp_path),
        target={
            'target_table_format': 'iceberg',
            'iceberg_version': 3,
            'hard_delete': True,
            'data_flattening_max_level': 0,
        },
    )

    with pytest.raises(ValueError, match='requires a state file'):
        routes.create_publisher(mock.Mock(), args)


def test_create_spec_matches_fastsync_uppercase_target_identifiers():
    args = Namespace(target={'dbname': 'target Db', 'iceberg_version': 3})
    columns = ['"Mixed Column" VARCHAR']
    primary_key = ['"Mixed Column"']

    with mock.patch.object(routes.IcebergTableSpec, 'from_fastsync') as from_fastsync:
        routes.create_spec(
            args,
            'Mixed Schema',
            'source.table With Space',
            columns,
            primary_key,
        )

    from_fastsync.assert_called_once_with(
        'TARGET DB',
        'MIXED SCHEMA',
        'TABLE WITH SPACE',
        columns,
        primary_key,
        3,
    )


def test_require_partial_sync_primary_key_fails_before_publication():
    with pytest.raises(ValueError, match='requires a primary key for source.table'):
        routes.require_partial_sync_primary_key(None, 'source.table')


def _table_spec(columns, primary_key=('ID',)):
    return IcebergTableSpec(
        SnowflakeObjectName('DATABASE', 'SCHEMA', 'TABLE'),
        tuple(columns),
        primary_key,
    )


def test_recovery_source_spec_rejects_new_columns_before_positional_export():
    persisted = _table_spec((
        IcebergColumn('ID', 'NUMBER(19,0)', False),
        IcebergColumn('VALUE', 'VARIANT'),
    ))
    current = _table_spec((
        *persisted.columns,
        IcebergColumn('NEW_VALUE', 'VARCHAR'),
    ))

    with pytest.raises(TableCompatibilityError, match='added columns: NEW_VALUE'):
        routes.validate_recovery_source_spec(persisted, current)


def test_recovery_source_spec_rejects_column_reordering():
    persisted = _table_spec((
        IcebergColumn('ID', 'NUMBER(19,0)', False),
        IcebergColumn('VALUE', 'VARIANT'),
    ))
    current = _table_spec(tuple(reversed(persisted.columns)))

    with pytest.raises(TableCompatibilityError, match='column order changed'):
        routes.validate_recovery_source_spec(persisted, current)


@pytest.mark.parametrize(
    ('current_columns', 'message'),
    [
        (
            (IcebergColumn('ID', 'NUMBER(19,0)', False),),
            'missing columns: VALUE',
        ),
        (
            (
                IcebergColumn('ID', 'NUMBER(19,0)', False),
                IcebergColumn('VALUE', 'VARCHAR'),
            ),
            'changed columns: VALUE',
        ),
        (
            (
                IcebergColumn('ID', 'NUMBER(19,0)', False),
                IcebergColumn('VALUE', 'VARIANT', False),
            ),
            'changed columns: VALUE',
        ),
    ],
)
def test_recovery_source_spec_rejects_missing_or_incompatible_columns(
    current_columns, message
):
    persisted = _table_spec((
        IcebergColumn('ID', 'NUMBER(19,0)', False),
        IcebergColumn('VALUE', 'VARIANT'),
    ))

    with pytest.raises(TableCompatibilityError, match=message):
        routes.validate_recovery_source_spec(
            persisted,
            _table_spec(current_columns),
        )


def test_recovery_source_spec_rejects_primary_key_changes():
    columns = (
        IcebergColumn('ID', 'NUMBER(19,0)', False),
        IcebergColumn('VALUE', 'VARIANT'),
    )

    with pytest.raises(TableCompatibilityError, match='primary key'):
        routes.validate_recovery_source_spec(
            _table_spec(columns),
            _table_spec(columns, ()),
        )


def test_recovery_source_spec_rejects_target_changes():
    persisted = _table_spec((IcebergColumn('ID', 'NUMBER(19,0)', False),))
    current = IcebergTableSpec(
        SnowflakeObjectName('DATABASE', 'OTHER_SCHEMA', 'TABLE'),
        persisted.columns,
        persisted.primary_key,
    )

    with pytest.raises(TableCompatibilityError, match='different Iceberg target'):
        routes.validate_recovery_source_spec(persisted, current)


def test_finalize_attempt_completes_all_cleanup_before_marking():
    publisher = mock.Mock()
    snowflake = mock.Mock()
    attempt = SimpleNamespace(
        s3_keys=['load/part.csv.gz'],
        staging_table='PW_STAGE_123',
        manifest_payload=SimpleNamespace(replacement_metadata=None),
    )
    target_config = {'s3_bucket': 'bucket'}

    with mock.patch.object(routes.utils, 'retry_snowflake_table_grants') as grants, \
            mock.patch.object(routes.utils, 'delete_s3_objects') as delete_s3:
        routes.finalize_attempt(
            publisher,
            snowflake,
            target_config,
            'SCHEMA',
            'source.table',
            attempt,
            'test cleanup',
        )

    grants.assert_called_once_with(
        snowflake, target_config, 'SCHEMA', 'source.table'
    )
    delete_s3.assert_called_once_with(
        snowflake,
        ['load/part.csv.gz'],
        'bucket',
        cleanup_context='test cleanup',
    )
    snowflake.drop_table.assert_called_once_with(
        'SCHEMA',
        'source.table',
        is_temporary=True,
        max_attempts=3,
        staging_table_name='PW_STAGE_123',
    )
    publisher.mark_finalized.assert_called_once_with(attempt)
    assert publisher.record_finalization_action.call_args_list == [
        mock.call(attempt, routes.FINALIZATION_GRANTS),
        mock.call(attempt, routes.FINALIZATION_S3_CLEANUP),
        mock.call(attempt, routes.FINALIZATION_STAGING_CLEANUP),
    ]


def test_finalize_attempt_restores_replacement_metadata_once():
    publisher = mock.Mock()
    snowflake = mock.Mock()
    attempt = SimpleNamespace(
        s3_keys=[],
        staging_table='PW_STAGE_123',
        manifest_payload=SimpleNamespace(
            replacement_metadata={'table_comment': 'kept'},
        ),
        finalization={routes.FINALIZATION_GRANTS: True},
    )

    with mock.patch.object(routes.utils, 'retry_snowflake_table_grants') as grants, \
            mock.patch.object(routes.utils, 'delete_s3_objects'):
        routes.finalize_attempt(
            publisher,
            snowflake,
            {'s3_bucket': 'bucket'},
            'SCHEMA',
            'source.table',
            attempt,
            'test cleanup',
        )

    publisher.restore_metadata.assert_called_once_with(attempt)
    grants.assert_not_called()
    assert publisher.record_finalization_action.call_args_list == [
        mock.call(attempt, routes.FINALIZATION_METADATA),
        mock.call(attempt, routes.FINALIZATION_S3_CLEANUP),
        mock.call(attempt, routes.FINALIZATION_STAGING_CLEANUP),
    ]


def test_finalize_attempt_runs_remaining_cleanup_and_withholds_finalized_marker():
    publisher = mock.Mock()
    snowflake = mock.Mock()
    attempt = SimpleNamespace(
        s3_keys=['load/part.csv.gz'],
        staging_table='PW_STAGE_123',
        manifest_payload=SimpleNamespace(replacement_metadata=None),
    )

    with mock.patch.object(
        routes.utils,
        'retry_snowflake_table_grants',
        side_effect=RuntimeError('grant failed'),
    ), mock.patch.object(routes.utils, 'delete_s3_objects') as delete_s3:
        with pytest.raises(RuntimeError, match='grant application: grant failed'):
            routes.finalize_attempt(
                publisher,
                snowflake,
                {'s3_bucket': 'bucket'},
                'SCHEMA',
                'source.table',
                attempt,
                'test cleanup',
            )

    delete_s3.assert_called_once()
    snowflake.drop_table.assert_called_once()
    publisher.mark_finalized.assert_not_called()


def test_complete_state_handoff_keeps_manifest_when_state_write_fails():
    publisher = mock.Mock()
    attempt = SimpleNamespace(source_bookmark={'position': 42})
    state_writer = mock.Mock(side_effect=RuntimeError('state failed'))

    with pytest.raises(RuntimeError, match='state failed'):
        routes.complete_state_handoff(publisher, attempt, state_writer)

    publisher.complete_state_handoff.assert_not_called()


@pytest.mark.parametrize(('end_value', 'state_writes'), [(None, 1), ('20', 0)])
def test_complete_partial_state_handoff_uses_persisted_range(
    end_value, state_writes
):
    publisher = mock.Mock()
    attempt = SimpleNamespace(
        source_bookmark={'position': 42},
        manifest_payload=SimpleNamespace(end_is_unbounded=end_value is None),
    )

    with mock.patch.object(routes.utils, 'save_state_file') as save_state:
        routes.complete_partial_state_handoff(
            publisher, attempt, '/runtime/state.json', 'source.table'
        )

    assert save_state.call_count == state_writes
    if state_writes:
        save_state.assert_called_once_with(
            '/runtime/state.json', 'source.table', {'position': 42}
        )
    publisher.complete_state_handoff.assert_called_once_with(attempt)


def test_restart_staging_cleans_both_stores_before_resetting_manifest():
    publisher = mock.Mock()
    snowflake = mock.Mock()
    attempt = SimpleNamespace(
        s3_keys=['load/part.csv.gz'],
        staging_table='PW_STAGE_123',
    )
    target_config = {'s3_bucket': 'bucket'}

    with mock.patch.object(routes.utils, 'delete_s3_objects') as delete_s3:
        routes.restart_staging(
            publisher,
            snowflake,
            target_config,
            'SCHEMA',
            'source.table',
            attempt,
        )

    delete_s3.assert_called_once_with(
        snowflake,
        ['load/part.csv.gz'],
        'bucket',
        cleanup_context='Incomplete Iceberg staging cleanup',
    )
    snowflake.drop_table.assert_called_once_with(
        'SCHEMA',
        'source.table',
        is_temporary=True,
        max_attempts=3,
        staging_table_name='PW_STAGE_123',
    )
    publisher.reset_staging.assert_called_once_with(attempt)


def test_restart_staging_does_not_reset_manifest_when_cleanup_fails():
    publisher = mock.Mock()
    snowflake = mock.Mock()
    attempt = SimpleNamespace(s3_keys=[], staging_table='PW_STAGE_123')
    snowflake.drop_table.side_effect = RuntimeError('drop failed')

    with pytest.raises(RuntimeError, match='Snowflake staging cleanup: drop failed'):
        routes.restart_staging(
            publisher,
            snowflake,
            {'s3_bucket': 'bucket'},
            'SCHEMA',
            'source.table',
            attempt,
        )

    publisher.reset_staging.assert_not_called()


@pytest.mark.parametrize(
    ('action', 'restart_count', 'publish_count', 'finalize_count', 'recovered'),
    [
        (routes.RECOVERY_RESTART_STAGING, 1, 0, 0, False),
        (routes.RECOVERY_PUBLISH, 0, 1, 1, True),
        (routes.RECOVERY_FINALIZE, 0, 0, 1, True),
        (routes.RECOVERY_STATE_HANDOFF, 0, 0, 0, True),
    ],
)
def test_resume_attempt_dispatches_durable_action(
    action, restart_count, publish_count, finalize_count, recovered
):
    publisher = mock.Mock()
    attempt = mock.Mock()
    spec = mock.Mock()
    publisher.reconcile.return_value = SimpleNamespace(action=action)
    restart = mock.Mock()
    publish = mock.Mock()
    finalize = mock.Mock()
    state_handoff = mock.Mock()

    result = routes.resume_attempt(
        publisher,
        attempt,
        spec,
        restart,
        publish,
        finalize,
        state_handoff,
    )

    assert result is recovered
    assert restart.call_count == restart_count
    assert publish.call_count == publish_count
    assert finalize.call_count == finalize_count
    assert state_handoff.call_count == int(recovered)


def test_resume_attempt_rejects_unknown_action_without_state_handoff():
    publisher = mock.Mock()
    publisher.reconcile.return_value = SimpleNamespace(action='unknown')
    state_handoff = mock.Mock()

    with pytest.raises(RuntimeError, match='Unsupported Iceberg recovery action'):
        routes.resume_attempt(
            publisher,
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
            state_handoff,
        )

    state_handoff.assert_not_called()
