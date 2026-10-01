"""PostgreSQL pgoutput orchestration and durable migration state tests."""

import json

from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest

from pipelinewise.cli import commands
from pipelinewise.cli.errors import PreRunChecksException
from pipelinewise.cli.pipelinewise import PipelineWise
from pipelinewise.fastsync.commons.tap_postgres import (
    FastSyncTapPostgres,
    PGOUTPUT_MIGRATION_STATE_KEY,
)


def _runner(tmp_path, streams=None):
    properties_path = tmp_path / 'properties.json'
    properties_path.write_text(json.dumps({'streams': streams or []}), encoding='utf-8')
    runner = object.__new__(PipelineWise)
    runner.tap = {
        'files': {
            'config': str(tmp_path / 'config.json'),
            'properties': str(properties_path),
        },
    }
    runner.tap_bin = '/connector path/tap-postgres'
    runner.tap_run_log_file = str(tmp_path / 'tap.log')
    runner.profiling_mode = False
    runner.profiling_dir = None
    runner.extra_log = False
    runner.logger = Mock()
    return runner


def _tap_files(tmp_path, state):
    state_path = tmp_path / 'state.json'
    config_path = tmp_path / 'config.json'
    state_path.write_text(json.dumps(state), encoding='utf-8')
    config_path.write_text(
        json.dumps({'dbname': 'my_db', 'tap_id': 'my_tap'}), encoding='utf-8'
    )
    return SimpleNamespace(
        type='tap-postgres',
        state=str(state_path),
        config=str(config_path),
    )


def _migration_marker(phase='bridge'):
    marker = {
        'version': 1,
        'phase': phase,
        'source_slot': 'pipelinewise_my_db_my_tap',
        'destination_slot': 'pipelinewise_my_tap',
        'copy_lsn': 90,
        'bridge_lsn': 100,
    }
    if phase == 'retire':
        marker['retire_lsn'] = 120
    return marker


def _logical_stream(stream_id):
    return {
        'tap_stream_id': stream_id,
        'metadata': [{
            'breadcrumb': [],
            'metadata': {'selected': True, 'replication-method': 'LOG_BASED'},
        }],
    }


def test_publication_preflight_uses_connector_cli_and_reports_failure(tmp_path):
    runner = _runner(tmp_path)
    expected = [
        '/connector path/tap-postgres',
        '--config',
        str(tmp_path / 'config.json'),
        '--catalog',
        str(tmp_path / 'properties.json'),
        '--prepare-publication',
    ]

    with patch.object(commands, 'run_command_argv', return_value=[0, '', '']) as run:
        runner._prepare_postgres_pgoutput_publication()
    run.assert_called_once_with(expected)

    with patch.object(commands, 'run_command_argv', return_value=[2, '', 'permission denied']):
        with pytest.raises(
            PreRunChecksException,
            match='permission denied.*No state or replication-slot changes were made',
        ):
            runner._prepare_postgres_pgoutput_publication()


def test_publication_preflight_preserves_paths_with_spaces_and_quotes(tmp_path):
    command_dir = tmp_path / "preflight 'quoted' directory"
    command_dir.mkdir()
    runner = _runner(command_dir)
    tap_bin = command_dir / 'tap "postgres" executable'
    tap_bin.write_text(
        '#!/bin/sh\nprintf \'%s\\n\' "$@" > "$0.args"\n',
        encoding='utf-8',
    )
    tap_bin.chmod(0o755)
    runner.tap_bin = str(tap_bin)

    runner._prepare_postgres_pgoutput_publication()

    assert (command_dir / f'{tap_bin.name}.args').read_text(encoding='utf-8').splitlines() == [
        '--config',
        str(command_dir / 'config.json'),
        '--catalog',
        str(command_dir / 'properties.json'),
        '--prepare-publication',
    ]


def test_persisted_bridge_is_advanced_and_rewritten_before_next_run(tmp_path):
    marker = _migration_marker()
    tap = _tap_files(tmp_path, {
        PGOUTPUT_MIGRATION_STATE_KEY: marker,
        'bookmarks': {'public-one': {'lsn': 100}},
    })
    runner = _runner(tmp_path, [_logical_stream('public-one')])
    updated = {**marker, 'phase': 'pgoutput'}

    with patch.object(
        FastSyncTapPostgres, 'advance_migrated_replication_slot', return_value=updated
    ) as advance:
        runner._process_postgres_pgoutput_migration(tap, after_success=False)

    advance.assert_called_once_with({'dbname': 'my_db', 'tap_id': 'my_tap'}, marker)
    assert json.loads((tmp_path / 'state.json').read_text(encoding='utf-8')) == {
        PGOUTPUT_MIGRATION_STATE_KEY: updated,
        'bookmarks': {'public-one': {'lsn': 100}},
    }


def test_bridge_is_retained_until_every_logical_bookmark_reaches_boundary(tmp_path):
    marker = _migration_marker()
    tap = _tap_files(tmp_path, {
        PGOUTPUT_MIGRATION_STATE_KEY: marker,
        'bookmarks': {
            'public-one': {'lsn': 100},
            'public-two': {'lsn': 99},
        },
    })
    runner = _runner(
        tmp_path, [_logical_stream('public-one'), _logical_stream('public-two')]
    )

    with patch.object(
        FastSyncTapPostgres, 'advance_migrated_replication_slot'
    ) as advance, pytest.raises(RuntimeError, match='target-durable bridge boundary'):
        runner._process_postgres_pgoutput_migration(tap, after_success=False)

    advance.assert_not_called()
    assert json.loads((tmp_path / 'state.json').read_text(encoding='utf-8'))[
        PGOUTPUT_MIGRATION_STATE_KEY
    ] == marker


def test_pre_run_never_retires_a_source_slot(tmp_path):
    marker = _migration_marker('retire')
    tap = _tap_files(tmp_path, {PGOUTPUT_MIGRATION_STATE_KEY: marker})
    runner = _runner(tmp_path)

    with patch.object(
        FastSyncTapPostgres, 'advance_canonical_replication_slot'
    ) as advance, patch.object(
        FastSyncTapPostgres, 'retire_migrated_replication_slot'
    ) as retire:
        runner._process_postgres_pgoutput_migration(tap, after_success=False)

    advance.assert_not_called()
    retire.assert_not_called()
    assert json.loads((tmp_path / 'state.json').read_text(encoding='utf-8'))[
        PGOUTPUT_MIGRATION_STATE_KEY
    ] == marker


def test_success_advances_minimum_durable_lsn_then_retires_source(tmp_path):
    streams = [_logical_stream('public-one'), _logical_stream('public-two')]
    runner = _runner(tmp_path, streams)
    marker = _migration_marker('retire')
    tap = _tap_files(tmp_path, {
        PGOUTPUT_MIGRATION_STATE_KEY: marker,
        'bookmarks': {
            'public-one': {'lsn': 130},
            'public-two': {'lsn': 125},
        },
    })

    calls = Mock()
    with patch.object(
        FastSyncTapPostgres, 'advance_canonical_replication_slot'
    ) as advance, patch.object(
        FastSyncTapPostgres, 'retire_migrated_replication_slot'
    ) as retire:
        calls.attach_mock(advance, 'advance')
        calls.attach_mock(retire, 'retire')
        runner._process_postgres_pgoutput_migration(tap, after_success=True)

    assert calls.mock_calls == [
        call.advance({'dbname': 'my_db', 'tap_id': 'my_tap'}, 125),
        call.retire({'dbname': 'my_db', 'tap_id': 'my_tap'}, marker),
    ]
    state = json.loads((tmp_path / 'state.json').read_text(encoding='utf-8'))
    assert PGOUTPUT_MIGRATION_STATE_KEY not in state
    assert state['bookmarks']['public-two']['lsn'] == 125


@pytest.mark.parametrize('bookmarks', [
    {'public-one': {'lsn': 130}, 'public-two': {'lsn': 119}},
    {'public-one': {'lsn': 130}},
])
def test_retirement_waits_for_every_logical_bookmark(tmp_path, bookmarks):
    runner = _runner(
        tmp_path, [_logical_stream('public-one'), _logical_stream('public-two')]
    )
    marker = _migration_marker('retire')
    tap = _tap_files(tmp_path, {
        PGOUTPUT_MIGRATION_STATE_KEY: marker,
        'bookmarks': bookmarks,
    })

    with patch.object(
        FastSyncTapPostgres, 'advance_canonical_replication_slot'
    ) as advance, patch.object(
        FastSyncTapPostgres, 'retire_migrated_replication_slot'
    ) as retire, pytest.raises(RuntimeError, match='target-durable pgoutput commit'):
        runner._process_postgres_pgoutput_migration(tap, after_success=True)

    advance.assert_not_called()
    retire.assert_not_called()
    assert json.loads((tmp_path / 'state.json').read_text(encoding='utf-8'))[
        PGOUTPUT_MIGRATION_STATE_KEY
    ] == marker


@pytest.mark.parametrize(
    'bookmarks, expected_lsn',
    [
        ({'public-one': {'lsn': 130}, 'public-two': {'lsn': 125}}, 125),
        ({'public-one': {'lsn': 130}}, None),
    ],
)
def test_success_releases_canonical_wal_only_at_shared_durable_lsn(
    tmp_path, bookmarks, expected_lsn
):
    runner = _runner(
        tmp_path, [_logical_stream('public-one'), _logical_stream('public-two')]
    )
    tap = _tap_files(tmp_path, {'bookmarks': bookmarks})

    with patch.object(
        FastSyncTapPostgres, 'advance_canonical_replication_slot'
    ) as advance:
        runner._process_postgres_pgoutput_migration(tap, after_success=True)

    if expected_lsn is None:
        advance.assert_not_called()
    else:
        advance.assert_called_once_with(
            {'dbname': 'my_db', 'tap_id': 'my_tap'}, expected_lsn
        )


def test_failed_singer_pipeline_never_runs_post_success_cleanup(tmp_path):
    runner = _runner(tmp_path)
    tap = _tap_files(tmp_path, {})

    with patch.object(
        runner, '_process_postgres_pgoutput_migration'
    ) as process_migration, patch.object(
        commands, 'build_singer_command', return_value='tap | target'
    ), patch.object(
        commands, 'run_command', side_effect=commands.RunCommandException('failed')
    ):
        with pytest.raises(commands.RunCommandException, match='failed'):
            runner.run_tap_singer(tap, Mock(), Mock())

    process_migration.assert_called_once_with(tap, after_success=False)


def test_final_target_state_is_persisted_before_post_success_cleanup(tmp_path):
    runner = _runner(tmp_path)
    tap = _tap_files(tmp_path, {})
    final_state = {'bookmarks': {'public-one': {'lsn': 123}}}
    phases = []

    def run_command(_command, _log_file, line_callback):
        line_callback(json.dumps(final_state))

    def process_migration(_tap, *, after_success):
        phases.append(after_success)
        if after_success:
            assert json.loads((tmp_path / 'state.json').read_text(encoding='utf-8')) == final_state

    with patch.object(
        runner, '_process_postgres_pgoutput_migration', side_effect=process_migration
    ), patch.object(
        commands, 'build_singer_command', return_value='tap | target'
    ), patch.object(commands, 'run_command', side_effect=run_command):
        runner.run_tap_singer(tap, Mock(), Mock())

    assert phases == [False, True]
