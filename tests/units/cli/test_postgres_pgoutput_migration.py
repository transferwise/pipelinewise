"""PostgreSQL pgoutput orchestration and durable migration state tests."""

import json
import sys
from pathlib import Path

from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest

from pipelinewise.cli import commands
from pipelinewise.cli.config import Config
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


def test_preflight_passes_durable_phase_and_explicit_fresh_start(tmp_path):
    runner = _runner(tmp_path)
    state = tmp_path / 'state.json'
    state.write_text('{}', encoding='utf-8')
    runner.tap['files']['state'] = str(state)
    with patch.object(commands, 'run_command_argv', return_value=[0, '', '']) as run:
        runner._prepare_postgres_pgoutput_publication(fresh_start=True)
    assert run.call_args.args[0][-3:] == ['--state', str(state), '--fresh-start']


def test_hung_preflight_is_killed_with_actionable_error():
    with pytest.raises(commands.RunCommandException, match='timed out.*source locks'):
        commands.run_command_argv([sys.executable, '-c', 'import time; time.sleep(20)'], timeout=0.05)


def test_logical_preparation_is_scoped_to_requested_tables(tmp_path):
    runner = _runner(tmp_path)
    runner.tap['type'] = 'tap-postgres'
    path = tmp_path / 'selection.json'
    path.write_text(json.dumps({'selection': [
        {'tap_stream_id': 'public-logical', 'replication_method': 'LOG_BASED'},
        {'tap_stream_id': 'public-full', 'replication_method': 'FULL_TABLE'},
    ]}), encoding='utf-8')
    runner.tap['files']['selection'] = str(path)
    assert not runner._postgres_tap_has_log_based_selection({'public.full'})
    assert runner._postgres_tap_has_log_based_selection({'public.logical'})


@pytest.mark.parametrize('second_lsn', [None, 90, 110])
def test_reset_retains_intent_until_all_snapshots_are_durable(tmp_path, second_lsn):
    runner = _runner(tmp_path, [_logical_stream('a'), _logical_stream('b')])
    state_path = tmp_path / 'state.json'
    state = {'bookmarks': {'a': {'lsn': 120}, 'b': {'lsn': second_lsn}},
             '_pipelinewise_pgoutput_fresh_start': {'version': 1}}
    state_path.write_text(json.dumps(state), encoding='utf-8')
    runner.tap['files']['state'] = str(state_path)
    result = {'source_slot': 'pipelinewise_db_old', 'destination_slot': 'pipelinewise_new', 'copy_lsn': 100}
    if second_lsn is None or second_lsn <= 100:
        with pytest.raises(PreRunChecksException, match='resync is incomplete'):
            runner._finish_postgres_slot_reset(result)
        assert json.loads(state_path.read_text()) == state
    else:
        runner._finish_postgres_slot_reset(result)
        persisted = json.loads(state_path.read_text())
        assert '_pipelinewise_pgoutput_fresh_start' not in persisted
        assert persisted[PGOUTPUT_MIGRATION_STATE_KEY]['phase'] == 'pgoutput'
        assert persisted[PGOUTPUT_MIGRATION_STATE_KEY]['bridge_lsn'] == 110


def test_rename_preserves_bookmarks_and_never_overwrites_new_progress(tmp_path):
    config = Config(str(tmp_path))
    old_files = Config.get_connector_files(str(tmp_path / 'target' / 'Old-id'))
    new_files = Config.get_connector_files(str(tmp_path / 'target' / 'new_id'))
    (tmp_path / 'target' / 'Old-id').mkdir(parents=True)
    source = {'host': 'host', 'port': 5432, 'dbname': 'database', 'user': 'user'}
    Path(old_files['config']).write_text(json.dumps({**source, 'tap_id': 'Old-id'}))
    Path(old_files['state']).write_text(json.dumps({'bookmarks': {'table': {'lsn': 123}}}))
    tap = {'id': 'new_id', 'type': 'tap-postgres', 'previous_tap_id': 'Old-id',
           'db_conn': source, 'files': new_files}
    target_connection = {'host': 'destination', 'port': 5432, 'dbname': 'warehouse'}
    config.targets = {'target': {'id': 'target', 'type': 'target-postgres',
                                 'db_conn': target_connection, 'taps': [tap]}}
    Path(config.config_path).write_text(json.dumps({'targets': [{'id': 'target', 'type': 'target-postgres'}]}))
    (tmp_path / 'target' / 'config.json').write_text(json.dumps(target_connection))
    Path(old_files['inheritable_config']).write_text(json.dumps(config.generate_inheritable_config(tap)))
    Path(old_files['selection']).write_text(json.dumps({'selection': Config.generate_selection(tap)}))
    Path(old_files['transformation']).write_text(json.dumps({'transformations': Config.generate_transformations(tap)}))
    runner = _runner(tmp_path)
    with patch.object(runner, '_validate_postgres_rename_source'):
        runner._preserve_renamed_postgres_state(config, ['new_id'])
    assert json.loads(Path(new_files['state']).read_text()) == {'bookmarks': {'table': {'lsn': 123}}}
    Path(new_files['state']).write_text(json.dumps({'bookmarks': {'table': {'lsn': 456}}}))
    with patch.object(runner, '_validate_postgres_rename_source'):
        runner._preserve_renamed_postgres_state(config, ['new_id'])
    assert json.loads(Path(new_files['state']).read_text())['bookmarks']['table']['lsn'] == 456
    assert Path(old_files['state']).exists()


@pytest.mark.parametrize('aliases', [
    {'new': {'type': 'tap-postgres', 'previous_tap_id': 'new'}},
    {'a': {'type': 'tap-postgres', 'previous_tap_id': 'old'},
     'b': {'type': 'tap-postgres', 'previous_tap_id': 'old'}},
    {'a': {'type': 'tap-postgres', 'previous_tap_id': '../old'}},
])
def test_rename_rejects_ambiguous_or_unsafe_aliases(aliases):
    from pipelinewise.cli.errors import InvalidConfigException

    with pytest.raises(InvalidConfigException, match='previous_tap_id'):
        Config.validate_postgres_previous_ids(aliases)


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
    run.assert_called_once_with(expected, timeout=360)

    with patch.object(commands, 'run_command_argv', return_value=[2, '', 'permission denied']):
        with pytest.raises(
            PreRunChecksException,
            match='permission denied.*State and slots are unchanged',
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


@pytest.mark.parametrize('names', [('a-b', 'a_b'), ('a' * 50 + '1', 'a' * 50 + '2')])
def test_import_rejects_ambiguous_historical_slot_ownership(names):
    from pipelinewise.cli.errors import InvalidConfigException

    taps = {
        name: {'id': name, 'type': 'tap-postgres', 'db_conn': {'host': 'source', 'dbname': 'database'},
               'schemas': [{'tables': [{'replication_method': 'LOG_BASED'}]}]}
        for name in names
    }
    with pytest.raises(InvalidConfigException, match='share historical slot'):
        Config.validate_postgres_legacy_slot_collisions(taps)
    Config.validate_postgres_legacy_slot_collisions(taps, ['unrelated_tap'])


def test_new_taps_with_long_database_name_keep_distinct_canonical_slots():
    taps = {
        name: {'id': name, 'type': 'tap-postgres', 'db_conn': {'host': 'source', 'dbname': 'd' * 63},
               'schemas': [{'tables': [{'replication_method': 'LOG_BASED'}]}]}
        for name in ('first', 'second')
    }
    Config.validate_postgres_legacy_slot_collisions(taps)
    slots = {FastSyncTapPostgres.validate_replication_slot_identity('d' * 63, name)[0] for name in taps}
    assert len(slots) == 2
