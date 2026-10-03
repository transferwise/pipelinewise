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
        'version': 2,
        'phase': phase,
        'source_slot': 'pipelinewise_my_db_my_tap',
        'destination_slot': 'ppw_slot_my_tap',
        'slot_lsn': 90,
        'boundary_lsn': 91,
    }
    if phase != 'bridge_pending':
        marker['bridge_lsn'] = 100
    if phase == 'overlap_complete':
        marker['crossover_lsn'] = 120
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
    result = {'destination_slot': 'ppw_slot_new', 'slot_lsn': 100}
    if second_lsn is None or second_lsn <= 100:
        with pytest.raises(PreRunChecksException, match='resync is incomplete'):
            runner._finish_postgres_slot_reset(result)
        assert json.loads(state_path.read_text()) == state
    else:
        runner._finish_postgres_slot_reset(result)
        persisted = json.loads(state_path.read_text())
        assert '_pipelinewise_pgoutput_fresh_start' not in persisted
        assert PGOUTPUT_MIGRATION_STATE_KEY not in persisted


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
            match='permission denied.*Replication slots are unchanged',
        ):
            runner._prepare_postgres_pgoutput_publication()


def test_publication_reconciliation_uses_connector_cli_and_tap_lock(tmp_path):
    """Import reconciliation passes the dedicated CLI flag under the tap lock."""
    runner = _runner(tmp_path, [_logical_stream('public-active')])
    (tmp_path / 'config.json').write_text(
        json.dumps({'dbname': 'my_db', 'tap_id': 'my_tap'}), encoding='utf-8')
    state = tmp_path / 'state.json'
    state.write_text('{}', encoding='utf-8')
    tap = {
        'id': 'my_tap',
        'type': 'tap-postgres',
        'files': {
            **runner.tap['files'],
            'state': str(state),
            'pidfile': str(tmp_path / 'tap.pid'),
        },
    }

    with patch.object(
        runner, 'get_connector_bin', return_value='/venv/bin/tap-postgres'
    ) as get_bin, patch(
        'pipelinewise.cli.pipelinewise.pidfile.PIDFile'
    ) as pid_file, patch.object(
        FastSyncTapPostgres, 'migration_slots_coexist', return_value=False
    ), patch.object(
        commands, 'run_command_argv', return_value=[0, '', '']
    ) as run:
        runner._reconcile_postgres_pgoutput_publication(tap)

    get_bin.assert_called_once_with('tap-postgres')
    pid_file.assert_called_once_with(str(tmp_path / 'tap.pid'))
    run.assert_called_once_with([
        '/venv/bin/tap-postgres',
        '--config',
        str(tmp_path / 'config.json'),
        '--catalog',
        str(tmp_path / 'properties.json'),
        '--prepare-publication',
        '--state',
        str(state),
        '--reconcile-publication',
    ], timeout=360)


def test_final_log_deselection_uses_explicit_connector_retirement_mode(tmp_path):
    """The connector may bypass a frozen selection only after LOG state is gone."""
    runner, files, _ = _retirement_runner(tmp_path, migration=True)
    events = []

    def reconcile(*_args, **kwargs):
        assert kwargs['final_log_deselection'] is True
        assert json.loads(Path(files.state).read_text()) == {
            'bookmarks': {'full': {'xmin': 4, 'version': 5}},
            'currently_syncing': None,
        }
        events.append('publication')

    def retire(_config, *, before_drop):
        before_drop()
        events.append('slots')

    with patch.object(
        runner, '_postgres_pgoutput_migration_is_frozen'
    ) as frozen, patch.object(
        runner, 'get_connector_bin', return_value='/venv/bin/tap-postgres'
    ), patch.object(
        runner, '_run_postgres_publication_preflight', side_effect=reconcile
    ), patch.object(
        FastSyncTapPostgres, 'retire_logical_slots', side_effect=retire
    ):
        runner._reconcile_postgres_pgoutput_publication(runner.tap)

    frozen.assert_not_called()
    assert events == ['publication', 'slots']


def test_import_reconciles_only_successfully_discovered_postgres_taps(tmp_path):
    """Discovery failure or another tap type must not mutate a publication."""
    runner = _runner(tmp_path)
    postgres = {'id': 'pg', 'type': 'tap-postgres', 'files': runner.tap['files']}
    mysql = {'id': 'mysql', 'type': 'tap-mysql'}

    with patch.object(runner, '_reconcile_postgres_pgoutput_publication') as reconcile:
        assert runner._reconcile_postgres_publication_after_discovery(
            'target', postgres, 'discovery failed'
        ) == 'discovery failed'
        assert runner._reconcile_postgres_publication_after_discovery(
            'target', mysql, None
        ) is None
        assert runner._reconcile_postgres_publication_after_discovery(
            'target', postgres, None
        ) is None

    reconcile.assert_called_once_with(postgres)


@pytest.mark.parametrize('discovery_error', [None, 'source unavailable'])
def test_identical_import_retries_unfinished_publication_reconciliation(tmp_path, discovery_error):
    runner = _runner(tmp_path)
    files = Config.get_connector_files(str(tmp_path))
    tap = {'id': 'pg', 'type': 'tap-postgres', 'files': files, 'schemas': []}
    config = SimpleNamespace(targets={'target': {'id': 'target', 'taps': [tap]}})
    Path(files['config']).write_text(json.dumps({'dbname': 'my_db', 'tap_id': 'pg'}))
    Path(files['selection']).write_text(json.dumps({'selection': [
        {'tap_stream_id': 'public-old', 'replication_method': 'LOG_BASED'},
    ]}))
    runner._validate_postgres_migration_import(config, ['pg'])
    runner._mark_pending_postgres_publication_changes(config)
    pending = Path(runner._postgres_publication_pending_path(tap))
    assert pending.exists()
    Path(files['selection']).write_text(json.dumps({'selection': Config.generate_selection(tap)}))

    with patch.object(runner, '_reconcile_postgres_pgoutput_publication', side_effect=RuntimeError('source busy')):
        error = runner._reconcile_postgres_publication_after_discovery('target', tap, discovery_error)
    assert error and pending.exists()

    retry = _runner(tmp_path)
    retry._validate_postgres_migration_import(config, ['pg'])
    with patch.object(retry, '_reconcile_postgres_pgoutput_publication') as reconcile:
        assert retry._reconcile_postgres_publication_after_discovery('target', tap, None) is None
    reconcile.assert_called_once_with(tap)
    assert not pending.exists()

    retry._validate_postgres_migration_import(config, ['pg'])
    with patch.object(retry, '_reconcile_postgres_pgoutput_publication') as reconcile:
        assert retry._reconcile_postgres_publication_after_discovery('target', tap, None) is None
    reconcile.assert_not_called()


def test_partial_logical_deselection_invalidates_only_removed_log_history(tmp_path):
    """Re-adding a removed logical stream must snapshot even before peers advance."""
    inactive = _logical_stream('removed-initial')
    inactive['metadata'][0]['metadata']['selected'] = False
    full = _logical_stream('full')
    full['metadata'][0]['metadata']['replication-method'] = 'FULL_TABLE'
    runner = _runner(tmp_path, [_logical_stream('active'), inactive, full])
    state = {
        'bookmarks': {
            'active': {'lsn': 100, 'version': 1},
            'removed': {'lsn': 100, 'xmin': 12, 'version': 2},
            'removed-initial': {'xmin': 13, 'version': 3},
            'full': {'xmin': 14, 'version': 4},
            'incremental': {'replication_key': 'updated_at', 'replication_key_value': 5},
        },
        'currently_syncing': 'removed',
        PGOUTPUT_MIGRATION_STATE_KEY: _migration_marker(),
    }
    files = _tap_files(tmp_path, state)
    runner.tap['files']['state'] = files.state

    assert runner._invalidate_deselected_postgres_logical_bookmarks(runner.tap) == {'active'}
    persisted = json.loads(Path(files.state).read_text())
    assert persisted['bookmarks'] == {
        key: value for key, value in state['bookmarks'].items()
        if key in {'active', 'full', 'incremental'}
    }
    assert persisted['currently_syncing'] is None
    assert persisted[PGOUTPUT_MIGRATION_STATE_KEY] == _migration_marker()


def _retirement_runner(tmp_path, *, migration=False):
    runner = _runner(tmp_path)
    state = {
        'bookmarks': {'old-log': {'lsn': 100, 'xmin': 3}, 'full': {'xmin': 4, 'version': 5}},
        'currently_syncing': 'old-log',
        '_pipelinewise_pgoutput_fresh_start': {'version': 1},
    }
    if migration:
        state[PGOUTPUT_MIGRATION_STATE_KEY] = _migration_marker()
    files = _tap_files(tmp_path, state)
    runner.tap.update({'type': 'tap-postgres', 'id': 'my_tap'})
    runner.tap['files'].update({'state': files.state, 'pidfile': str(tmp_path / 'tap.pid')})
    return runner, files, state


def test_final_logical_deselection_persists_before_publication_and_slot_mutations(tmp_path):
    """An interruption at either source change cannot preserve old logical history."""
    runner, files, _ = _retirement_runner(tmp_path)
    events = []

    def assert_invalidated():
        assert json.loads(Path(files.state).read_text()) == {
            'bookmarks': {'full': {'xmin': 4, 'version': 5}},
            'currently_syncing': None,
        }

    def prepare(*_args, **_kwargs):
        assert_invalidated()
        events.append('publication')

    def retire(_config, *, before_drop):
        before_drop()
        assert_invalidated()
        events.append('slots')

    with patch.object(runner, 'get_connector_bin'), patch.object(
        FastSyncTapPostgres, 'migration_slots_coexist', return_value=False
    ), patch.object(
        runner, '_run_postgres_publication_preflight', side_effect=prepare
    ), patch.object(FastSyncTapPostgres, 'retire_logical_slots', side_effect=retire):
        runner._reconcile_postgres_pgoutput_publication(runner.tap)
    assert events == ['publication', 'slots']


@pytest.mark.parametrize('persisted_marker', [False, True])
def test_frozen_migration_rejects_partial_deselection_before_state_invalidation(
        tmp_path, persisted_marker):
    """A partial selection rejection preserves retry state before any invalidation."""
    runner, files, original = _retirement_runner(tmp_path, migration=persisted_marker)
    Path(runner.tap['files']['properties']).write_text(
        json.dumps({'streams': [_logical_stream('active')]}), encoding='utf-8')

    with patch.object(
        FastSyncTapPostgres,
        'migration_slots_coexist',
        return_value=True,
    ) as slots_coexist, patch.object(
        runner,
        'get_connector_bin',
        return_value='/venv/bin/tap-postgres',
    ), patch.object(
        runner,
        '_run_postgres_publication_preflight',
        side_effect=PreRunChecksException('publication selection cannot change'),
    ) as prepare, patch.object(
        FastSyncTapPostgres, 'retire_logical_slots'
    ) as retire:
        with pytest.raises(PreRunChecksException, match='selection cannot change'):
            runner._reconcile_postgres_pgoutput_publication(runner.tap)

    if persisted_marker:
        slots_coexist.assert_not_called()
    else:
        slots_coexist.assert_called_once_with({'dbname': 'my_db', 'tap_id': 'my_tap'})
    prepare.assert_called_once()
    retire.assert_not_called()
    assert json.loads(Path(files.state).read_text()) == original


def test_failed_state_invalidation_prevents_publication_and_slot_changes(tmp_path):
    runner, files, original = _retirement_runner(tmp_path)
    with patch(
        'pipelinewise.cli.pipelinewise.fastsync_utils.save_dict_to_json', side_effect=OSError('disk full')
    ), patch.object(
        FastSyncTapPostgres, 'migration_slots_coexist', return_value=False
    ), patch.object(runner, '_run_postgres_publication_preflight') as prepare, patch.object(
        FastSyncTapPostgres, 'retire_logical_slots'
    ) as retire:
        with pytest.raises(OSError, match='disk full'):
            runner._reconcile_postgres_pgoutput_publication(runner.tap)
    prepare.assert_not_called()
    retire.assert_not_called()
    assert json.loads(Path(files.state).read_text()) == original


def test_failed_slot_retirement_retries_with_invalidated_logical_state(tmp_path):
    runner, files, _ = _retirement_runner(tmp_path)
    with patch.object(
        FastSyncTapPostgres, 'migration_slots_coexist', return_value=False
    ), patch.object(runner, 'get_connector_bin'), patch.object(
        runner, '_run_postgres_publication_preflight'
    ), patch.object(
        FastSyncTapPostgres, 'retire_logical_slots', side_effect=[RuntimeError('lost response'), None]
    ) as retire:
        with pytest.raises(RuntimeError, match='lost response'):
            runner._reconcile_postgres_pgoutput_publication(runner.tap)
        assert json.loads(Path(files.state).read_text())['bookmarks'] == {'full': {'xmin': 4, 'version': 5}}
        runner._reconcile_postgres_pgoutput_publication(runner.tap)
    assert retire.call_count == 2


@pytest.mark.parametrize('tap_id', ['orders-full', 'Orders', 'x' * 51])
def test_nonlogical_tap_with_historical_id_skips_canonical_source_cleanup(tmp_path, tap_id):
    """LOG-only naming rules must not prevent import of other replication methods."""
    runner, files, _ = _retirement_runner(tmp_path)
    runner.tap['id'] = tap_id
    with patch.object(runner, '_run_postgres_publication_preflight') as prepare, patch.object(
        FastSyncTapPostgres, 'retire_logical_slots'
    ) as retire:
        runner._reconcile_postgres_pgoutput_publication(runner.tap)
    prepare.assert_not_called()
    retire.assert_not_called()
    assert json.loads(Path(files.state).read_text()) == {
        'bookmarks': {'full': {'xmin': 4, 'version': 5}}, 'currently_syncing': None,
    }


def test_import_reports_publication_reconciliation_failure(tmp_path):
    """A failed reconciliation fails that imported tap with its identity attached."""
    runner = _runner(tmp_path)
    tap = {'id': 'pg', 'type': 'tap-postgres'}

    with patch.object(
        runner,
        '_reconcile_postgres_pgoutput_publication',
        side_effect=PreRunChecksException('reconciliation failed'),
    ):
        assert runner._reconcile_postgres_publication_after_discovery(
            'target', tap, None
        ) == 'target - pg: reconciliation failed'


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


def test_persisted_bridge_is_saved_as_promoted_before_wal2json_is_dropped(tmp_path):
    marker = _migration_marker()
    tap = _tap_files(tmp_path, {
        PGOUTPUT_MIGRATION_STATE_KEY: marker,
        'bookmarks': {'public-one': {'lsn': 100}},
    })
    runner = _runner(tmp_path, [_logical_stream('public-one')])
    updated = {**marker, 'phase': 'pgoutput_overlap'}

    def assert_promoted_was_saved(_config, dropped_marker):
        assert dropped_marker == updated
        saved = json.loads((tmp_path / 'state.json').read_text(encoding='utf-8'))
        assert saved[PGOUTPUT_MIGRATION_STATE_KEY] == updated

    with patch.object(
        FastSyncTapPostgres, 'promote_migrated_replication_slot', return_value=updated
    ) as promote, patch.object(
        FastSyncTapPostgres,
        'drop_promoted_wal2json_slot',
        side_effect=assert_promoted_was_saved,
    ) as drop:
        runner._process_postgres_pgoutput_migration(tap, after_success=False)

    promote.assert_called_once_with({'dbname': 'my_db', 'tap_id': 'my_tap'}, marker)
    drop.assert_called_once_with({'dbname': 'my_db', 'tap_id': 'my_tap'}, updated)
    assert json.loads((tmp_path / 'state.json').read_text(encoding='utf-8')) == {
        PGOUTPUT_MIGRATION_STATE_KEY: updated,
        'bookmarks': {'public-one': {'lsn': 100}},
    }


def test_success_does_not_advance_canonical_slot_while_bridge_boundary_is_pending(tmp_path):
    marker = _migration_marker('bridge_pending')
    state = {
        PGOUTPUT_MIGRATION_STATE_KEY: marker,
        'bookmarks': {'public-one': {'lsn': 95}},
    }
    tap = _tap_files(tmp_path, state)
    runner = _runner(tmp_path, [_logical_stream('public-one')])

    with patch.object(
        FastSyncTapPostgres, 'advance_canonical_replication_slot'
    ) as advance_canonical, patch.object(
        FastSyncTapPostgres, 'promote_migrated_replication_slot'
    ) as promote:
        runner._process_postgres_pgoutput_migration(tap, after_success=True)

    advance_canonical.assert_not_called()
    promote.assert_not_called()
    assert json.loads((tmp_path / 'state.json').read_text(encoding='utf-8')) == state


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
        FastSyncTapPostgres, 'promote_migrated_replication_slot'
    ) as promote, pytest.raises(RuntimeError, match='target-durable bridge boundary'):
        runner._process_postgres_pgoutput_migration(tap, after_success=False)

    promote.assert_not_called()
    assert json.loads((tmp_path / 'state.json').read_text(encoding='utf-8'))[
        PGOUTPUT_MIGRATION_STATE_KEY
    ] == marker


def test_pre_run_retries_idempotent_wal2json_drop_after_promotion(tmp_path):
    marker = _migration_marker('pgoutput_overlap')
    tap = _tap_files(tmp_path, {PGOUTPUT_MIGRATION_STATE_KEY: marker})
    runner = _runner(tmp_path)

    with patch.object(
        FastSyncTapPostgres, 'advance_canonical_replication_slot'
    ) as advance, patch.object(
        FastSyncTapPostgres, 'drop_promoted_wal2json_slot'
    ) as drop:
        runner._process_postgres_pgoutput_migration(tap, after_success=False)

    advance.assert_not_called()
    drop.assert_called_once_with({'dbname': 'my_db', 'tap_id': 'my_tap'}, marker)
    assert json.loads((tmp_path / 'state.json').read_text(encoding='utf-8'))[
        PGOUTPUT_MIGRATION_STATE_KEY
    ] == marker


def test_overlap_completion_advances_to_crossover_and_clears_marker(tmp_path):
    streams = [_logical_stream('public-one'), _logical_stream('public-two')]
    runner = _runner(tmp_path, streams)
    marker = _migration_marker('overlap_complete')
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
        FastSyncTapPostgres, 'drop_promoted_wal2json_slot'
    ) as drop:
        calls.attach_mock(advance, 'advance')
        calls.attach_mock(drop, 'drop')
        runner._process_postgres_pgoutput_migration(tap, after_success=True)

    assert calls.mock_calls == [
        call.drop({'dbname': 'my_db', 'tap_id': 'my_tap'}, marker),
        call.advance({'dbname': 'my_db', 'tap_id': 'my_tap'}, 120),
    ]
    state = json.loads((tmp_path / 'state.json').read_text(encoding='utf-8'))
    assert PGOUTPUT_MIGRATION_STATE_KEY not in state
    assert state['bookmarks']['public-two']['lsn'] == 125


@pytest.mark.parametrize('bookmarks', [
    {'public-one': {'lsn': 130}, 'public-two': {'lsn': 119}},
    {'public-one': {'lsn': 130}},
])
def test_overlap_completion_waits_for_every_logical_bookmark(tmp_path, bookmarks):
    runner = _runner(
        tmp_path, [_logical_stream('public-one'), _logical_stream('public-two')]
    )
    marker = _migration_marker('overlap_complete')
    tap = _tap_files(tmp_path, {
        PGOUTPUT_MIGRATION_STATE_KEY: marker,
        'bookmarks': bookmarks,
    })

    with patch.object(
        FastSyncTapPostgres, 'advance_canonical_replication_slot'
    ) as advance, patch.object(
        FastSyncTapPostgres, 'drop_promoted_wal2json_slot'
    ) as drop, pytest.raises(RuntimeError, match='target-durable crossover boundary'):
        runner._process_postgres_pgoutput_migration(tap, after_success=True)

    advance.assert_not_called()
    drop.assert_not_called()
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
