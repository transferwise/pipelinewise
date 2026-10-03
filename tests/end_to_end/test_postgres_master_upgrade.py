"""Upgrade real wal2json state from the pinned pre-migration master tap."""

import json
import os
import subprocess
import threading
from pathlib import Path

from .test_postgres_pgoutput_slots import (
    MIGRATION_STATE_KEY, SOURCE_SCHEMA, STREAM_ID, TABLE_NAME, TAP_ID, TARGET_ID,
    _assert_promoted_overlap, _finish_overlap, _read_state, _read_state_lsn,
    _run, _run_success, _simple_migration_case, _slot_status, _source_target_rows,
)


def _persist_target_acknowledgements(output, state_path, errors):
    """Expose only target-emitted state to the old tap's feedback reader."""
    try:
        for line in output:
            state = json.loads(line)
            pending = state_path.with_suffix('.master-ack.tmp')
            with pending.open('w', encoding='utf-8') as state_file:
                json.dump(state, state_file)
                state_file.flush()
                os.fsync(state_file.fileno())
            pending.replace(state_path)
    except Exception as error:
        errors.append(error)


def _run_master_tap(case, *, idle_feedback=False):
    """Run the complete old package with a real target and live state persistence."""
    home = Path(os.environ['PIPELINEWISE_HOME'])
    baseline = home / '.upgrade-baseline'
    assert (baseline / 'tap_postgres' / '__init__.py').is_file(), (
        'Prepare the pinned master archive as documented in tests/end_to_end/AGENTS.md'
    )
    tap_dir = case.state_path.parent
    old_config = _read_state(tap_dir / 'config.json')
    old_config.update(
        break_at_end_lsn=not idle_feedback,
        max_run_seconds=35 if idle_feedback else 20,
        logical_poll_total_seconds=40 if idle_feedback else 10,
    )
    old_config_path = tap_dir / 'master-config.json'
    old_config_path.write_text(json.dumps(old_config), encoding='utf-8')
    target_config = _read_state(case.config_dir / TARGET_ID / 'config.json')
    target_config.update(_read_state(tap_dir / 'inheritable_config.json'))
    target_config['batch_size_rows'] = 1
    target_config_path = tap_dir / 'master-target-config.json'
    target_config_path.write_text(json.dumps(target_config), encoding='utf-8')
    if not case.state_path.exists():
        case.state_path.write_text('{}', encoding='utf-8')
    tap_command = [
        str(home / '.virtualenvs/tap-postgres/bin/python'), '-c',
        'import pathlib, sys, tap_postgres; '
        'assert pathlib.Path(tap_postgres.__file__).is_relative_to(pathlib.Path(sys.argv.pop(1))); '
        'tap_postgres.main()',
        str(baseline), '--config', str(old_config_path), '--catalog', str(tap_dir / 'properties.json'),
        '--state', str(case.state_path),
    ]
    target_command = [str(home / '.virtualenvs/target-postgres/bin/target-postgres'),
                      '--config', str(target_config_path)]
    errors = []
    with (tap_dir / 'master-tap.log').open('w+') as tap_log, \
            (tap_dir / 'master-target.log').open('w+') as target_log:
        tap = subprocess.Popen(tap_command, stdout=subprocess.PIPE, stderr=tap_log,
                               env={**case.command_env, 'PYTHONPATH': str(baseline)})
        target = None
        reader = None
        try:
            target = subprocess.Popen(target_command, stdin=tap.stdout, stdout=subprocess.PIPE,
                                      stderr=target_log, text=True, env=case.command_env)
            tap.stdout.close()
            reader = threading.Thread(target=_persist_target_acknowledgements,
                                      args=(target.stdout, case.state_path, errors), daemon=True)
            reader.start()
            tap.wait(timeout=65)
            target.wait(timeout=30)
        finally:
            for process in (tap, target):
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
            if reader is not None:
                reader.join(timeout=10)
        tap_log.seek(0)
        target_log.seek(0)
        assert tap.returncode == 0, tap_log.read()
        assert target.returncode == 0, target_log.read()
        assert not reader.is_alive()
        assert not errors, errors
    assert MIGRATION_STATE_KEY not in _read_state(case.state_path)
    assert _slot_status(case.e2e, case.pgoutput_slot) is None
    assert _source_target_rows(case.e2e)[0] == _source_target_rows(case.e2e)[1]


def test_master_snapshot_and_cdc_upgrade_to_pgoutput(tmp_path):
    with _simple_migration_case(tmp_path, bootstrap_fastsync=False) as case:
        _run_master_tap(case)
        case.e2e.run_query_tap_postgres(
            f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET status = 'old tap CDC' WHERE id = 1; "
            f"INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} VALUES (2, 'old tap insert', 'payload-2')"
        )
        _run_master_tap(case)
        bookmark = _read_state_lsn(case.state_path)
        assert _slot_status(case.e2e, case.wal2json_slot)['confirmed_flush_lsn'] <= bookmark
        case.e2e.run_query_tap_postgres(
            f"UPDATE {SOURCE_SCHEMA}.{TABLE_NAME} SET status = 'bridge update' WHERE id = 1; "
            f'DELETE FROM {SOURCE_SCHEMA}.{TABLE_NAME} WHERE id = 2; '
            f"INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} VALUES (3, 'bridge insert', 'payload-3')"
        )
        _run_success(case.run_command, case.command_env)
        _assert_promoted_overlap(case)
        _finish_overlap(case)
        assert _read_state_lsn(case.state_path) >= bookmark
        assert _source_target_rows(case.e2e)[0] == _source_target_rows(case.e2e)[1]


def test_master_idle_keepalive_state_is_rejected_then_whole_tap_resync_recovers(tmp_path):
    with _simple_migration_case(tmp_path, bootstrap_fastsync=False) as case:
        _run_master_tap(case)
        _run_master_tap(case, idle_feedback=True)
        before = _read_state(case.state_path)
        slot = _slot_status(case.e2e, case.wal2json_slot)
        assert slot['confirmed_flush_lsn'] > before['bookmarks'][STREAM_ID]['lsn']

        result = _run(case.run_command, case.command_env)
        assert result.returncode != 0
        assert 'predates legacy slot' in result.stdout + result.stderr
        assert _read_state(case.state_path) == before
        assert _slot_status(case.e2e, case.wal2json_slot) == slot

        _run_success(['pipelinewise', 'fast_sync', '--tap', TAP_ID, '--target', TARGET_ID], case.command_env)
        assert _slot_status(case.e2e, case.wal2json_slot) is None
        assert _slot_status(case.e2e, case.pgoutput_slot)['plugin'] == 'pgoutput'
        assert MIGRATION_STATE_KEY not in _read_state(case.state_path)
        case.e2e.run_query_tap_postgres(
            f"INSERT INTO {SOURCE_SCHEMA}.{TABLE_NAME} VALUES (2, 'after recovery', 'payload-2')"
        )
        _run_success(case.run_command, case.command_env)
        assert _source_target_rows(case.e2e)[0] == _source_target_rows(case.e2e)[1]
