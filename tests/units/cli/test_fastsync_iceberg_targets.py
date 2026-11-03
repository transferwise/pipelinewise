"""Iceberg FastSync refuses native targets before any source or state change."""

import json
import multiprocessing
import os
import signal
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from pipelinewise.cli.errors import PreRunChecksException
from pipelinewise.cli.pipelinewise import PipelineWise
from tests.units.cli.cli_args import CliArgs


CONFIG_DIR = 'tests/units/cli/resources/sample_json_config'
VIRTUALENVS_DIR = './virtualenvs-dummy'
TARGET_CONFIG = {'dbname': 'database', 'default_target_schema': 'schema'}
NOT_READY = PreRunChecksException(
    'FastSync can only publish to new or Iceberg tables: DATABASE.SCHEMA.ORDERS is native. '
    'No source or state changes were made.'
)


def _raise_pre_run_checks(message):
    raise PreRunChecksException(message)


def _exit_with(code):
    os._exit(code)


@pytest.fixture(name='pipelinewise')
def _pipelinewise(tmp_path):
    instance = PipelineWise(CliArgs(target='target_one', tap='tap_one'), CONFIG_DIR, VIRTUALENVS_DIR)
    instance.tap['type'] = 'tap-postgres'
    instance.tap['target_table_format'] = 'iceberg'
    instance.tap['iceberg_version'] = 3
    instance.tap['files']['state'] = str(tmp_path / 'state.json')
    return instance


def test_native_target_stops_slot_reset_before_any_change(pipelinewise, tmp_path):
    """An unfiltered PostgreSQL resync keeps its slot and bookmarks when a target is native."""
    selected = {'full_sync': ['public.orders'], 'partial_sync': {'public.events': {'column': 'id'}}}
    state = {'bookmarks': {'public-orders': {'lsn': 42}, 'public-events': {'id': 7}}, 'currently_syncing': None}
    state_path = Path(pipelinewise.tap['files']['state'])
    state_path.write_text(json.dumps(state), encoding='utf-8')
    with patch.object(
        pipelinewise, '_get_sync_tables_setting_from_selection_file', return_value=selected,
    ), patch.object(
        pipelinewise, '_run_in_child_process', side_effect=lambda target, *args: target(*args),
    ), patch.object(
        pipelinewise, '_iceberg_target_config', return_value=TARGET_CONFIG,
    ), patch(
        'pipelinewise.cli.native_to_iceberg.assert_fastsync_targets_ready', side_effect=NOT_READY,
    ) as ready, patch.object(
        pipelinewise, '_preflight_source_slot_reset',
    ) as slot_preflight, patch(
        'pipelinewise.cli.pipelinewise.FastSyncTapPostgres.reset_slot',
    ) as reset_slot, patch('pipelinewise.cli.pipelinewise.Process') as process:
        with pytest.raises(PreRunChecksException, match='No source or state changes were made'):
            pipelinewise.do_sync_tables(reset_source_slot=True)

    ready.assert_called_once_with(
        TARGET_CONFIG,
        f'{CONFIG_DIR}/target_one',
        ['public.events', 'public.orders'],
    )
    slot_preflight.assert_not_called()
    reset_slot.assert_not_called()
    process.assert_not_called()
    assert json.loads(state_path.read_text(encoding='utf-8')) == state
    assert sorted(path.name for path in tmp_path.iterdir()) == ['state.json']


@pytest.mark.parametrize(
    ('table_format', 'target_type'),
    [(None, 'target-snowflake'), ('native', 'target-snowflake'), ('iceberg', 'target-postgres')],
)
def test_check_only_applies_to_snowflake_iceberg_taps(pipelinewise, table_format, target_type):
    """Native and non-Snowflake routes keep their existing behaviour."""
    pipelinewise.tap['target_table_format'] = table_format
    pipelinewise.target['type'] = target_type

    with patch.object(pipelinewise, '_run_in_child_process') as run_child:
        pipelinewise._check_fastsync_targets_are_iceberg({'full_sync': ['public.orders'], 'partial_sync': {}})

    run_child.assert_not_called()


def test_child_process_errors_are_raised_in_the_parent(pipelinewise):
    """The parent re-raises the child's own exception type and message."""
    with pytest.raises(PreRunChecksException, match='child refused'):
        pipelinewise._run_in_child_process(_raise_pre_run_checks, 'child refused')


def test_child_process_exit_without_a_result_fails_the_parent(pipelinewise):
    """A child that dies without reporting still fails the parent with its exit status."""
    with pytest.raises(ChildProcessError, match='exited with status 3 without reporting a result'):
        pipelinewise._run_in_child_process(_exit_with, 3)


def test_child_that_exits_cleanly_without_reporting_fails(pipelinewise):
    """A child that exits 0 without reporting is a failure, never a silent success."""
    with pytest.raises(ChildProcessError, match='exited with status 0 without reporting a result'):
        pipelinewise._run_in_child_process(_exit_with, 0)


def _deadlocked(*_):
    pytest.fail('the parent deadlocked waiting for a child blocked on the pipe')


def test_large_child_errors_do_not_deadlock(pipelinewise):
    """An error larger than the pipe buffer is read while the parent waits for the child."""
    # Forking from the main thread avoids copying a multi-threaded process; a timer bounds the wait.
    previous = signal.signal(signal.SIGALRM, _deadlocked)
    outer_timer = signal.setitimer(signal.ITIMER_REAL, 60)
    try:
        with pytest.raises(PreRunChecksException) as error:
            pipelinewise._run_in_child_process(_raise_pre_run_checks, 'x' * 2_000_000)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
        signal.setitimer(signal.ITIMER_REAL, *outer_timer)
        for child in multiprocessing.active_children():  # a deadlocked child would block interpreter exit
            child.kill()
            child.join()

    assert len(str(error.value)) == 2_000_000


def test_child_installs_command_signal_handlers_before_its_work(pipelinewise):
    """A child installs its own exit handlers before any work, never running under the tap's stop handler."""
    calls = MagicMock()
    with patch('pipelinewise.cli.pipelinewise.signal.signal', calls.set_signal):
        pipelinewise._run_with_command_signals(calls.target, 'argument')

    handler = pipelinewise._stop_command_on_signal
    assert calls.mock_calls == [
        call.set_signal(signal.SIGINT, handler),
        call.set_signal(signal.SIGTERM, handler),
        call.target('argument'),
    ]


def _report_sigterm_handler():
    handler = signal.getsignal(signal.SIGTERM)
    raise PreRunChecksException(getattr(handler, '__name__', repr(handler)))


def test_child_work_runs_under_command_signal_handlers(pipelinewise):
    """The real child, not only the wrapper, runs its work under the pidfile-safe handler."""
    with pytest.raises(PreRunChecksException, match='^_stop_command_on_signal$'):
        pipelinewise._run_in_child_process(_report_sigterm_handler)
