"""Iceberg FastSync refuses native targets before any source or state change."""

import os
import signal
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


def test_native_target_stops_slot_reset_before_any_change(pipelinewise):
    """An unfiltered PostgreSQL resync keeps its slot and bookmarks when a target is native."""
    selected = {'full_sync': ['public.orders'], 'partial_sync': {'public.events': {'column': 'id'}}}
    with patch.object(
        pipelinewise, '_get_sync_tables_setting_from_selection_file', return_value=selected,
    ), patch.object(
        pipelinewise, '_run_in_child_process', side_effect=lambda target, *args: target(*args),
    ), patch.object(
        pipelinewise, '_iceberg_target_config', return_value=TARGET_CONFIG,
    ), patch(
        'pipelinewise.cli.native_to_iceberg.assert_fastsync_targets_ready', side_effect=NOT_READY,
    ) as ready, patch.object(
        pipelinewise, '_preflight_postgres_slot_reset',
    ) as slot_preflight, patch(
        'pipelinewise.cli.pipelinewise.FastSyncTapPostgres.reset_slot',
    ) as reset_slot, patch('pipelinewise.cli.pipelinewise.Process') as process:
        with pytest.raises(PreRunChecksException, match='No source or state changes were made'):
            pipelinewise.do_sync_tables(reset_postgres_slot=True)

    ready.assert_called_once_with(
        TARGET_CONFIG,
        f'{CONFIG_DIR}/target_one',
        ['public.events', 'public.orders'],
    )
    slot_preflight.assert_not_called()
    reset_slot.assert_not_called()
    process.assert_not_called()
    assert not os.path.exists(pipelinewise.tap['files']['state'])


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


def test_child_process_exit_status_is_preserved(pipelinewise):
    """A child that exits without an exception still fails the parent."""
    with pytest.raises(SystemExit) as error:
        pipelinewise._run_in_child_process(_exit_with, 3)

    assert error.value.code == 3


def test_child_installs_command_signal_handlers(pipelinewise):
    """A child never inherits the handler that deletes the tap pidfile."""
    target = MagicMock()
    with patch('pipelinewise.cli.pipelinewise.signal.signal') as set_signal:
        pipelinewise._run_with_command_signals(target, 'argument')

    handler = pipelinewise._stop_command_on_signal
    assert set_signal.call_args_list == [call(signal.SIGINT, handler), call(signal.SIGTERM, handler)]
    target.assert_called_once_with('argument')
