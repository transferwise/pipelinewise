"""Retry explicit tap disconnects while preserving durable state and combined logs."""

import ast
import io
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest

from pipelinewise.cli import commands
from pipelinewise.cli.pipelinewise import (
    MYSQL_BINLOG_DISCONNECT_CONTROL_PREFIX,
    MYSQL_BINLOG_DISCONNECT_MARKER,
    MYSQL_BINLOG_DISCONNECT_MAX_ATTEMPTS,
    MYSQL_BINLOG_DISCONNECT_RETRY_DELAY_SECONDS,
    PipelineWise,
    _is_retryable_mysql_disconnect,
    _persist_singer_state,
)
from pipelinewise.fastsync.commons import utils as fastsync_utils


DISCONNECT_MESSAGE = 'Binlog connection lost; restart replication from the durable checkpoint.'


def marker_line(overrides=None):
    marker = {**MYSQL_BINLOG_DISCONNECT_MARKER, **(overrides or {})}
    return (
        MYSQL_BINLOG_DISCONNECT_CONTROL_PREFIX
        + json.dumps(marker, separators=(',', ':'))
        + '\n'
    )


def runner(tmp_path):
    pipelinewise = object.__new__(PipelineWise)
    pipelinewise.tap_run_log_file = str(tmp_path / 'run.log')
    pipelinewise.profiling_mode = False
    pipelinewise.profiling_dir = None
    pipelinewise.extra_log = False
    pipelinewise.logger = Mock()
    return pipelinewise


def params(state_path, tap_type='tap-mysql'):
    return SimpleNamespace(type=tap_type, state=str(state_path)), Mock(), Mock()


def test_disconnect_rebuilds_command_from_latest_durable_state(tmp_path):
    pipelinewise = runner(tmp_path)
    state_path = tmp_path / 'state.json'
    tap, target, transform = params(state_path)
    seen_states = []

    def build_command(**_kwargs):
        seen_states.append(json.loads(state_path.read_text()) if state_path.exists() else None)
        return f'command-{len(seen_states)}'

    def run_command(command, _log_file, line_callback):
        if command == 'command-1':
            line_callback('{"bookmarks":{"db-items":{"log_pos":100}}}\n')
            line_callback('{"bookmarks":{"db-items":{"log_pos":123}}}\n')
            line_callback(marker_line())
            raise commands.RunCommandException('summary omitted the disconnect')

    with patch.object(commands, 'build_singer_command', side_effect=build_command), \
            patch.object(commands, 'run_command', side_effect=run_command) as run, \
            patch('pipelinewise.cli.pipelinewise.time', return_value=0), \
            patch('pipelinewise.cli.pipelinewise.sleep') as retry_sleep:
        pipelinewise.run_tap_singer(tap, target, transform)

    assert seen_states == [None, {'bookmarks': {'db-items': {'log_pos': 123}}}]
    assert [invocation.args[0] for invocation in run.call_args_list] == ['command-1', 'command-2']
    retry_sleep.assert_called_once_with(MYSQL_BINLOG_DISCONNECT_RETRY_DELAY_SECONDS)


def test_disconnect_stops_after_two_retries_and_retains_latest_durable_state(tmp_path):
    pipelinewise = runner(tmp_path)
    state_path = tmp_path / 'state.json'
    tap, target, transform = params(state_path)
    attempt = 0

    def run_command(_command, _log_file, line_callback):
        nonlocal attempt
        attempt += 1
        line_callback(json.dumps({'bookmarks': {'db-items': {'log_pos': attempt * 100}}}))
        line_callback(marker_line())
        raise commands.RunCommandException(DISCONNECT_MESSAGE)

    with patch.object(commands, 'build_singer_command', return_value='command') as build, \
            patch.object(commands, 'run_command', side_effect=run_command) as run, \
            patch('pipelinewise.cli.pipelinewise.sleep') as retry_sleep:
        with pytest.raises(commands.RunCommandException, match=DISCONNECT_MESSAGE):
            pipelinewise.run_tap_singer(tap, target, transform)

    assert build.call_count == run.call_count == MYSQL_BINLOG_DISCONNECT_MAX_ATTEMPTS
    assert retry_sleep.call_args_list == [call(MYSQL_BINLOG_DISCONNECT_RETRY_DELAY_SECONDS)] * 2
    assert json.loads(state_path.read_text()) == {'bookmarks': {'db-items': {'log_pos': 300}}}


class ProcessOutput:
    """Supply complete subprocess output without invoking a shell."""

    def __init__(self, output, returncode):
        self.stdout = io.BytesIO(output.encode('utf-8'))
        self.returncode = returncode

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.stdout.close()

    def wait(self):
        return self.returncode

    def poll(self):
        return self.returncode


@pytest.mark.parametrize('terminal_status', ['success', 'failed'])
def test_one_combined_terminal_log_and_no_retry_from_an_earlier_attempt_marker(tmp_path, terminal_status):
    pipelinewise = runner(tmp_path)
    tap, target, transform = params(tmp_path / 'state.json')
    processes = [
        ProcessOutput('first attempt\n' + marker_line(), 1),
        ProcessOutput('last attempt\nERROR unrelated failure\n', int(terminal_status == 'failed')),
    ]
    with patch.object(commands, 'build_singer_command', return_value='command'), \
            patch.object(commands, 'Popen', side_effect=processes) as popen, \
            patch('pipelinewise.cli.pipelinewise.sleep') as retry_sleep:
        if terminal_status == 'failed':
            with pytest.raises(commands.RunCommandException):
                pipelinewise.run_tap_singer(tap, target, transform)
        else:
            pipelinewise.run_tap_singer(tap, target, transform)

    assert popen.call_count == 2
    retry_sleep.assert_called_once()
    assert list(tmp_path.iterdir()) == [tmp_path / f'run.log.{terminal_status}']
    combined_log = (tmp_path / f'run.log.{terminal_status}').read_text()
    assert 'first attempt' in combined_log
    assert 'last attempt' in combined_log
    assert 'Retrying Singer pipeline: attempt 2 of 3' in combined_log
    assert marker_line() in combined_log


@pytest.mark.parametrize('human_log', [
    'CRITICAL tap_mysql - network dropped',
    'logger_name=tap_mysql log_level=CRITICAL message=network dropped',
    '{"logger":"tap_mysql","level":"critical","message":"network dropped"}',
])
def test_retry_marker_is_independent_of_logging_format(tmp_path, human_log):
    pipelinewise = runner(tmp_path)
    tap, target, transform = params(tmp_path / 'state.json')

    def run_command(command, _log_file, line_callback):
        if command == 'first':
            line_callback(human_log)
            line_callback(marker_line())
            raise commands.RunCommandException('No recognizable message or failed log')

    with patch.object(commands, 'build_singer_command', side_effect=['first', 'last']), \
            patch.object(commands, 'run_command', side_effect=run_command) as run, \
            patch('pipelinewise.cli.pipelinewise.sleep'):
        pipelinewise.run_tap_singer(tap, target, transform)
    assert run.call_count == 2


@pytest.mark.parametrize('tap_type,output,error', [
    ('tap-postgres', marker_line(), DISCONNECT_MESSAGE),
    ('tap-mysql', '', DISCONNECT_MESSAGE),
    ('tap-mysql', DISCONNECT_MESSAGE, DISCONNECT_MESSAGE),
    ('tap-mysql', 'logger_name=tap_mysql log_level=CRITICAL message=' + DISCONNECT_MESSAGE, DISCONNECT_MESSAGE),
    ('tap-mysql', '', 'unrelated singer failure'),
])
def test_human_messages_exception_text_and_other_taps_cannot_trigger_retry(tmp_path, tap_type, output, error):
    pipelinewise = runner(tmp_path)
    tap, target, transform = params(tmp_path / 'state.json', tap_type)
    failure = commands.RunCommandException(error)

    def run_command(_command, _log_file, line_callback):
        line_callback(output)
        raise failure

    with patch.object(commands, 'build_singer_command', return_value='command') as build, \
            patch.object(commands, 'run_command', side_effect=run_command) as run, \
            patch('pipelinewise.cli.pipelinewise.sleep') as retry_sleep:
        with pytest.raises(commands.RunCommandException) as raised:
            pipelinewise.run_tap_singer(tap, target, transform)
    assert raised.value is failure
    build.assert_called_once()
    run.assert_called_once()
    retry_sleep.assert_not_called()


@pytest.mark.parametrize('overrides', [
    {'version': True}, {'version': 1.0}, {'version': 2}, {'component': 'target-snowflake'}, {'event': 'other'},
])
def test_only_the_exact_versioned_control_marker_is_retryable(overrides):
    assert not _is_retryable_mysql_disconnect('tap-mysql', marker_line(overrides))


def test_control_marker_can_follow_an_unterminated_output_fragment():
    assert _is_retryable_mysql_disconnect(
        'tap-mysql',
        'unterminated upstream output' + marker_line(),
    )


def test_lines_without_the_control_prefix_are_not_json_parsed():
    with patch('pipelinewise.cli.pipelinewise.json.loads') as loads:
        assert not _is_retryable_mysql_disconnect(
            'tap-mysql',
            '{"bookmarks":{"db-items":{"log_pos":123}}}',
        )
    loads.assert_not_called()


def test_tap_and_orchestrator_share_the_control_protocol():
    tap_root = Path(__file__).resolve().parents[3] / 'singer-connectors/tap-mysql/tap_mysql'
    module = ast.parse((tap_root / 'connection.py').read_text())
    marker = next(node.value for node in module.body if isinstance(node, ast.Assign)
                  and any(isinstance(target, ast.Name) and target.id == 'MYSQL_BINLOG_DISCONNECT_MARKER'
                          for target in node.targets))
    assert ast.literal_eval(marker) == MYSQL_BINLOG_DISCONNECT_MARKER
    prefix = next(node.value for node in module.body if isinstance(node, ast.Assign)
                  and any(isinstance(target, ast.Name)
                          and target.id == 'MYSQL_BINLOG_DISCONNECT_CONTROL_PREFIX'
                          for target in node.targets))
    assert ast.literal_eval(prefix) == MYSQL_BINLOG_DISCONNECT_CONTROL_PREFIX


def test_frequent_singer_state_saves_use_debug_but_other_atomic_saves_keep_info(tmp_path):
    singer_path = str(tmp_path / 'singer.json')
    other_path = str(tmp_path / 'other.json')
    with patch.object(fastsync_utils.LOGGER, 'log') as log:
        _persist_singer_state(singer_path, '{"bookmarks": {}}')
        fastsync_utils.save_dict_to_json(other_path, {'bookmarks': {}})
    assert log.call_args_list == [
        call(logging.DEBUG, 'Saving new state file to %s', singer_path),
        call(logging.INFO, 'Saving new state file to %s', other_path),
    ]
    assert json.loads((tmp_path / 'singer.json').read_text()) == {'bookmarks': {}}
