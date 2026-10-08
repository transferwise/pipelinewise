"""Subprocess log sanitization must preserve replication protocol output."""

import json
import sys
from unittest.mock import patch

import pytest

from pipelinewise.cli import commands, utils


@pytest.mark.parametrize('failed', [False, True])
def test_captured_logs_are_redacted_before_callbacks_and_error_reporting(tmp_path, failed):
    config = tmp_path / 'config with spaces.json'
    config.write_text(json.dumps({'user': 'private-login', 'password': 'private-password'}), encoding='utf-8')
    state = json.dumps({'bookmarks': {'stream': {'value': 'private-password'}}})
    control = 'PIPELINEWISE_CONTROL:{"event":"private-login"}'
    script = tmp_path / 'tap.py'
    script.write_text(
        f'print("CRITICAL login private-login password private-password")\n'
        f'print({state!r})\nprint({control!r})\nraise SystemExit({int(failed)})\n',
        encoding='utf-8',
    )
    received = []

    def callback(line):
        received.append(line)
        return line

    command = f'{sys.executable} "{script}" --config "{config}"'
    logfile = tmp_path / 'tap.log'
    if failed:
        with pytest.raises(commands.RunCommandException) as error:
            commands.run_command(command, str(logfile), callback)
        assert 'private-login' not in str(error.value)
        assert 'private-password' not in str(error.value)
    else:
        status, stdout, stderr = commands.run_command(command, str(logfile), callback)
        assert status == 0 and stderr is None
        assert stdout == ''.join(received)

    assert received == [
        'CRITICAL login [REDACTED] password [REDACTED]\n', state + '\n', control + '\n',
    ]
    suffix = 'failed' if failed else 'success'
    assert logfile.with_suffix('.log.' + suffix).read_text(encoding='utf-8') == ''.join(received)


def test_discovery_stdout_is_preserved_while_stderr_is_redacted(tmp_path):
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'username': 'private-login', 'password': 'private-password'}), encoding='utf-8')
    catalog = json.dumps({'streams': [{'stream': 'private-login'}]})
    script = tmp_path / 'tap.py'
    script.write_text(
        f'import sys\nprint({catalog!r})\n'
        'print("CRITICAL user private-login password private-password", file=sys.stderr)\nraise SystemExit(1)\n',
        encoding='utf-8',
    )
    status, stdout, stderr = commands.run_command(f'{sys.executable} "{script}" --config "{config}"')
    assert status == 1
    assert stdout == catalog + '\n'
    assert stderr == 'CRITICAL user [REDACTED] password [REDACTED]\n'


def test_fastsync_tap_and_target_configs_protect_logs(tmp_path):
    tap_config, target_config = tmp_path / 'tap config.json', tmp_path / 'target config.json'
    tap_config.write_text(json.dumps({'user': 'source-user', 'password': 1}), encoding='utf-8')
    target_config.write_text(json.dumps({
        'user': 'target-user', 'private_key_passphrase': 'target-secret-passphrase',
    }), encoding='utf-8')
    script = tmp_path / 'fastsync.py'
    script.write_text(
        'print("source.example user=source-user password=1 rows=1")\n'
        'print("user=target-user private_key_passphrase=target-secret-passphrase")\n', encoding='utf-8',
    )
    received = []

    def capture(line):
        received.append(line)
        return line

    commands.run_command(
        f'{sys.executable} "{script}" --tap "{tap_config}" --target "{target_config}"',
        str(tmp_path / 'fastsync.log'), capture,
    )
    assert received == [
        'source.example user=[REDACTED] password=[REDACTED] rows=1\n',
        'user=[REDACTED] private_key_passphrase=[REDACTED]\n',
    ]


def test_short_authentication_secrets_are_redacted_without_changing_replication_state(tmp_path):
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'password': 1, 'access_token': 'abc'}), encoding='utf-8')
    state = json.dumps({'bookmarks': {'stream': {'value': 'abc', 'position': 1}}})
    script = tmp_path / 'tap.py'
    script.write_text(
        'print("Authorization: Bearer abc")\n'
        'print("RuntimeError: authentication failed (1); dbname=abc rows=1")\n'
        f'print({state!r})\n', encoding='utf-8',
    )
    logfile = tmp_path / 'tap.log'
    status, stdout, stderr = commands.run_command(f'{sys.executable} "{script}" --config "{config}"', str(logfile))
    expected = ('Authorization: Bearer [REDACTED]\n'
                'RuntimeError: authentication failed ([REDACTED]); dbname=abc rows=1\n' + state + '\n')
    assert status == 0 and stderr is None
    assert stdout == expected
    assert logfile.with_suffix('.log.success').read_text(encoding='utf-8') == expected


def test_plain_logs_are_not_parsed_as_state_messages():
    with patch.object(utils.json, 'loads') as loads:
        assert not utils.is_state_message('Connecting to PostgreSQL source host: source.example\n')
    loads.assert_not_called()
    assert utils.is_state_message('  {"bookmarks": {"stream": {"value": 1}}}')
