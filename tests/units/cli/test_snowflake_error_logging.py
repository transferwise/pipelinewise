"""Data-bearing Snowflake errors must not escape through logs and alerts."""

import json
import sys

from unittest.mock import Mock

import pytest

from pipelinewise.cli import commands
from pipelinewise.cli.alert_handlers.slack_alert_handler import SlackAlertHandler


@pytest.mark.parametrize('separator', [' ', '\n', '\r\n'])
def test_sanitized_snowflake_failure_protects_cli_log_callback_and_slack(tmp_path, separator):
    marker = 'SYNTHETIC_ROW_VALUE_CANARY'
    state = json.dumps({'bookmarks': {'stream': {'value': 'Row Values: protocol bookmark'}}})
    control = 'PIPELINEWISE_CONTROL:' + json.dumps({'event': 'Row Values: protocol control'})
    query_id = '01234567-89ab-cdef-0123-456789abcdef'
    script = tmp_path / 'snowflake_error.py'
    script.write_text(
        'from singer.snowflake_errors import sanitize_snowflake_errors\n'
        'from snowflake.connector.errors import ProgrammingError\n'
        f'marker = {marker!r}\nseparator = {separator!r}\n'
        f'print({state!r}, flush=True)\nprint({control!r}, flush=True)\n'
        '@sanitize_snowflake_errors\n'
        'def execute():\n'
        '    message = "Duplicate key " + marker + separator + "Row Values: [" + marker + "]"\n'
        f'    error = ProgrammingError(msg=message, errno=100090, sqlstate="42P18", sfqid={query_id!r})\n'
        '    error.add_note(marker)\n'
        '    raise error from ValueError(marker)\n'
        'execute()\n',
        encoding='utf-8',
    )
    received = []
    logfile = tmp_path / 'tap.log'
    with pytest.raises(commands.RunCommandException) as caught:
        commands.run_command(
            f'{sys.executable} "{script}"', str(logfile),
            lambda line: received.append(line) or line,
        )
    failed_log = logfile.with_suffix('.log.failed').read_text(encoding='utf-8')
    handler = SlackAlertHandler.__new__(SlackAlertHandler)
    handler.channel = '#synthetic-channel'
    handler.data_diff_channel = None
    handler.client = Mock()
    handler.send('Synthetic tap failure', exc=caught.value)
    slack_text = handler.client.chat_postMessage.call_args.kwargs['text']

    assert received[:2] == [state + '\n', control + '\n']
    assert marker not in failed_log + ''.join(received) + str(caught.value) + slack_text
    for diagnostic in (failed_log, str(caught.value), slack_text):
        assert '100090 (42P18)' in diagnostic
        assert '[REDACTED]' in diagnostic
        assert query_id in diagnostic
