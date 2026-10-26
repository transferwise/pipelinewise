"""Signal-driven exits must keep the output children write while shutting down."""

import sys
import time

import pytest

from pipelinewise.cli import commands


def _interrupting_callback(received):
    def callback(line):
        received.append(line)
        if line == 'started\n':
            raise SystemExit(1)
        return line

    return callback


def _run_interrupted(tmp_path, script_body):
    script = tmp_path / 'child.py'
    script.write_text(script_body, encoding='utf-8')
    received = []
    with pytest.raises(SystemExit):
        commands.run_command(
            f'{sys.executable} "{script}"', str(tmp_path / 'tap.log'), _interrupting_callback(received)
        )
    return received, (tmp_path / 'tap.log.running').read_text(encoding='utf-8')


def test_shutdown_output_is_logged_after_signal_exit(tmp_path):
    received, logged = _run_interrupted(
        tmp_path,
        'import sys, time\n'
        'print("started", flush=True)\n'
        'time.sleep(0.2)\n'
        'print("Killed source session 42.")\n'
        'sys.stdout.write("partial line")\n',
    )

    assert received == ['started\n', 'Killed source session 42.\n', 'partial line']
    assert logged == 'Killed source session 42.\npartial line'


def test_shutdown_drain_stops_at_timeout(tmp_path, monkeypatch):
    monkeypatch.setattr(commands, 'SHUTDOWN_OUTPUT_DRAIN_SECONDS', 0.1)
    start = time.monotonic()

    received, logged = _run_interrupted(
        tmp_path,
        'import time\n'
        'print("started", flush=True)\n'
        'print("early", flush=True)\n'
        'time.sleep(1)\n'
        'print("late", flush=True)\n',
    )

    assert received == ['started\n', 'early\n']
    assert logged == 'early\n'
    assert time.monotonic() - start < 5
