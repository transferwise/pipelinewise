"""stop_tap must stop the running tap's process tree whether it runs as a command or a signal handler."""

import os
import signal
import subprocess
import sys
from unittest.mock import PropertyMock, patch

import pidfile
import pytest

from pipelinewise.cli import pipelinewise as pipelinewise_module
from pipelinewise.cli.pipelinewise import PipelineWise
from tests.units.cli.cli_args import CliArgs
from tests.units.cli.test_cli import CONFIG_DIR, VIRTUALENVS_DIR
from tests.units.process_helpers import wait_until_gone


def _pipelinewise(tmp_path):
    ppw = PipelineWise(CliArgs(target='target_one', tap='tap_one'), CONFIG_DIR, VIRTUALENVS_DIR)
    ppw.tap['files']['pidfile'] = str(tmp_path / 'pipelinewise.pid')
    return ppw


def _write_pidfile(ppw, pid):
    with open(ppw.tap['files']['pidfile'], 'w', encoding='utf-8') as pid_file:
        pid_file.write(str(pid))


def _spawn(code, *args):
    return subprocess.Popen([sys.executable, '-c', code, *args], stdout=subprocess.PIPE)


def _recorded_tap_is_running(is_running):
    return patch.object(pidfile.PIDFile, 'is_running', new_callable=PropertyMock, return_value=is_running)


def test_signal_without_pidfile_still_stops_children(tmp_path):
    ppw = _pipelinewise(tmp_path)
    child = _spawn('import time; time.sleep(60)')
    try:
        with pytest.raises(SystemExit):
            ppw.stop_tap(signal.SIGTERM, None)

        assert wait_until_gone(child.pid, timeout=0)
    finally:
        child.kill()
        child.wait()
        child.stdout.close()


def test_forked_sync_process_keeps_pidfile_and_uses_its_shorter_timeout(tmp_path):
    ppw = _pipelinewise(tmp_path)
    _write_pidfile(ppw, os.getpid())
    ppw._owner_pid = os.getpid() + 1

    with patch.object(ppw, '_terminate_process_tree') as terminate, pytest.raises(SystemExit):
        ppw.stop_tap(signal.SIGTERM, None)

    terminate.assert_called_once_with(os.getpid(), pipelinewise_module.STOP_TAP_CHILD_TIMEOUT_SECONDS)
    assert os.path.isfile(ppw.tap['files']['pidfile'])


@pytest.mark.parametrize('records_this_process', [True, False])
def test_owner_removes_pidfile_only_while_it_records_this_process(tmp_path, records_this_process):
    ppw = _pipelinewise(tmp_path)
    _write_pidfile(ppw, os.getpid() if records_this_process else os.getpid() + 1)

    with patch.object(ppw, '_terminate_process_tree') as terminate, pytest.raises(SystemExit):
        ppw.stop_tap(signal.SIGTERM, None)

    terminate.assert_called_once_with(os.getpid(), pipelinewise_module.STOP_TAP_TIMEOUT_SECONDS)
    assert os.path.isfile(ppw.tap['files']['pidfile']) is not records_this_process


def test_command_signals_the_recorded_tap_process(tmp_path):
    ppw = _pipelinewise(tmp_path)
    marker = tmp_path / 'sigterm-received'
    tap = _spawn(
        'import signal, sys, time\n'
        'def stop(signum, frame):\n'
        '    open(sys.argv[1], "w").close()\n'
        '    sys.exit(0)\n'
        'signal.signal(signal.SIGTERM, stop)\n'
        'print("ready", flush=True)\n'
        'time.sleep(60)\n',
        str(marker),
    )
    try:
        tap.stdout.readline()
        _write_pidfile(ppw, tap.pid)

        with _recorded_tap_is_running(True), pytest.raises(SystemExit) as exc:
            ppw.stop_tap()

        assert exc.value.code == 1
        assert marker.is_file()
        # The stopped run removes its own pidfile; the command must not delete a live run's pidfile.
        assert os.path.isfile(ppw.tap['files']['pidfile'])
    finally:
        tap.kill()
        tap.wait()
        tap.stdout.close()


def test_command_kills_the_tap_process_tree_after_timeout(tmp_path, monkeypatch):
    monkeypatch.setattr(pipelinewise_module, 'STOP_TAP_COMMAND_TIMEOUT_SECONDS', 0.5)
    ppw = _pipelinewise(tmp_path)
    tap = _spawn(
        'import signal, subprocess, sys, time\n'
        'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
        'child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])\n'
        'print(child.pid, flush=True)\n'
        'time.sleep(60)\n'
    )
    grandchild_pid = None
    try:
        grandchild_pid = int(tap.stdout.readline())
        _write_pidfile(ppw, tap.pid)

        with _recorded_tap_is_running(True), pytest.raises(SystemExit):
            ppw.stop_tap()

        assert tap.wait(timeout=5) == -signal.SIGKILL
        assert wait_until_gone(grandchild_pid)
    finally:
        tap.kill()
        tap.wait()
        tap.stdout.close()
        if grandchild_pid is not None and not wait_until_gone(grandchild_pid, timeout=0):
            os.kill(grandchild_pid, signal.SIGKILL)


def test_command_does_not_signal_a_stale_pid(tmp_path):
    ppw = _pipelinewise(tmp_path)
    _write_pidfile(ppw, os.getpid() + 1)

    with _recorded_tap_is_running(False), patch.object(pipelinewise_module.psutil, 'Process') as process, \
            pytest.raises(SystemExit) as exc:
        ppw.stop_tap()

    assert exc.value.code == 1
    process.assert_not_called()
