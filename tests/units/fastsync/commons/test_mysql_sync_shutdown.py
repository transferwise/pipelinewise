"""MySQL FastSync processes must stop their export workers when they are terminated."""

import signal
import subprocess
import sys
from unittest.mock import Mock

import psutil
import pytest

from pipelinewise.fastsync.commons import tap_mysql
from tests.units.process_helpers import wait_until_gone

SYNC_PROCESS = (
    'import multiprocessing, time\n'
    'from pipelinewise.fastsync.commons import tap_mysql\n'
    'tap_mysql.install_mysql_sync_signal_handlers()\n'
    'with multiprocessing.Pool(2, initializer=tap_mysql.install_mysql_worker_signal_handlers) as pool:\n'
    '    result = pool.map_async(time.sleep, [60, 60])\n'
    '    print(" ".join(str(worker.pid) for worker in pool._pool), flush=True)\n'
    '    result.get()\n'
)


def test_sigterm_handler_exits_and_keeps_existing_handlers():
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    tap_mysql.install_mysql_sync_signal_handlers()

    with pytest.raises(SystemExit) as exc:
        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
    assert exc.value.code == 128 + signal.SIGTERM

    existing = Mock()
    signal.signal(signal.SIGTERM, existing)
    tap_mysql.install_mysql_sync_signal_handlers()
    assert signal.getsignal(signal.SIGTERM) is existing


@pytest.mark.skipif(not sys.platform.startswith('linux'), reason='Production pools fork on Linux')
def test_sigterm_on_sync_process_stops_its_workers():
    proc = subprocess.Popen([sys.executable, '-c', SYNC_PROCESS], stdout=subprocess.PIPE)
    worker_pids = [int(pid) for pid in proc.stdout.readline().split()]
    try:
        proc.terminate()

        assert proc.wait(timeout=10) == 128 + signal.SIGTERM
        assert all(wait_until_gone(pid) for pid in worker_pids)
    finally:
        proc.kill()
        proc.wait()
        proc.stdout.close()
        for pid in worker_pids:
            if psutil.pid_exists(pid):
                psutil.Process(pid).kill()
