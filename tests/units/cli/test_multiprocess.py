"""The child-process wrapper reports results without letting a dying child hang the parent."""

import os
import struct
import threading

from pipelinewise.cli.multiprocess import Process


PROCESSES = {}


def _send_partial_message_then_die():
    connection = PROCESSES['partial']._cconn
    os.write(connection.fileno(), struct.pack('!i', 1_000_000) + b'partial')
    os._exit(9)


def _raise_value_error():
    raise ValueError('child failed')


def _return_normally():
    return None


def _join(process):
    outcome = {}

    def wait():
        outcome['result'] = process.join_with_result(poll_seconds=0.1)

    waiter = threading.Thread(target=wait, daemon=True)
    waiter.start()
    waiter.join(timeout=30)
    assert not waiter.is_alive(), 'join_with_result hung on the child pipe'
    return outcome['result']


def test_a_child_that_dies_mid_message_does_not_hang_the_parent():
    """A truncated result is end-of-file, so the parent reports the dead child instead of waiting."""
    process = Process(target=_send_partial_message_then_die)
    PROCESSES['partial'] = process
    process.start()

    assert _join(process) is None
    assert process.result_received is False
    assert process.exitcode == 9


def test_a_normal_child_reports_success():
    """A child that returns normally reports success explicitly."""
    process = Process(target=_return_normally)
    process.start()

    assert _join(process) is None
    assert process.result_received is True
    assert process.exitcode == 0


def test_a_failing_child_reports_its_exception():
    """A child's exception and traceback reach the parent."""
    process = Process(target=_raise_value_error)
    process.start()

    error, traceback_text = _join(process)

    assert isinstance(error, ValueError)
    assert str(error) == 'child failed'
    assert 'ValueError: child failed' in traceback_text
    assert process.result_received is True
