"""The child-process wrapper reports results without letting a dying child hang the parent."""

import os
import struct
import threading

from pipelinewise.cli.multiprocess import Process


class _DiesMidMessage(Process):
    """Write a truncated result frame and die; the pipe travels with the process under any start method."""

    def run(self):
        os.write(self._cconn.fileno(), struct.pack('!i', 1_000_000) + b'partial')
        os._exit(9)


class _SendsUnreadableResult(Process):
    """Send a complete result frame that no pickle can load."""

    def run(self):
        self._cconn.send_bytes(b'not a pickle')


class _KeywordOnlyError(Exception):
    """Like Snowflake's HTTP errors, a pickle cannot rebuild it from its message alone."""

    def __init__(self, *, msg):
        super().__init__(msg)


def _raise_keyword_only_error():
    raise _KeywordOnlyError(msg='403 Forbidden')


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
    process = _DiesMidMessage()
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


def test_an_error_a_pickle_cannot_rebuild_still_reaches_the_parent():
    """The parent gets the child's message instead of a pickling TypeError and a zombie child."""
    process = Process(target=_raise_keyword_only_error)
    process.start()

    error, traceback_text = _join(process)

    assert isinstance(error, RuntimeError)
    assert str(error).endswith('_KeywordOnlyError: 403 Forbidden')
    assert '_KeywordOnlyError: 403 Forbidden' in traceback_text
    assert process.result_received is True
    assert process.exitcode == 0


def test_a_result_the_parent_cannot_read_is_an_error():
    """An unreadable result becomes an error, and the child is still joined."""
    process = _SendsUnreadableResult()
    process.start()

    error, traceback_text = _join(process)

    assert isinstance(error, RuntimeError)
    assert 'could not be read' in str(error)
    assert traceback_text == ''
    assert process.result_received is True
    assert process.exitcode == 0
