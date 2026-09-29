"""Disconnect control messages remain independent of human logging formats."""

import json
import os
import subprocess
import sys
from unittest.mock import patch

import pytest
from pymysql.err import OperationalError

from tap_mysql import MYSQL_BINLOG_DISCONNECT_CONTROL_PREFIX, main
from tap_mysql.connection import (
    BinlogStreamDisconnectedError,
    MYSQL_BINLOG_DISCONNECT_MARKER,
    MYSQL_BINLOG_RETRY_PENDING_ENV,
    MySQLConnection,
    make_connection_wrapper,
)


@pytest.mark.parametrize('retry_pending', [None, '0', 'invalid'])
def test_tap_writes_control_marker_directly_to_stderr(capsys, monkeypatch, retry_pending):
    if retry_pending is None:
        monkeypatch.delenv(MYSQL_BINLOG_RETRY_PENDING_ENV, raising=False)
    else:
        monkeypatch.setenv(MYSQL_BINLOG_RETRY_PENDING_ENV, retry_pending)
    failure = BinlogStreamDisconnectedError('source connection lost')
    with patch('tap_mysql.main_impl', side_effect=failure), patch('tap_mysql.LOGGER.critical') as log:
        with pytest.raises(BinlogStreamDisconnectedError) as raised:
            main()
    captured = capsys.readouterr()
    assert captured.err.startswith(MYSQL_BINLOG_DISCONNECT_CONTROL_PREFIX)
    assert json.loads(captured.err.removeprefix(MYSQL_BINLOG_DISCONNECT_CONTROL_PREFIX)) == (
        MYSQL_BINLOG_DISCONNECT_MARKER
    )
    assert captured.out == ''
    assert raised.value is failure
    log.assert_called_once_with(failure)


@pytest.mark.parametrize('error_code', [None, 2006, 2013])
def test_managed_retry_logs_warning_and_exits_nonzero_without_raising_disconnect(capsys, monkeypatch, error_code):
    monkeypatch.setenv(MYSQL_BINLOG_RETRY_PENDING_ENV, '1')
    failure = BinlogStreamDisconnectedError('source connection lost')
    if error_code is not None:
        failure.__cause__ = OperationalError(error_code, 'source disconnected')
    with patch('tap_mysql.main_impl', side_effect=failure), \
            patch('tap_mysql.LOGGER.warning') as warning, patch('tap_mysql.LOGGER.critical') as critical:
        with pytest.raises(SystemExit) as raised:
            main()
    assert raised.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ''
    assert json.loads(captured.err.removeprefix(MYSQL_BINLOG_DISCONNECT_CONTROL_PREFIX)) == (
        MYSQL_BINLOG_DISCONNECT_MARKER
    )
    cause = failure.__cause__ or failure
    warning.assert_called_once_with(
        'Binlog connection lost; PipelineWise will retry from durable state. Cause: %s.%s: %s',
        type(cause).__module__, type(cause).__qualname__, cause,
    )
    critical.assert_not_called()


@pytest.mark.parametrize('error_code', [2006, 2013])
@pytest.mark.parametrize('retry_pending,disconnect,traceback_expected', [
    ('1', True, False), ('0', True, True), (None, True, True), ('1', False, True),
])
def test_subprocess_traceback_is_suppressed_only_for_managed_retryable_disconnect(
        retry_pending, disconnect, traceback_expected, error_code):
    environment = dict(os.environ)
    environment.pop(MYSQL_BINLOG_RETRY_PENDING_ENV, None)
    if retry_pending is not None:
        environment[MYSQL_BINLOG_RETRY_PENDING_ENV] = retry_pending
    script = '''
import sys
import tap_mysql
from pymysql.err import OperationalError
from tap_mysql.connection import BinlogStreamDisconnectedError

def fail():
    if sys.argv[1] == 'disconnect':
        try:
            raise OperationalError(int(sys.argv[2]), 'source disconnected')
        except OperationalError as exc:
            raise BinlogStreamDisconnectedError('source connection lost') from exc
    raise ValueError('unrelated failure')

tap_mysql.main_impl = fail
sys.exit(tap_mysql.main())
'''
    result = subprocess.run(
        [sys.executable, '-c', script, 'disconnect' if disconnect else 'unrelated', str(error_code)],
        env=environment, capture_output=True, text=True, check=False, timeout=20,
    )
    assert result.returncode == 1
    assert result.stdout == ''
    assert ('Traceback (most recent call last)' in result.stderr) is traceback_expected
    assert (MYSQL_BINLOG_DISCONNECT_CONTROL_PREFIX in result.stderr) is disconnect
    if disconnect:
        assert f"pymysql.err.OperationalError: ({error_code}, 'source disconnected')" in result.stderr
    if not traceback_expected:
        assert 'WARNING' in result.stderr
        assert 'CRITICAL' not in result.stderr
    else:
        assert 'CRITICAL' in result.stderr


@pytest.mark.parametrize('failure', [
    RuntimeError('Binlog connection lost; restart replication from the durable checkpoint.'),
    ValueError('unrelated failure'), OperationalError(2006, 'initial connection failed'),
])
def test_unrelated_errors_never_emit_a_disconnect_marker(failure, capsys, monkeypatch):
    monkeypatch.setenv(MYSQL_BINLOG_RETRY_PENDING_ENV, '1')
    with patch('tap_mysql.main_impl', side_effect=failure), patch('tap_mysql.LOGGER.critical'):
        with pytest.raises(type(failure)):
            main()
    assert capsys.readouterr().err == ''


@pytest.mark.parametrize('error_code', [2006, 2013])
def test_guarded_stream_disconnect_raises_typed_signal(error_code):
    config = {'host': 'localhost', 'port': 3306, 'user': 'test', 'password': 'test', 'use_gtid': False}
    with patch('tap_mysql.connection.connect_with_backoff'):
        connection = make_connection_wrapper(config)()
    failure = OperationalError(error_code, 'source disconnected')
    with patch.object(MySQLConnection, '_read_packet', side_effect=failure):
        with pytest.raises(BinlogStreamDisconnectedError) as raised:
            connection._read_packet()
    assert raised.value.__cause__ is failure
