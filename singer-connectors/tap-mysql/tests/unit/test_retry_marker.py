"""Disconnect control messages remain independent of human logging formats."""

import json
from unittest.mock import patch

import pytest
from pymysql.err import OperationalError

from tap_mysql import main
from tap_mysql.connection import (
    BinlogStreamDisconnectedError,
    MYSQL_BINLOG_DISCONNECT_MARKER,
    MySQLConnection,
    make_connection_wrapper,
)


def test_tap_writes_control_marker_directly_to_stderr(capsys):
    failure = BinlogStreamDisconnectedError('source connection lost')
    with patch('tap_mysql.main_impl', side_effect=failure), patch('tap_mysql.LOGGER.critical') as log:
        with pytest.raises(BinlogStreamDisconnectedError) as raised:
            main()
    captured = capsys.readouterr()
    assert json.loads(captured.err) == MYSQL_BINLOG_DISCONNECT_MARKER
    assert captured.out == ''
    assert raised.value is failure
    log.assert_called_once_with(failure)


@pytest.mark.parametrize('failure', [
    RuntimeError('Binlog connection lost; restart replication from the durable checkpoint.'),
    ValueError('unrelated failure'), OperationalError(2006, 'initial connection failed'),
])
def test_unrelated_errors_never_emit_a_disconnect_marker(failure, capsys):
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
