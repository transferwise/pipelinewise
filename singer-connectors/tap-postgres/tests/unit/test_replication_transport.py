"""Transport regressions for durable acknowledgements and server keepalives."""

import struct
from unittest.mock import Mock, patch

import psycopg2
import pytest

from tap_postgres.replication import ReplicationConnection, ReplicationCursor


def _cursor(*messages):
    connection = Mock()
    connection.encoding = 'utf-8'
    cursor = ReplicationCursor(connection)
    cursor.pgconn.flush.return_value = 0
    cursor.pgconn.put_copy_data.return_value = 1
    cursor.pgconn.error_message = b''
    cursor.pgconn.is_busy.return_value = False
    cursor.pgconn.get_result.return_value = None
    cursor.pgconn.get_copy_data.side_effect = [
        *((len(message), memoryview(message)) for message in messages), (0, b'')]
    return cursor


@pytest.mark.parametrize('acknowledged_lsn', [0, 100])
def test_keepalive_never_advances_durable_acknowledgement(acknowledged_lsn):
    cursor = _cursor(b'k' + struct.pack('!QqB', 999, 0, 1))
    cursor.send_feedback(write_lsn=acknowledged_lsn, flush_lsn=acknowledged_lsn)

    assert cursor.read_message() is None

    sent_feedback = cursor.pgconn.put_copy_data.call_args.args[0]
    _, write, flush, apply, _, reply = struct.unpack('!cQQQqB', sent_feedback)
    assert (write, flush, apply, reply) == (acknowledged_lsn, acknowledged_lsn, 0, 0)
    assert cursor.wal_end == 999


def test_data_message_position_does_not_advance_acknowledgement():
    cursor = _cursor(b'w' + struct.pack('!QQq', 200, 999, 0) + b'payload')
    cursor.send_feedback(write_lsn=100, flush_lsn=100)

    message = cursor.read_message()

    assert (message.payload, message.data_start, message.wal_end) == (b'payload', 200, 999)
    assert cursor.flush_lsn == 100
    cursor.pgconn.consume_input.assert_not_called()


def test_empty_replication_socket_is_polled_once_without_blocking():
    cursor = _cursor()
    cursor.pgconn.get_copy_data.side_effect = [(0, b''), (0, b'')]
    assert cursor.read_message() is None
    cursor.pgconn.consume_input.assert_called_once()


def test_wal2json_decodes_using_source_connection_encoding():
    cursor = _cursor(b'w' + struct.pack('!QQq', 200, 999, 0) + 'caf\u00e9'.encode('latin-1'))
    cursor.connection.encoding = 'latin-1'
    cursor.decode = True
    assert cursor.read_message().payload == 'caf\u00e9'


@pytest.mark.parametrize('message', [b'', b'k', b'k' + b'0' * 30, b'w', b'unknown'])
def test_malformed_transport_messages_fail_closed(message):
    cursor = _cursor(message or b'?')
    with pytest.raises(psycopg2.OperationalError, match='Invalid logical replication transport'):
        cursor.read_message()


def test_connection_closure_does_not_look_like_idle_success():
    cursor = _cursor()
    cursor.pgconn.get_copy_data.side_effect = [(-1, b'')]
    with pytest.raises(psycopg2.OperationalError, match='ended before the run boundary'):
        cursor.read_message()


def test_stream_failure_includes_postgresql_error_detail():
    cursor = _cursor()
    cursor.pgconn.get_copy_data.side_effect = [(-1, b'')]
    cursor.pgconn.get_result.return_value = Mock(
        error_message=b'ERROR: publication "ppw_slot_orders" does not exist\n')
    with pytest.raises(psycopg2.OperationalError, match='publication "ppw_slot_orders" does not exist'):
        cursor.read_message()


def test_broken_connection_reports_libpq_error_without_waiting_for_a_result():
    cursor = _cursor()
    cursor.pgconn.get_copy_data.side_effect = [(-2, b'')]
    cursor.pgconn.is_busy.return_value = True
    cursor.pgconn.error_message = b'SSL connection has been closed unexpectedly\n'
    with pytest.raises(psycopg2.OperationalError, match='SSL connection has been closed unexpectedly'):
        cursor.read_message()
    cursor.pgconn.get_result.assert_not_called()


def test_feedback_retries_when_output_buffer_is_full_without_losing_acknowledgement():
    cursor = _cursor()
    cursor.pgconn.put_copy_data.side_effect = [0, 1]
    cursor.pgconn.flush.side_effect = [1, 0, 0]
    with patch('tap_postgres.replication.select', return_value=([cursor], [cursor], [])):
        cursor.send_feedback(flush_lsn=100, force=True)
    assert cursor.pgconn.put_copy_data.call_count == 2
    cursor.pgconn.consume_input.assert_called_once()
    assert cursor.flush_lsn == 100


def test_feedback_output_backpressure_has_a_bounded_timeout():
    cursor = _cursor()
    cursor.pgconn.flush.return_value = 1
    with patch('tap_postgres.replication.time.monotonic', side_effect=[0, 31]), \
            pytest.raises(psycopg2.OperationalError, match='Timed out sending'):
        cursor.send_feedback(flush_lsn=100, force=True)


def test_connection_preserves_libpq_options_without_connection_factory():
    with patch('tap_postgres.replication.psycopg.connect') as connect:
        connection = ReplicationConnection(
            host='source', password="quo'te\\password", sslmode='require',
            connect_timeout=30, connection_factory=object)
        connection.close()
    connect.assert_called_once_with(
        host='source', password="quo'te\\password", sslmode='require',
        connect_timeout=30, replication='database', autocommit=True)
    connect.return_value.close.assert_called_once()
