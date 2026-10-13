"""Tests for shared Snowflake SQL result handling."""

import logging
import traceback
from unittest.mock import MagicMock, patch

import pytest
from snowflake.connector.errors import ProgrammingError

from pipelinewise.fastsync.commons.snowflake_sql_client import SnowflakeSqlClient


def _client_with_cursor():
    client = SnowflakeSqlClient({})
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value
    client.open_connection = MagicMock(return_value=connection)
    return client, cursor


def _query(client, method_name):
    if method_name == 'query':
        return client.query('SELECT value', {'value': 'one'})
    with patch.object(client, '_monotonic', side_effect=[10.0, 10.0]):
        return client.query_with_timeout(
            'SELECT value',
            {'value': 'one'},
            timeout_seconds=5.0,
        )


@pytest.mark.parametrize('method_name', ['query', 'query_with_timeout'])
@pytest.mark.parametrize('rowcount', [None, -1])
def test_fetches_described_unknown_rowcount(method_name, rowcount):
    """A result set is fetched even when the connector cannot report its size."""
    client, cursor = _client_with_cursor()
    cursor.description = (('VALUE',),)
    cursor.rowcount = rowcount
    cursor.fetchall.return_value = [{'VALUE': 'one'}]

    rows = _query(client, method_name)

    assert rows == [{'VALUE': 'one'}]
    cursor.fetchall.assert_called_once_with()


@pytest.mark.parametrize('method_name', ['query', 'query_with_timeout'])
def test_skips_fetch_without_description(method_name):
    """A statement without result metadata returns no rows."""
    client, cursor = _client_with_cursor()
    cursor.description = None
    cursor.rowcount = 1

    rows = _query(client, method_name)

    assert rows == []
    cursor.fetchall.assert_not_called()


def _data_error(message, errno=100090, sqlstate='42P18'):
    error = ProgrammingError(msg=message, errno=errno, sqlstate=sqlstate, sfqid='safe-query-id')
    error.query = 'SELECT PRIVATE_ROW_CANARY'
    error.__cause__ = RuntimeError('PRIVATE_ROW_CANARY in chained error')
    error.__context__ = RuntimeError('PRIVATE_ROW_CANARY in previous error')
    error.add_note('PRIVATE_ROW_CANARY in diagnostic note')
    return error


def _assert_safe_error(error, original, errno=100090, sqlstate='42P18'):
    assert error is original
    assert type(error) is ProgrammingError
    assert error.errno == errno
    assert error.sqlstate == sqlstate
    assert error.sfqid == 'safe-query-id'
    for value in (str(error), repr(error), repr(error.args), error.raw_msg, getattr(error, 'query', None)):
        assert 'PRIVATE_ROW_CANARY' not in str(value)
    assert 'PRIVATE_ROW_CANARY' not in ''.join(traceback.format_exception(error))


@pytest.mark.parametrize('method_name', ['query', 'query_with_timeout', 'execute_transaction'])
@pytest.mark.parametrize(
    ('message', 'errno', 'sqlstate'),
    [
        ('PRIVATE_ROW_CANARY: Duplicate row detected. Row Values: [PRIVATE_ROW_CANARY]', 100090, '42P18'),
        ('PRIVATE_ROW_CANARY: Duplicate row detected.\nRow Values:\n[PRIVATE_ROW_CANARY]', 100090, '42P18'),
        ("Numeric value 'PRIVATE_ROW_CANARY' is not recognized", 100038, '22018'),
    ],
)
def test_execution_failure_preserves_sdk_identity_without_row_values(method_name, message, errno, sqlstate):
    client, cursor = _client_with_cursor()
    original = _data_error(message, errno=errno, sqlstate=sqlstate)
    cursor.execute.side_effect = original

    with pytest.raises(ProgrammingError) as raised:
        if method_name == 'execute_transaction':
            client.execute_transaction(['MERGE INTO "SAFE_TABLE"', 'DELETE FROM "SAFE_TABLE"'])
        else:
            _query(client, method_name)

    _assert_safe_error(raised.value, original, errno, sqlstate)
    cursor.execute.assert_called_once()
    cursor.fetchall.assert_not_called()
    connection = client.open_connection.return_value
    connection.commit.assert_not_called()
    if method_name == 'execute_transaction':
        connection.rollback.assert_called_once_with()
        connection.close.assert_called_once_with()


@pytest.mark.parametrize('method_name', ['query', 'query_with_timeout', 'execute_transaction'])
def test_missing_table_error_remains_available_for_recovery(method_name):
    client, cursor = _client_with_cursor()
    original = ProgrammingError(
        msg='Table SAFE_TABLE does not exist or not authorized', errno=2043, sqlstate='42S02', sfqid='safe-query-id',
    )
    original_text = str(original)
    cursor.execute.side_effect = original

    with pytest.raises(ProgrammingError) as raised:
        if method_name == 'execute_transaction':
            client.execute_transaction(['SELECT * FROM "SAFE_TABLE"'])
        else:
            _query(client, method_name)

    assert raised.value is original
    assert raised.value.errno == 2043
    assert raised.value.sqlstate == '42S02'
    assert str(raised.value) == original_text
    assert raised.value.raw_msg == 'Table SAFE_TABLE does not exist or not authorized'


def test_transaction_cleanup_logs_do_not_expose_row_values(caplog):
    client, cursor = _client_with_cursor()
    client.ignore_cleanup_errors = True
    client.sql_logger = logging.getLogger('test_snowflake_cleanup')
    original = _data_error('PRIVATE_ROW_CANARY: Duplicate row detected. Row Values: [PRIVATE_ROW_CANARY]')
    rollback_error = _data_error("Numeric value 'PRIVATE_ROW_CANARY' cannot be rolled back", 100038, '22018')
    close_error = _data_error("Numeric value 'PRIVATE_ROW_CANARY' cannot be closed", 100038, '22018')
    cursor.execute.side_effect = original
    connection = client.open_connection.return_value
    connection.rollback.side_effect = rollback_error
    connection.close.side_effect = close_error

    with caplog.at_level(logging.WARNING, logger=client.sql_logger.name), pytest.raises(ProgrammingError) as raised:
        client.execute_transaction(['MERGE INTO "SAFE_TABLE"'])

    _assert_safe_error(raised.value, original)
    assert 'Failed to roll back Snowflake publication transaction' in caplog.text
    assert 'Failed to close Snowflake publication connection' in caplog.text
    assert 'PRIVATE_ROW_CANARY' not in caplog.text
    connection.rollback.assert_called_once_with()
    connection.close.assert_called_once_with()
    connection.commit.assert_not_called()
