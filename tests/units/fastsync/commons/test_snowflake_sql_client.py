"""Tests for shared Snowflake SQL result handling."""

from unittest.mock import MagicMock, patch

import pytest

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
